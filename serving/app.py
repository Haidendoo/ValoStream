#!/usr/bin/env python3
"""Spatial serving over a validated mart snapshot or the legacy raw vault."""

from contextlib import asynccontextmanager, closing
import logging
import os
import time
from typing import Any, Dict, List, Literal, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
import h3
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel
import pymysql

if __package__:
    from .quality import read_report
    from .queries import ServingSource
    from .releases import FreshnessPolicy, read_state, selected_release, timestamp
else:
    from quality import read_report
    from queries import ServingSource
    from releases import FreshnessPolicy, read_state, selected_release, timestamp

log = logging.getLogger(__name__)
SOURCE = ServingSource(
    catalog=os.getenv("STARROCKS_CATALOG", "nessie_main"),
    vault_schema=os.getenv("STARROCKS_VAULT_SCHEMA", "vault"),
    mart_schema=os.getenv("STARROCKS_MART_SCHEMA") or None,
)
RELEASE_FILE = os.getenv("STARROCKS_RELEASE_FILE") or None
QUALITY_REPORT_FILE = os.getenv("VALOSTREAM_QUALITY_REPORT_FILE") or None
if RELEASE_FILE and SOURCE.mart_schema:
    raise ValueError("Choose STARROCKS_RELEASE_FILE or STARROCKS_MART_SCHEMA, not both")
FRESHNESS = FreshnessPolicy(
    int(os.getenv("STARROCKS_MAX_BUILD_AGE_SECONDS", "0")),
    int(os.getenv("STARROCKS_MAX_EVENT_AGE_SECONDS", "0")),
)
if not RELEASE_FILE and (FRESHNESS.max_build_age_seconds or FRESHNESS.max_event_age_seconds):
    raise ValueError("Freshness limits require STARROCKS_RELEASE_FILE")

REQUEST_COUNT = Counter("valostream_api_requests_total", "Serving requests", ["endpoint", "status"])
REQUEST_LATENCY = Histogram("valostream_api_request_duration_seconds", "Serving latency", ["endpoint"])


def get_source(request: Request):
    # Resolve once per request. Fallback queries must use this same snapshot.
    if RELEASE_FILE is None:
        yield SOURCE
        return
    try:
        with selected_release(RELEASE_FILE) as release:
            request.state.release = release
            yield ServingSource(mart_schema=release.schema)
    except (OSError, ValueError, TypeError, KeyError) as error:
        log.exception("Serving release manifest unavailable")
        raise HTTPException(status_code=503, detail="Serving release unavailable") from error


def get_connection():
    return pymysql.connect(
        host=os.getenv("STARROCKS_HOST", "localhost"),
        port=int(os.getenv("STARROCKS_PORT", "9030")),
        user=os.getenv("STARROCKS_USER", "root"),
        password=os.getenv("STARROCKS_PASSWORD", ""),
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True, connect_timeout=5, read_timeout=15, write_timeout=15,
    )


def query_rows(sql, params=()):
    with closing(get_connection()) as conn, conn.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall()


def query_failed(message, error):
    log.exception(message)
    code = 503 if isinstance(error, pymysql.Error) else 500
    raise HTTPException(status_code=code, detail=message) from error


@asynccontextmanager
async def lifespan(app):
    try:
        query_rows("SELECT 1")
    except pymysql.Error:
        log.warning("StarRocks unavailable at startup; /healthz will report readiness")
    yield


app = FastAPI(
    title="ValoStream Spatial Serving API", version="1.3.0", lifespan=lifespan,
    description="Location-aware popularity recommendations, H3 demand, and current courier positions.",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])


@app.middleware("http")
async def observe_requests(request: Request, call_next):
    start, status = time.perf_counter(), 500
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        route = request.scope.get("route")
        endpoint = route.path if route else "unmatched"
        if endpoint != "/metrics":
            REQUEST_COUNT.labels(endpoint, str(status)).inc()
            REQUEST_LATENCY.labels(endpoint).observe(time.perf_counter() - start)


@app.get("/metrics")
def get_metrics():
    # A fresh registry avoids exporting stale metadata after a manifest disappears.
    registry = CollectorRegistry()

    def gauge(name, help_text, value):
        Gauge("valostream_" + name, help_text, registry=registry).set(value)

    gauge("release_manifest_configured", "Whether the API follows a release manifest", RELEASE_FILE is not None)
    gauge("quality_report_configured", "Whether daily volume monitoring is configured", QUALITY_REPORT_FILE is not None)
    if QUALITY_REPORT_FILE is not None:
        try:
            quality = read_report(QUALITY_REPORT_FILE)
        except (OSError, ValueError, TypeError, KeyError, OverflowError):
            gauge("quality_report_valid", "Whether the daily volume report can be read", 0)
        else:
            gauge("quality_report_valid", "Whether the daily volume report can be read", 1)
            gauge("quality_healthy", "Whether daily volume checks have a full baseline and pass", quality["status"] == "ok")
            gauge("quality_window_end_timestamp_seconds", "End of the last checked UTC day", quality["window_end"])
    if RELEASE_FILE is not None:
        try:
            release = read_state(RELEASE_FILE).active
        except (OSError, ValueError, TypeError, KeyError):
            gauge("release_manifest_valid", "Whether the configured manifest can be read", 0)
        else:
            gauge("release_manifest_valid", "Whether the configured manifest can be read", 1)
            gauge("release_build_timestamp_seconds", "UTC timestamp of active release build completion",
                  timestamp(release.built_at).timestamp())
            gauge("release_freshness_healthy", "Whether configured freshness checks pass",
                  FRESHNESS.evaluate(release)["status"] in ("ok", "unchecked"))
            gauge("release_max_build_age_seconds", "Configured build age limit; zero disables", FRESHNESS.max_build_age_seconds)
            gauge("release_max_event_age_seconds", "Configured event age limit; zero disables", FRESHNESS.max_event_age_seconds)
            known = Gauge("valostream_release_event_watermark_known", "Whether a stream watermark is known",
                          ["stream"], registry=registry)
            events = Gauge("valostream_release_event_timestamp_seconds", "Latest event timestamp in the active release",
                           ["stream"], registry=registry)
            for stream in ("interactions", "couriers"):
                value = (release.watermarks or {}).get(stream)
                known.labels(stream).set(value is not None)
                if value is not None:
                    events.labels(stream).set(timestamp(value).timestamp())
    return Response(content=generate_latest() + generate_latest(registry), media_type=CONTENT_TYPE_LATEST)


class MerchantRecommendation(BaseModel):
    merchant_id: str
    rank: int
    score: float
    interaction_count: int
    distance_meters: float
    avg_latitude: float
    avg_longitude: float
    h3_res7: Optional[str] = None


class RecommendationResponse(BaseModel):
    user_id: str
    user_location: Dict[str, float]
    user_h3_res7: str
    recommendations: List[MerchantRecommendation]
    total_returned: int
    latency_ms: float


class HexagonFeature(BaseModel):
    hex: str
    count: int
    centroid: List[float]
    properties: Dict[str, Any]


class HexagonResponse(BaseModel):
    entity: str
    resolution: int
    total_hexagons: int
    hexagons: List[HexagonFeature]
    latency_ms: float


@app.get("/healthz")
def health_check(request: Request, source: ServingSource = Depends(get_source)):
    release = getattr(request.state, "release", None)
    freshness = FRESHNESS.evaluate(release) if release else {"status": "unavailable"}
    if freshness["status"] in ("stale", "unknown", "clock_skew"):
        raise HTTPException(status_code=503, detail={"message": "Serving release freshness check failed",
                                                    "freshness": freshness})
    try:
        # Check the active source, not just the availability of a SQL connection.
        with closing(get_connection()) as conn, conn.cursor() as cursor:
            for table in source.readiness_tables():
                cursor.execute(f"SELECT * FROM {table} LIMIT 0")
        return {"status": "ok", "starrocks": "connected", "lakehouse": "reachable",
                "engine": "vectorized-olap", "source": "marts" if source.mart_schema else "vault",
                "mart_schema": source.mart_schema, "freshness": freshness}
    except Exception as error:
        log.exception("Serving source readiness failed")
        raise HTTPException(status_code=503, detail="Serving data source unavailable") from error


@app.get("/recommendations", response_model=RecommendationResponse)
def get_recommendations(
    user_id: str = Query(..., min_length=1, max_length=256),
    lat: float = Query(10.7750, ge=-90, le=90),
    lon: float = Query(106.7000, ge=-180, le=180),
    limit: int = Query(10, ge=1, le=50),
    use_h3_filter: bool = Query(False, description="Restrict to H3 neighbors; fall back globally if empty"),
    source: ServingSource = Depends(get_source),
):
    start = time.perf_counter()
    try:
        cell = h3.latlng_to_cell(lat, lon, 7)
        neighbors = [int(value, 16) for value in h3.grid_disk(cell, 1)] if use_h3_filter else []
        rows = query_rows(*source.recommendations(lat, lon, limit, neighbors))
        if not rows and use_h3_filter:
            # The first connection has already closed; preserve the full request latency.
            rows = query_rows(*source.recommendations(lat, lon, limit))
        recommendations = [MerchantRecommendation(
            merchant_id=row["merchant_id"], rank=rank, score=float(row["score"]),
            interaction_count=int(row["interaction_count"]),
            distance_meters=float(row["dist_meters"] if row["dist_meters"] is not None else 1000.0),
            avg_latitude=float(row["avg_lat"]), avg_longitude=float(row["avg_lon"]),
            h3_res7=row["sample_h3_res7"].lower() if row.get("sample_h3_res7") else None,
        ) for rank, row in enumerate(rows, 1)]
        return RecommendationResponse(
            user_id=user_id, user_location={"latitude": lat, "longitude": lon}, user_h3_res7=cell,
            recommendations=recommendations, total_returned=len(recommendations),
            latency_ms=round((time.perf_counter() - start) * 1000, 2),
        )
    except Exception as error:
        query_failed("Recommendation query failed", error)


@app.get("/spatial/hexagons", response_model=HexagonResponse)
def get_spatial_hexagons(
    entity: Literal["interactions", "couriers"] = "interactions",
    resolution: int = Query(7, description="Supported H3 resolutions: 7 and 9"),
    limit: int = Query(100, ge=1, le=500),
    source: ServingSource = Depends(get_source),
):
    if resolution not in (7, 9):
        raise HTTPException(status_code=422, detail="H3 resolution must be 7 or 9")
    start = time.perf_counter()
    try:
        rows = query_rows(*source.hexagons(entity, resolution, limit))
        features = []
        for row in rows:
            if not row.get("h3_hex"):
                continue
            properties = ({"avg_dwell_ms": float(row["avg_dwell_ms"] or 0)} if entity == "interactions"
                          else {"avg_speed_kmh": float(row["avg_speed_kmh"] or 0),
                                "active_couriers": int(row["active_couriers"])})
            features.append(HexagonFeature(
                hex=row["h3_hex"].lower(), count=int(row["metric_count"]),
                centroid=[float(row["centroid_lon"]), float(row["centroid_lat"])], properties=properties,
            ))
        return HexagonResponse(entity=entity, resolution=resolution, total_hexagons=len(features),
                               hexagons=features, latency_ms=round((time.perf_counter() - start) * 1000, 2))
    except Exception as error:
        query_failed("Hexagon aggregation query failed", error)


@app.get("/spatial/couriers")
def get_live_couriers(
    status: Optional[Literal["to_merchant", "at_merchant", "delivering", "idle"]] = None,
    limit: int = Query(50, ge=1, le=200),
    source: ServingSource = Depends(get_source),
):
    start = time.perf_counter()
    try:
        rows = query_rows(*source.live_couriers(status, limit))
        couriers = [{
            "courier_id": row["courier_id"], "latitude": float(row["latitude"]),
            "longitude": float(row["longitude"]), "speed_kmh": float(row["speed_kmh"] or 0),
            "bearing_deg": int(row["bearing_deg"] or 0), "status": row["status"],
            "h3_res7": row["h3_res7"].lower() if row["h3_res7"] else None,
            "h3_res9": row["h3_res9"].lower() if row["h3_res9"] else None,
            "load_date": str(row["load_date"]),
        } for row in rows]
        return {"total": len(couriers), "couriers": couriers,
                "latency_ms": round((time.perf_counter() - start) * 1000, 2)}
    except Exception as error:
        query_failed("Courier query failed", error)


@app.get("/stats")
def lakehouse_stats(source: ServingSource = Depends(get_source)):
    """Counts for the active source: raw vault or normalized staging tables."""
    try:
        sql = " UNION ALL ".join(
            f"SELECT '{name}' AS tbl, COUNT(*) AS cnt FROM {table}" for name, table in source.tables().items()
        )
        return {row["tbl"]: int(row["cnt"]) for row in query_rows(sql)}
    except Exception as error:
        query_failed("Failed to fetch stats", error)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
