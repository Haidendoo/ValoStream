#!/usr/bin/env python3
"""Spatial serving over a validated mart snapshot or the legacy raw vault."""

from contextlib import asynccontextmanager, closing
import logging
import os
import time
from typing import Any, Dict, List, Literal, Optional

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
import h3
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel
import pymysql

if __package__:
    from .queries import ServingSource
else:
    from queries import ServingSource

log = logging.getLogger(__name__)
SOURCE = ServingSource(
    catalog=os.getenv("STARROCKS_CATALOG", "nessie_main"),
    vault_schema=os.getenv("STARROCKS_VAULT_SCHEMA", "vault"),
    mart_schema=os.getenv("STARROCKS_MART_SCHEMA") or None,
)

REQUEST_COUNT = Counter("valostream_api_requests_total", "Serving requests", ["endpoint", "status"])
REQUEST_LATENCY = Histogram("valostream_api_request_duration_seconds", "Serving latency", ["endpoint"])


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
    title="ValoStream Spatial Serving API", version="1.2.0", lifespan=lifespan,
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
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


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
def health_check():
    try:
        # Check the active source, not just the availability of a SQL connection.
        with closing(get_connection()) as conn, conn.cursor() as cursor:
            for table in SOURCE.readiness_tables():
                cursor.execute(f"SELECT * FROM {table} LIMIT 0")
        return {"status": "ok", "starrocks": "connected", "lakehouse": "reachable",
                "engine": "vectorized-olap", "source": "marts" if SOURCE.mart_schema else "vault"}
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
):
    start = time.perf_counter()
    try:
        cell = h3.latlng_to_cell(lat, lon, 7)
        neighbors = [int(value, 16) for value in h3.grid_disk(cell, 1)] if use_h3_filter else []
        rows = query_rows(*SOURCE.recommendations(lat, lon, limit, neighbors))
        if not rows and use_h3_filter:
            # The first connection has already closed; preserve the full request latency.
            rows = query_rows(*SOURCE.recommendations(lat, lon, limit))
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
):
    if resolution not in (7, 9):
        raise HTTPException(status_code=422, detail="H3 resolution must be 7 or 9")
    start = time.perf_counter()
    try:
        rows = query_rows(*SOURCE.hexagons(entity, resolution, limit))
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
):
    start = time.perf_counter()
    try:
        rows = query_rows(*SOURCE.live_couriers(status, limit))
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
def lakehouse_stats():
    """Counts for the active source: raw vault or normalized staging tables."""
    try:
        sql = " UNION ALL ".join(
            f"SELECT '{name}' AS tbl, COUNT(*) AS cnt FROM {table}" for name, table in SOURCE.tables().items()
        )
        return {row["tbl"]: int(row["cnt"]) for row in query_rows(sql)}
    except Exception as error:
        query_failed("Failed to fetch stats", error)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
