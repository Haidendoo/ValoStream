#!/usr/bin/env python3
"""
ValoStream Location-Aware Serving API
Vectorized OLAP Serving API querying Apache Iceberg Lakehouse on StarRocks
with Uber H3 Hexagonal Discrete Global Grid & GeoParquet Spatial Analytics.
"""

import os
import math
import time
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pymysql
import h3
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter, Histogram

REQUEST_COUNT = Counter(
    "valostream_api_requests_total",
    "Total requests to ValoStream serving API",
    ["endpoint", "status"]
)
REQUEST_LATENCY = Histogram(
    "valostream_api_request_duration_seconds",
    "Request latency in seconds",
    ["endpoint"]
)

STARROCKS_HOST = os.getenv("STARROCKS_HOST", "localhost")
STARROCKS_PORT = int(os.getenv("STARROCKS_PORT", "9030"))
STARROCKS_USER = os.getenv("STARROCKS_USER", "root")
STARROCKS_PASSWORD = os.getenv("STARROCKS_PASSWORD", "")
STARROCKS_CATALOG = os.getenv("STARROCKS_CATALOG", "nessie_main")

def get_connection():
    return pymysql.connect(
        host=STARROCKS_HOST,
        port=STARROCKS_PORT,
        user=STARROCKS_USER,
        password=STARROCKS_PASSWORD,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Test DB connection at startup
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
        conn.close()
        print(f" Connected to StarRocks at {STARROCKS_HOST}:{STARROCKS_PORT}")
    except Exception as e:
        print(f"⚠️ StarRocks initial connection warning: {e}")
    yield

app = FastAPI(
    title="ValoStream Location-Aware Recommender & Spatial API",
    version="1.1.0",
    description="Vectorized OLAP Serving API querying Iceberg Lakehouse on StarRocks with Uber H3 & GeoParquet",
    lifespan=lifespan
)

# Enable CORS for Deck.gl / frontends
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    centroid: List[float]  # [lon, lat]
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
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
        conn.close()
        return {"status": "ok", "starrocks": "connected", "lakehouse": "healthy", "engine": "vectorized-olap"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database connection failed: {e}")

@app.get("/recommendations", response_model=RecommendationResponse)
def get_recommendations(
    user_id: str = Query(..., description="Target customer user ID, e.g. USR_000123"),
    lat: float = Query(10.7750, description="Customer current latitude (Ho Chi Minh City)"),
    lon: float = Query(106.7000, description="Customer current longitude (Ho Chi Minh City)"),
    limit: int = Query(10, ge=1, le=50, description="Number of recommendations to return"),
    use_h3_filter: bool = Query(False, description="Filter merchants to customer's H3 neighborhood (k-ring 1)")
):
    t0 = time.time()
    try:
        user_h3_str = h3.latlng_to_cell(lat, lon, 7)
        user_h3_neighbors = [int(h, 16) for h in h3.grid_disk(user_h3_str, 1)]

        conn = get_connection()
        with conn.cursor() as cur:
            # Query StarRocks vectorized engine over Nessie Iceberg catalog
            # Prefer sat_interaction_context_spatial for H3 / spatial index
            h3_clause = ""
            params: list = [lon, lat]
            if use_h3_filter:
                placeholders = ",".join(["%s"] * len(user_h3_neighbors))
                h3_clause = f"AND s.h3_res7 IN ({placeholders})"
                params.extend(user_h3_neighbors)

            params.append(limit)

            sql = f"""
            SELECT 
                m.merchant_id,
                count(*) AS interaction_count,
                round(avg(s.latitude), 6) AS avg_lat,
                round(avg(s.longitude), 6) AS avg_lon,
                hex(max(s.h3_res7)) AS sample_h3_res7,
                round(ST_Distance_Sphere(avg(s.longitude), avg(s.latitude), %s, %s), 1) AS dist_meters
            FROM {STARROCKS_CATALOG}.vault.link_user_merchant_interaction l
            JOIN {STARROCKS_CATALOG}.vault.sat_interaction_context_spatial s ON l.hk_interaction_id = s.hk_interaction_id
            JOIN {STARROCKS_CATALOG}.vault.hub_merchant m ON l.hk_merchant_id = m.hk_merchant_id
            WHERE s.latitude IS NOT NULL AND s.longitude IS NOT NULL {h3_clause}
            GROUP BY m.merchant_id
            ORDER BY interaction_count DESC
            LIMIT %s;
            """
            cur.execute(sql, params)
            rows = cur.fetchall()

            # Fallback to general table if spatial has fewer rows
            if not rows and use_h3_filter:
                # Retry without strict H3 filter
                return get_recommendations(user_id=user_id, lat=lat, lon=lon, limit=limit, use_h3_filter=False)

        conn.close()

        recommendations = []
        for rank, row in enumerate(rows, start=1):
            dist_km = (row["dist_meters"] or 1000.0) / 1000.0
            score = round(float(row["interaction_count"]) / (1.0 + dist_km * 0.5), 2)
            h3_hex = row.get("sample_h3_res7")
            recommendations.append(
                MerchantRecommendation(
                    merchant_id=row["merchant_id"],
                    rank=rank,
                    score=score,
                    interaction_count=int(row["interaction_count"]),
                    distance_meters=float(row["dist_meters"] or 0.0),
                    avg_latitude=float(row["avg_lat"]),
                    avg_longitude=float(row["avg_lon"]),
                    h3_res7=h3_hex.lower() if h3_hex else None
                )
            )

        latency_ms = round((time.time() - t0) * 1000, 2)
        return RecommendationResponse(
            user_id=user_id,
            user_location={"latitude": lat, "longitude": lon},
            user_h3_res7=user_h3_str,
            recommendations=recommendations,
            total_returned=len(recommendations),
            latency_ms=latency_ms
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Recommendation query failed: {e}")

@app.get("/spatial/hexagons", response_model=HexagonResponse)
def get_spatial_hexagons(
    entity: str = Query("interactions", regex="^(interactions|couriers)$", description="Target spatial domain"),
    resolution: int = Query(7, ge=7, le=9, description="H3 resolution (7 or 9)"),
    limit: int = Query(100, ge=1, le=500, description="Max hexagons to return")
):
    """
    Returns H3 hexagon discrete global grid cells with aggregated metrics
    ready for 3D Deck.gl H3HexagonLayer or Kepler.gl map visualization.
    """
    t0 = time.time()
    col_h3 = f"h3_res{resolution}"
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            if entity == "interactions":
                sql = f"""
                SELECT 
                    hex({col_h3}) AS h3_hex,
                    count(*) AS metric_count,
                    round(avg(latitude), 6) AS centroid_lat,
                    round(avg(longitude), 6) AS centroid_lon,
                    round(avg(dwell_time_ms), 0) AS avg_dwell_ms
                FROM {STARROCKS_CATALOG}.vault.sat_interaction_context_spatial
                WHERE {col_h3} IS NOT NULL
                GROUP BY {col_h3}
                ORDER BY metric_count DESC
                LIMIT %s;
                """
                cur.execute(sql, (limit,))
                rows = cur.fetchall()
                hexagons = [
                    HexagonFeature(
                        hex=r["h3_hex"].lower(),
                        count=int(r["metric_count"]),
                        centroid=[float(r["centroid_lon"]), float(r["centroid_lat"])],
                        properties={"avg_dwell_ms": float(r["avg_dwell_ms"] or 0)}
                    )
                    for r in rows if r.get("h3_hex")
                ]
            else:  # couriers
                sql = f"""
                SELECT 
                    hex({col_h3}) AS h3_hex,
                    count(*) AS metric_count,
                    round(avg(latitude), 6) AS centroid_lat,
                    round(avg(longitude), 6) AS centroid_lon,
                    round(avg(speed_kmh), 1) AS avg_speed_kmh,
                    count(DISTINCT hk_courier_id) AS active_couriers
                FROM {STARROCKS_CATALOG}.vault.sat_courier_telemetry_spatial
                WHERE {col_h3} IS NOT NULL
                GROUP BY {col_h3}
                ORDER BY metric_count DESC
                LIMIT %s;
                """
                cur.execute(sql, (limit,))
                rows = cur.fetchall()
                hexagons = [
                    HexagonFeature(
                        hex=r["h3_hex"].lower(),
                        count=int(r["metric_count"]),
                        centroid=[float(r["centroid_lon"]), float(r["centroid_lat"])],
                        properties={
                            "avg_speed_kmh": float(r["avg_speed_kmh"] or 0),
                            "active_couriers": int(r["active_couriers"] or 0)
                        }
                    )
                    for r in rows if r.get("h3_hex")
                ]
        conn.close()

        latency_ms = round((time.time() - t0) * 1000, 2)
        return HexagonResponse(
            entity=entity,
            resolution=resolution,
            total_hexagons=len(hexagons),
            hexagons=hexagons,
            latency_ms=latency_ms
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Hexagon aggregation query failed: {e}")

@app.get("/spatial/couriers")
def get_live_couriers(
    status: Optional[str] = Query(None, description="Filter by status (e.g. delivering, at_merchant, idle)"),
    limit: int = Query(50, ge=1, le=200)
):
    """Returns the latest courier positions with H3 cells and telemetry."""
    t0 = time.time()
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            status_clause = "WHERE s.h3_res7 IS NOT NULL"
            params: list = []
            if status:
                status_clause += " AND s.status = %s"
                params.append(status)
            params.append(limit)

            sql = f"""
            SELECT 
                c.courier_id,
                s.latitude,
                s.longitude,
                s.speed_kmh,
                s.bearing_deg,
                s.status,
                hex(s.h3_res7) AS h3_res7,
                hex(s.h3_res9) AS h3_res9,
                s.load_date
            FROM {STARROCKS_CATALOG}.vault.sat_courier_telemetry_spatial s
            JOIN {STARROCKS_CATALOG}.vault.hub_courier c ON s.hk_courier_id = c.hk_courier_id
            {status_clause}
            ORDER BY s.load_date DESC
            LIMIT %s;
            """
            cur.execute(sql, params)
            rows = cur.fetchall()
        conn.close()

        couriers = [
            {
                "courier_id": r["courier_id"],
                "latitude": float(r["latitude"]),
                "longitude": float(r["longitude"]),
                "speed_kmh": float(r["speed_kmh"] or 0.0),
                "bearing_deg": int(r["bearing_deg"] or 0),
                "status": r["status"],
                "h3_res7": r["h3_res7"].lower() if r["h3_res7"] else None,
                "h3_res9": r["h3_res9"].lower() if r["h3_res9"] else None,
                "load_date": str(r["load_date"])
            }
            for r in rows
        ]

        return {
            "total": len(couriers),
            "couriers": couriers,
            "latency_ms": round((time.time() - t0) * 1000, 2)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Courier query failed: {e}")

@app.get("/stats")
def lakehouse_stats():
    """Returns row counts across the Data Vault 2.0 tables in StarRocks."""
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            sql = f"""
            SELECT 'hub_user' AS tbl, count(*) AS cnt FROM {STARROCKS_CATALOG}.vault.hub_user
            UNION ALL
            SELECT 'hub_merchant' AS tbl, count(*) AS cnt FROM {STARROCKS_CATALOG}.vault.hub_merchant
            UNION ALL
            SELECT 'hub_courier' AS tbl, count(*) AS cnt FROM {STARROCKS_CATALOG}.vault.hub_courier
            UNION ALL
            SELECT 'link_user_merchant_interaction' AS tbl, count(*) AS cnt FROM {STARROCKS_CATALOG}.vault.link_user_merchant_interaction
            UNION ALL
            SELECT 'sat_interaction_context_spatial' AS tbl, count(*) AS cnt FROM {STARROCKS_CATALOG}.vault.sat_interaction_context_spatial
            UNION ALL
            SELECT 'sat_courier_telemetry_spatial' AS tbl, count(*) AS cnt FROM {STARROCKS_CATALOG}.vault.sat_courier_telemetry_spatial;
            """
            cur.execute(sql)
            rows = cur.fetchall()
        conn.close()
        return {r["tbl"]: r["cnt"] for r in rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch stats: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
