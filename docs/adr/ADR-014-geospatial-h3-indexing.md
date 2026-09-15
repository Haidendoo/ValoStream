# ADR-014: Geospatial Storage (GeoParquet) and H3 Indexing for Visualization

**Status:** Accepted  
**Date:** 2026-09-15  

## Context
ValoStream clickstream and transactional events frequently carry spatial context (user mobile GPS, merchant store coordinates, delivery geolocations). Processing and visualizing massive geospatial data on a streaming lakehouse introduces two key challenges:
1. **Lakehouse Spatial Pruning Gap:** Vanilla Apache Iceberg does not natively parse spatial bounding boxes (`bbox`) in manifest metadata for partition or file pruning, causing spatial queries to perform costly full-table scans over Parquet data files.
2. **Frontend Rendering Bottleneck:** Streaming millions of raw coordinate points `(latitude, longitude)` directly to web dashboards triggers network congestion and browser GPU memory crashes.

## Decision
Adopt **GeoParquet** for standardized vector geometry storage and Uber's **H3 Hexagonal Hierarchical Spatial Index** for spatial indexing, analytics, and visualization:

1. **Streaming Spatial Computation (Flink + Apache Sedona):**
   * Integrate `sedona-flink` and `com.uber:h3` in Apache Flink stream pipelines.
   * Ingest raw `(latitude, longitude)` and compute:
     - `geom_wkb` (Well-Known Binary compliant with GeoParquet standards).
     - `h3_res7` (`BIGINT`, ~1.2 km² cell area) for macro-level regional filtering and partitioning.
     - `h3_res9` (`BIGINT`, ~0.1 km² cell area) for micro-level spatial sorting and visualization.

2. **Lakehouse Spatial Co-location (Apache Iceberg):**
   * Co-locate geographically adjacent records into the same Parquet data files by applying `SORTED BY (h3_res9)` during Flink writes and Iceberg `rewrite_data_files` maintenance passes.
   * Enables high compression ratios and Iceberg `min/max` integer metadata pruning on `h3_res9`.

3. **Sub-second Spatial Analytics & Serving (StarRocks):**
   * Transform spatial queries from heavy geometric functions (`ST_Contains`, `ST_DWithin`) into fast integer operations (`WHERE h3_res7 = ? GROUP BY h3_res8`).
   * Build asynchronous materialized rollups for interaction density and conversion rates per H3 cell.

4. **GPU-Accelerated Visualization (Deck.gl / Kepler.gl):**
   * Transmit pre-aggregated H3 cells `{hex_id, count, avg_metric}` to the web client.
   * Render directly using Deck.gl's native `H3HexagonLayer`, allowing smooth interactive 3D spatial exploration across millions of interactions without client lag.

## Alternatives Considered
- **Raw Float Coordinates Only (`lat`, `lon`):** Forces StarRocks to run compute-intensive trigonometry on billions of rows; cannot leverage Iceberg metadata pruning.
- **S2 Geometry / Geohash:** Geohash has rectangular cells with significant boundary and distance distortion across latitudes; H3 hexagons feature uniform adjacency (every neighbor is equidistant), ideal for heatmap diffusion and visual analytics.

## Consequences
- Adds minimal storage overhead (~16 bytes per interaction for two `BIGINT` H3 indexes).
- Requires packaging `sedona-flink` and H3 dependency jars into the Flink deployment image.
- Yields order-of-magnitude performance gains (50x–100x) for spatial aggregations and seamless integration with modern geospatial visual tooling.
