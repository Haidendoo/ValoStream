"""Query plans for validated marts and the legacy raw-vault input."""

from dataclasses import dataclass
import re


def identifier(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", name):
        raise ValueError(f"Invalid StarRocks identifier: {name!r}")
    return f"`{name}`"


@dataclass(frozen=True)
class ServingSource:
    catalog: str = "nessie_main"
    vault_schema: str = "vault"
    mart_schema: str | None = None

    def __post_init__(self):
        for name in (self.catalog, self.vault_schema, self.mart_schema):
            if name is not None:
                identifier(name)

    def raw(self, table):
        return f"{identifier(self.catalog)}.{identifier(self.vault_schema)}.{identifier(table)}"

    def mart(self, table):
        if not self.mart_schema:
            raise ValueError("No mart schema configured")
        return f"`default_catalog`.{identifier(self.mart_schema)}.{identifier(table)}"

    def interactions(self):
        if self.mart_schema:
            return f"SELECT * FROM {self.mart('fct_interactions')}"
        # Replayed hubs/links must not multiply the satellite event count.
        return f"""
            SELECT s.*, m.merchant_id
            FROM (SELECT DISTINCT hk_interaction_id, load_date, latitude, longitude,
                         h3_res7, h3_res9, dwell_time_ms, event_type, device_type, referrer_page
                  FROM {self.raw('sat_interaction_context_spatial')}) s
            JOIN (SELECT DISTINCT hk_interaction_id, hk_merchant_id
                  FROM {self.raw('link_user_merchant_interaction')}) l
              ON s.hk_interaction_id = l.hk_interaction_id
            JOIN (SELECT DISTINCT hk_merchant_id, merchant_id FROM {self.raw('hub_merchant')}) m
              ON l.hk_merchant_id = m.hk_merchant_id
        """

    def couriers(self):
        if self.mart_schema:
            return f"SELECT * FROM {self.mart('current_courier_positions')}"
        return f"""
            SELECT s.hk_courier_id, c.courier_id, s.load_date, s.latitude, s.longitude,
                   s.speed_kmh, s.bearing_deg, s.status, s.h3_res7, s.h3_res9
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY hk_courier_id ORDER BY load_date DESC, status,
                    latitude, longitude, speed_kmh, bearing_deg, h3_res7, h3_res9
                ) AS position_rank
                FROM {self.raw('sat_courier_telemetry_spatial')}
            ) s
            JOIN (SELECT DISTINCT hk_courier_id, courier_id FROM {self.raw('hub_courier')}) c
              ON s.hk_courier_id = c.hk_courier_id
            WHERE s.position_rank = 1
        """

    def recommendations(self, lat, lon, limit, neighbors=()):
        params = [lon, lat]
        h3_clause = ""
        if neighbors:
            h3_clause = "AND h3_res7 IN (" + ",".join(["%s"] * len(neighbors)) + ")"
            params.extend(neighbors)
        params.append(limit)
        return f"""
            WITH interactions AS ({self.interactions()}), merchants AS (
                SELECT merchant_id, COUNT(*) AS interaction_count,
                       ROUND(AVG(latitude), 6) AS avg_lat, ROUND(AVG(longitude), 6) AS avg_lon,
                       HEX(MAX(h3_res7)) AS sample_h3_res7,
                       ROUND(ST_Distance_Sphere(AVG(longitude), AVG(latitude), %s, %s), 1) AS dist_meters
                FROM interactions
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL {h3_clause}
                GROUP BY merchant_id
            )
            SELECT *, ROUND(interaction_count / (1.0 + COALESCE(dist_meters, 1000.0) / 2000.0), 2) AS score
            FROM merchants ORDER BY score DESC, interaction_count DESC, merchant_id LIMIT %s
        """, params

    def hexagons(self, entity, resolution, limit):
        if resolution not in (7, 9) or entity not in ("interactions", "couriers"):
            raise ValueError("Unsupported spatial query")
        column = f"h3_res{resolution}"
        if entity == "couriers":
            return f"""
                WITH positions AS ({self.couriers()})
                SELECT HEX({column}) AS h3_hex, COUNT(*) AS metric_count,
                       AVG(latitude) AS centroid_lat, AVG(longitude) AS centroid_lon,
                       AVG(speed_kmh) AS avg_speed_kmh, COUNT(*) AS active_couriers
                FROM positions WHERE {column} IS NOT NULL
                GROUP BY {column} ORDER BY metric_count DESC, h3_hex LIMIT %s
            """, [limit]
        if self.mart_schema:
            return f"""
                SELECT HEX(h3_cell) AS h3_hex, SUM(event_count) AS metric_count,
                       SUM(centroid_latitude * event_count) / NULLIF(SUM(event_count), 0) AS centroid_lat,
                       SUM(centroid_longitude * event_count) / NULLIF(SUM(event_count), 0) AS centroid_lon,
                       SUM(total_dwell_time_ms) / NULLIF(SUM(dwell_sample_count), 0) AS avg_dwell_ms
                FROM {self.mart('mart_h3_demand_hourly')} WHERE resolution = %s
                GROUP BY h3_cell ORDER BY metric_count DESC, h3_hex LIMIT %s
            """, [resolution, limit]
        return f"""
            SELECT HEX({column}) AS h3_hex, COUNT(*) AS metric_count,
                   AVG(latitude) AS centroid_lat, AVG(longitude) AS centroid_lon,
                   AVG(dwell_time_ms) AS avg_dwell_ms
            FROM (SELECT DISTINCT hk_interaction_id, load_date, latitude, longitude,
                         h3_res7, h3_res9, dwell_time_ms, event_type, device_type, referrer_page
                  FROM {self.raw('sat_interaction_context_spatial')}) s
            WHERE {column} IS NOT NULL
            GROUP BY {column} ORDER BY metric_count DESC, h3_hex LIMIT %s
        """, [limit]

    def live_couriers(self, status, limit):
        predicate, params = "", []
        if status is not None:
            predicate = "AND status = %s"
            params.append(status)
        params.append(limit)
        # Filter after selecting current rows: an old matching status must not reappear.
        return f"""
            WITH positions AS ({self.couriers()})
            SELECT courier_id, latitude, longitude, speed_kmh, bearing_deg, status,
                   HEX(h3_res7) AS h3_res7, HEX(h3_res9) AS h3_res9, load_date
            FROM positions WHERE h3_res7 IS NOT NULL {predicate}
            ORDER BY load_date DESC, courier_id LIMIT %s
        """, params

    def tables(self):
        names = {
            "hub_user": "stg_users", "hub_merchant": "stg_merchants", "hub_courier": "stg_couriers",
            "link_user_merchant_interaction": "stg_interaction_links",
            "sat_interaction_context_spatial": "stg_interactions",
            "sat_courier_telemetry_spatial": "stg_courier_telemetry",
        }
        return {name: self.mart(model) if self.mart_schema else self.raw(name) for name, model in names.items()}

    def readiness_tables(self):
        tables = list(self.tables().values())
        if self.mart_schema:
            tables += [self.mart(name) for name in
                       ("fct_interactions", "current_courier_positions", "mart_h3_demand_hourly")]
        return tables
