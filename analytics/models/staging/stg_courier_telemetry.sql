{{ config(distributed_by=['hk_courier_id']) }}
select distinct hk_courier_id, load_date, latitude, longitude, speed_kmh,
    bearing_deg, status, h3_res7, h3_res9
from {{ source('vault', 'sat_courier_telemetry_spatial') }}
