select hk_courier_id from {{ ref('stg_courier_telemetry') }}
where latitude is null or longitude is null
    or not (latitude between -90 and 90)
    or not (longitude between -180 and 180)
    or speed_kmh < 0 or bearing_deg < 0 or bearing_deg >= 360
