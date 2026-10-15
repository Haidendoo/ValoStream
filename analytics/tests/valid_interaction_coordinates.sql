select hk_interaction_id from {{ ref('stg_interactions') }}
where latitude is null or longitude is null
    or not (latitude between -90 and 90)
    or not (longitude between -180 and 180)
    or dwell_time_ms < 0
