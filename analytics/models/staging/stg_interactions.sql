{{ config(distributed_by=['hk_interaction_id']) }}
-- Exact replays collapse; conflicting payloads keep duplicate keys and fail tests.
select distinct hk_interaction_id, load_date, event_type, device_type,
    dwell_time_ms, referrer_page, latitude, longitude, h3_res7, h3_res9
from {{ source('vault', 'sat_interaction_context_spatial') }}
