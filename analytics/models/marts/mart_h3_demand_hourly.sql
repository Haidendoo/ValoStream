{{ config(distributed_by=['h3_cell']) }}
with cells as (
    select date_trunc('hour', event_at) as event_hour, 7 as resolution,
        h3_res7 as h3_cell, event_type, dwell_time_ms, latitude, longitude
    from {{ ref('fct_interactions') }} where h3_res7 is not null
    union all
    select date_trunc('hour', event_at), 9, h3_res9, event_type,
        dwell_time_ms, latitude, longitude
    from {{ ref('fct_interactions') }} where h3_res9 is not null
)
select h3_cell, resolution, event_hour, event_type, count(*) as event_count,
    sum(dwell_time_ms) as total_dwell_time_ms,
    count(dwell_time_ms) as dwell_sample_count,
    avg(latitude) as centroid_latitude, avg(longitude) as centroid_longitude
from cells
group by h3_cell, resolution, event_hour, event_type
