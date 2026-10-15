{{ config(distributed_by=['hk_courier_id']) }}
with ranked as (
    select *, row_number() over (
        partition by hk_courier_id order by load_date desc
    ) as position_rank
    from {{ ref('stg_courier_telemetry') }}
)
select s.hk_courier_id, c.courier_id, s.load_date, s.latitude, s.longitude,
    s.speed_kmh, s.bearing_deg, s.status, s.h3_res7, s.h3_res9
from ranked s
join {{ ref('stg_couriers') }} c on s.hk_courier_id = c.hk_courier_id
where position_rank = 1
