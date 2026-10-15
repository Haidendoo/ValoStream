{{ config(distributed_by=['hk_courier_id']) }}
select hk_courier_id, courier_id, min(load_date) as first_seen_at
from {{ source('vault', 'hub_courier') }}
group by hk_courier_id, courier_id
