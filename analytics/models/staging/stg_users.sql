{{ config(distributed_by=['hk_user_id']) }}
select hk_user_id, user_id, min(load_date) as first_seen_at
from {{ source('vault', 'hub_user') }}
group by hk_user_id, user_id
