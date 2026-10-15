{{ config(distributed_by=['hk_merchant_id']) }}
select hk_merchant_id, merchant_id, min(load_date) as first_seen_at
from {{ source('vault', 'hub_merchant') }}
group by hk_merchant_id, merchant_id
