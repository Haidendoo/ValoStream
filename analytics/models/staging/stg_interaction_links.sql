{{ config(distributed_by=['hk_interaction_id']) }}
select hk_interaction_id, hk_user_id, hk_merchant_id, min(load_date) as first_seen_at
from {{ source('vault', 'link_user_merchant_interaction') }}
group by hk_interaction_id, hk_user_id, hk_merchant_id
