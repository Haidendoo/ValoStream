{{ config(distributed_by=['hk_user_id']) }}
select hk_user_id, hk_merchant_id, count(*) as interaction_count,
    sum(case when event_type = 'purchase' then 1 else 0 end) as purchase_count,
    max(event_at) as last_interaction_at
from {{ ref('fct_interactions') }}
group by hk_user_id, hk_merchant_id
