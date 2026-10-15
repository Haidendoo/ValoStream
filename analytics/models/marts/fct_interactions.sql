{{ config(distributed_by=['hk_interaction_id']) }}
select s.hk_interaction_id, l.hk_user_id, l.hk_merchant_id,
    u.user_id, m.merchant_id, s.load_date as event_at, s.event_type,
    s.device_type, s.dwell_time_ms, s.referrer_page,
    s.latitude, s.longitude, s.h3_res7, s.h3_res9
from {{ ref('stg_interactions') }} s
join {{ ref('stg_interaction_links') }} l on s.hk_interaction_id = l.hk_interaction_id
join {{ ref('stg_users') }} u on l.hk_user_id = u.hk_user_id
join {{ ref('stg_merchants') }} m on l.hk_merchant_id = m.hk_merchant_id
