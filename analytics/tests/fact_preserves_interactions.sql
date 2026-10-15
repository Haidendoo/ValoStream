select s.hk_interaction_id
from {{ ref('stg_interactions') }} s
left join {{ ref('fct_interactions') }} f on s.hk_interaction_id = f.hk_interaction_id
where f.hk_interaction_id is null
