-- Each resolution counts the same eligible facts, irrespective of fixture volume.
with expected as (
    select 7 as resolution, count(h3_res7) as events from {{ ref('fct_interactions') }}
    union all
    select 9, count(h3_res9) from {{ ref('fct_interactions') }}
), actual as (
    select resolution, sum(event_count) as events
    from {{ ref('mart_h3_demand_hourly') }} group by resolution
)
select e.resolution from expected e
left join actual a on e.resolution = a.resolution
where e.events <> coalesce(a.events, 0)
