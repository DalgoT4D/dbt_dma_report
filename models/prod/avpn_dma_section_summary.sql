{{ config(materialized='table', tags=['avpn_dma']) }}

select
    section_code,
    section_name,
    display_order,
    overall_weight_pct,
    count(distinct assessment_id) as organization_count,
    round(avg(points_earned), 2) as average_points,
    max(maximum_points) as maximum_points,
    round(avg(section_pct), 2) as average_section_pct,
    round(percentile_cont(0.5) within group (order by section_pct)::numeric, 2) as median_section_pct,
    round(min(section_pct), 2) as minimum_section_pct,
    round(max(section_pct), 2) as maximum_section_pct,
    round(avg(weighted_score_contribution), 2) as average_weighted_contribution
from {{ ref('int_avpn_dma_section_scores') }}
group by section_code, section_name, display_order, overall_weight_pct

