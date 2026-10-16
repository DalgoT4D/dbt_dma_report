{{ config(materialized='table', tags=['avpn_dma']) }}

select
    flag_code,
    flag_label,
    flag_description,
    count(distinct assessment_id) filter (where is_flagged) as flagged_organization_count,
    count(distinct assessment_id) as total_organization_count,
    round(
        100.0 * count(distinct assessment_id) filter (where is_flagged)
        / nullif(count(distinct assessment_id), 0),
        2
    ) as flagged_organization_pct
from {{ ref('int_avpn_dma_flags') }}
group by flag_code, flag_label, flag_description
