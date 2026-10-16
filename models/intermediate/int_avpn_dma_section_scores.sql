{{ config(materialized='table', tags=['avpn_dma']) }}

with assessments as (
    select * from {{ ref('stg_avpn_dma_assessments') }}
)

select
    assessments.assessment_id,
    assessments.assessment_date,
    assessments.organization_name,
    assessments.operating_location,
    assessments.thematic_area,
    assessments.staff_size,
    assessments.expenditure_band,
    assessments.programme_count,
    assessments.annual_reach,
    assessments.data_staff_count,
    assessments.partner_delivery,
    assessments.maturity_level,
    assessments.maturity_band,
    assessments.overall_score,
    assessments.infrastructure_index,
    assessments.data_use_index,
    section.section_code,
    section.section_name,
    section.display_order,
    section.points_earned,
    section.maximum_points,
    section.section_pct,
    section.overall_weight_pct,
    section.section_pct * section.overall_weight_pct / 100.0 as weighted_score_contribution,
    case
        when section.section_pct <= 20 then 'Nascent'
        when section.section_pct <= 40 then 'Emerging'
        when section.section_pct <= 60 then 'Developing'
        when section.section_pct <= 80 then 'Established'
        else 'Advanced'
    end as section_performance_band
from assessments
cross join lateral (
    values
        ('C', 'Measurement framework', 3, assessments.section_c_points, 12::numeric, assessments.section_c_pct, 10::numeric),
        ('D', 'Data collection', 4, assessments.section_d_points, 20::numeric, assessments.section_d_pct, 15::numeric),
        ('E', 'Data storage and governance', 5, assessments.section_e_points, 20::numeric, assessments.section_e_pct, 12::numeric),
        ('F', 'Purpose and use of data', 6, assessments.section_f_points, 12::numeric, assessments.section_f_pct, 20::numeric),
        ('G', 'Decision-making and leadership', 7, assessments.section_g_points, 16::numeric, assessments.section_g_pct, 20::numeric),
        ('H', 'Data sharing', 8, assessments.section_h_points, 8::numeric, assessments.section_h_pct, 5::numeric),
        ('I', 'Tools and tech stack', 9, assessments.section_i_points, 12::numeric, assessments.section_i_pct, 8::numeric),
        ('J', 'Skills and training', 10, assessments.section_j_points, 12::numeric, assessments.section_j_pct, 10::numeric)
) as section(
    section_code,
    section_name,
    display_order,
    points_earned,
    maximum_points,
    section_pct,
    overall_weight_pct
)

