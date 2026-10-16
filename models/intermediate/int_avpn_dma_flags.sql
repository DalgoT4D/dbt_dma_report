{{ config(materialized='table', tags=['avpn_dma']) }}

with assessments as (
    select * from {{ ref('stg_avpn_dma_assessments') }}
)

select
    assessments.assessment_id,
    assessments.assessment_date,
    assessments.organization_name,
    assessments.thematic_area,
    assessments.staff_size,
    assessments.maturity_band,
    assessments.overall_score,
    flag.flag_code,
    flag.flag_label,
    flag.flag_description,
    flag.is_flagged
from assessments
cross join lateral (
    values
        ('no_reach_tracking', 'Reach is not tracked', 'The organisation reports that it does not track annual reach.', assessments.flag_no_reach_tracking),
        ('low_confidence', 'Low response confidence', 'More than 30% of confidence-sensitive responses were marked unsure.', assessments.flag_low_confidence),
        ('capability_gap', 'Infrastructure exceeds use', 'Infrastructure maturity is more than 20 points above data-use maturity.', assessments.flag_capability_gap),
        ('inverse_gap', 'Use exceeds infrastructure', 'Data-use maturity is more than 20 points above infrastructure maturity.', assessments.flag_inverse_gap),
        ('framework_gap', 'Framework is not connected', 'A Theory of Change exists, but indicators are not adequately linked to it.', assessments.flag_framework_gap),
        ('single_point', 'Single point of failure', 'Storage is weak and data accountability is absent or distributed.', assessments.flag_single_point),
        ('no_training', 'No recent data training', 'No data training topics were selected for the last 12 months.', assessments.flag_no_training)
) as flag(flag_code, flag_label, flag_description, is_flagged)

