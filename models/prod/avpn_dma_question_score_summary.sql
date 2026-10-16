{{ config(materialized='table', tags=['avpn_dma']) }}

select
    section_code,
    section_name,
    question_code,
    question_label,
    display_order,
    count(distinct assessment_id) as organization_count,
    round(avg(question_score), 2) as average_question_score,
    max(maximum_question_score) as maximum_question_score,
    round(avg(question_score_pct), 2) as average_question_score_pct,
    round(percentile_cont(0.5) within group (order by question_score)::numeric, 2) as median_question_score
from {{ ref('int_avpn_dma_question_scores') }}
group by section_code, section_name, question_code, question_label, display_order
