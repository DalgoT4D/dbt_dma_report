{{ config(materialized='table', tags=['avpn_dma']) }}

with responses as (
    select * from {{ ref('int_avpn_dma_responses') }}
),

question_totals as (
    select
        question_code,
        count(distinct assessment_id) as organizations_answering
    from responses
    group by question_code
)

select
    responses.section_code,
    responses.section_name,
    responses.question_code,
    responses.question_label,
    responses.response_type,
    responses.display_order,
    responses.response_code,
    responses.response_label,
    responses.response_order,
    count(distinct responses.assessment_id) as organization_count,
    question_totals.organizations_answering,
    round(
        100.0 * count(distinct responses.assessment_id)
        / nullif(question_totals.organizations_answering, 0),
        2
    ) as response_pct
from responses
inner join question_totals using (question_code)
group by
    responses.section_code,
    responses.section_name,
    responses.question_code,
    responses.question_label,
    responses.response_type,
    responses.display_order,
    responses.response_code,
    responses.response_label,
    responses.response_order,
    question_totals.organizations_answering

