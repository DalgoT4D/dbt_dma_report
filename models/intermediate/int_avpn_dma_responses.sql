{{ config(materialized='table', tags=['avpn_dma']) }}

with assessments as (
    select * from {{ ref('stg_avpn_dma_assessments') }}
),

questions as (
    select * from {{ ref('int_avpn_dma_question_catalog') }}
),

choices as (
    select * from {{ ref('int_avpn_dma_choice_catalog') }}
),

answers as (
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
        questions.section_code,
        questions.section_name,
        questions.question_code,
        questions.question_label,
        questions.response_type,
        questions.choice_list_name,
        questions.display_order,
        questions.is_scored,
        nullif(trim(assessments.raw_payload ->> questions.json_key), '') as raw_response
    from assessments
    cross join questions
),

expanded as (
    select
        answers.*,
        answers.raw_response as response_code
    from answers
    where answers.response_type <> 'select_multiple'

    union all

    select
        answers.*,
        selected.response_code
    from answers
    cross join lateral regexp_split_to_table(
        answers.raw_response,
        '[[:space:]]+'
    ) as selected(response_code)
    where answers.response_type = 'select_multiple'
)

select
    expanded.assessment_id,
    expanded.assessment_date,
    expanded.organization_name,
    expanded.operating_location,
    expanded.thematic_area,
    expanded.staff_size,
    expanded.expenditure_band,
    expanded.programme_count,
    expanded.annual_reach,
    expanded.data_staff_count,
    expanded.partner_delivery,
    expanded.maturity_level,
    expanded.maturity_band,
    expanded.overall_score,
    expanded.section_code,
    expanded.section_name,
    expanded.question_code,
    expanded.question_label,
    expanded.response_type,
    expanded.display_order,
    expanded.is_scored,
    expanded.response_code,
    coalesce(choices.option_label, expanded.response_code) as response_label,
    choices.option_order as response_order,
    case
        when expanded.response_code ~ '^[0-9]+([.][0-9]+)?$'
        then expanded.response_code::numeric
    end as response_numeric,
    expanded.response_code = 'unsure' as is_unsure
from expanded
left join choices
    on expanded.choice_list_name = choices.list_name
    and expanded.response_code = choices.option_code
where expanded.response_code is not null

