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
    score.section_code,
    score.section_name,
    score.question_code,
    score.question_label,
    score.display_order,
    score.question_score,
    4::numeric as maximum_question_score,
    score.question_score / 4.0 * 100 as question_score_pct
from assessments
cross join lateral (
    values
        ('C', 'Measurement framework', 'q12', 'Documented Theory of Change or logframe', 112, assessments.score_q12),
        ('C', 'Measurement framework', 'q13', 'Consistent quantifiable indicators', 113, assessments.score_q13),
        ('C', 'Measurement framework', 'q14', 'Indicators linked to the measurement framework', 114, assessments.score_q14),
        ('D', 'Data collection', 'q15', 'Breadth of routinely collected data', 115, assessments.score_q15),
        ('D', 'Data collection', 'q16', 'Data capture maturity', 116, assessments.score_q16),
        ('D', 'Data collection', 'q17', 'Data collection frequency', 117, assessments.score_q17),
        ('D', 'Data collection', 'q18', 'Data quality checks', 118, assessments.score_q18),
        ('D', 'Data collection', 'q19', 'Average self-rated data quality', 119, assessments.score_q19),
        ('E', 'Data storage and governance', 'q20', 'Data storage maturity', 120, assessments.score_q20),
        ('E', 'Data storage and governance', 'q21', 'Data consolidation', 121, assessments.score_q21),
        ('E', 'Data storage and governance', 'q22', 'Data protection practices', 122, assessments.score_q22),
        ('E', 'Data storage and governance', 'q23', 'Knowledge of applicable privacy laws', 123, assessments.score_q23),
        ('E', 'Data storage and governance', 'q24', 'Written data policy', 124, assessments.score_q24),
        ('F', 'Purpose and use of data', 'q25', 'Breadth and frequency of data use', 125, assessments.score_q25),
        ('F', 'Purpose and use of data', 'q26', 'Analysis maturity', 126, assessments.score_q26),
        ('F', 'Purpose and use of data', 'q27', 'Presentation of data and insights', 127, assessments.score_q27),
        ('G', 'Decision-making and leadership', 'q28', 'Use of evidence in significant decisions', 128, assessments.score_q28),
        ('G', 'Decision-making and leadership', 'q29', 'Leadership relationship with data', 129, assessments.score_q29),
        ('G', 'Decision-making and leadership', 'q30', 'Data accountability', 130, assessments.score_q30),
        ('G', 'Decision-making and leadership', 'q31', 'Leadership investment in data', 131, assessments.score_q31),
        ('H', 'Data sharing', 'q33', 'Internal data sharing', 133, assessments.score_q33),
        ('H', 'Data sharing', 'q34', 'External data sharing', 134, assessments.score_q34),
        ('I', 'Tools and tech stack', 'q35', 'Data collection and storage tools', 135, assessments.score_q35),
        ('I', 'Tools and tech stack', 'q36', 'Analysis and visualisation tools', 136, assessments.score_q36),
        ('I', 'Tools and tech stack', 'q37', 'Reporting automation', 137, assessments.score_q37),
        ('J', 'Skills and training', 'q38', 'Data roles', 138, assessments.score_q38),
        ('J', 'Skills and training', 'q39', 'Average staff data skill level', 139, assessments.score_q39),
        ('J', 'Skills and training', 'q40', 'Recent staff data training', 140, assessments.score_q40)
) as score(
    section_code,
    section_name,
    question_code,
    question_label,
    display_order,
    question_score
)

