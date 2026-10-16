{{ config(materialized='table', tags=['avpn_dma']) }}

with source as (
    select "data"::jsonb as payload
    from {{ source('staging', 'avpn_digital_data_maturity_assessment') }}
    where "data" is not null
),

ranked as (
    select
        payload,
        row_number() over (
            partition by coalesce(
                nullif(payload ->> '_uuid', ''),
                nullif(payload ->> 'meta/instanceID', ''),
                payload ->> '_id'
            )
            order by nullif(payload ->> '_submission_time', '')::timestamptz desc nulls last
        ) as submission_rank
    from source
),

typed as (
    select
        coalesce(
            nullif(payload ->> '_uuid', ''),
            nullif(replace(payload ->> 'meta/instanceID', 'uuid:', ''), ''),
            payload ->> '_id'
        ) as assessment_id,
        payload ->> '_id' as source_record_id,
        payload ->> '_status' as submission_status,
        nullif(payload ->> '_submission_time', '')::timestamptz as submitted_at,
        nullif(payload ->> 'start_time', '')::timestamptz as started_at,
        nullif(payload ->> 'end_time', '')::timestamptz as completed_at,
        nullif(payload ->> 'today_date', '')::date as assessment_date,
        nullif(trim(payload ->> 'sec_a/q1_org_name'), '') as organization_name,
        nullif(trim(payload ->> 'sec_a/q2_respondent'), '') as respondent_name,
        nullif(lower(trim(payload ->> 'sec_a/q3_email')), '') as respondent_email,
        nullif(trim(payload ->> 'sec_a/q4_location'), '') as operating_location,
        payload ->> 'sec_a/q5_thematic' as thematic_area_code,
        payload ->> 'sec_a/q5_thematic_other' as thematic_area_other,
        payload ->> 'sec_a/q6_staff' as staff_size_code,
        payload ->> 'sec_a/q7_expenditure' as expenditure_band_code,
        payload ->> 'sec_b/q8_programmes' as programme_count_code,
        payload ->> 'sec_b/q9_reach' as annual_reach_code,
        payload ->> 'sec_b/q10_data_staff' as data_staff_count_code,
        payload ->> 'sec_b/q11_partners' as partner_delivery_code,

        payload ->> 'sec_c/q12_toc' as q12_toc_code,
        payload ->> 'sec_c/q13_indicators' as q13_indicators_code,
        payload ->> 'sec_c/q14_linked' as q14_linked_code,
        payload ->> 'sec_d/q15_data_types' as q15_data_types_codes,
        payload ->> 'sec_d/q16_capture' as q16_capture_codes,
        payload ->> 'sec_d/q17_frequency' as q17_frequency_code,
        payload ->> 'sec_d/q18_checks' as q18_checks_codes,
        payload ->> 'sec_d/q19/q19_accuracy' as q19_accuracy_code,
        case when payload ->> 'sec_d/q19/q19_accuracy' ~ '^[1-5]$' then (payload ->> 'sec_d/q19/q19_accuracy')::numeric end as q19_accuracy_rating,
        payload ->> 'sec_d/q19/q19_completeness' as q19_completeness_code,
        case when payload ->> 'sec_d/q19/q19_completeness' ~ '^[1-5]$' then (payload ->> 'sec_d/q19/q19_completeness')::numeric end as q19_completeness_rating,
        payload ->> 'sec_d/q19/q19_timeliness' as q19_timeliness_code,
        case when payload ->> 'sec_d/q19/q19_timeliness' ~ '^[1-5]$' then (payload ->> 'sec_d/q19/q19_timeliness')::numeric end as q19_timeliness_rating,
        payload ->> 'sec_d/q19/q19_consistency' as q19_consistency_code,
        case when payload ->> 'sec_d/q19/q19_consistency' ~ '^[1-5]$' then (payload ->> 'sec_d/q19/q19_consistency')::numeric end as q19_consistency_rating,
        payload ->> 'sec_e/q20_storage' as q20_storage_codes,
        payload ->> 'sec_e/q21_consolidated' as q21_consolidated_code,
        payload ->> 'sec_e/q22_protection' as q22_protection_codes,
        payload ->> 'sec_e/q23_law' as q23_law_code,
        payload ->> 'sec_e/q24_policy' as q24_policy_code,
        payload ->> 'sec_f/q25/q25_design' as q25_design_code,
        case when payload ->> 'sec_f/q25/q25_design' ~ '^[1-5]$' then (payload ->> 'sec_f/q25/q25_design')::numeric end as q25_design_rating,
        payload ->> 'sec_f/q25/q25_monitoring' as q25_monitoring_code,
        case when payload ->> 'sec_f/q25/q25_monitoring' ~ '^[1-5]$' then (payload ->> 'sec_f/q25/q25_monitoring')::numeric end as q25_monitoring_rating,
        payload ->> 'sec_f/q25/q25_outcomes' as q25_outcomes_code,
        case when payload ->> 'sec_f/q25/q25_outcomes' ~ '^[1-5]$' then (payload ->> 'sec_f/q25/q25_outcomes')::numeric end as q25_outcomes_rating,
        payload ->> 'sec_f/q25/q25_budgets' as q25_budgets_code,
        case when payload ->> 'sec_f/q25/q25_budgets' ~ '^[1-5]$' then (payload ->> 'sec_f/q25/q25_budgets')::numeric end as q25_budgets_rating,
        payload ->> 'sec_f/q25/q25_staffing' as q25_staffing_code,
        case when payload ->> 'sec_f/q25/q25_staffing' ~ '^[1-5]$' then (payload ->> 'sec_f/q25/q25_staffing')::numeric end as q25_staffing_rating,
        payload ->> 'sec_f/q25/q25_fundraising' as q25_fundraising_code,
        case when payload ->> 'sec_f/q25/q25_fundraising' ~ '^[1-5]$' then (payload ->> 'sec_f/q25/q25_fundraising')::numeric end as q25_fundraising_rating,
        payload ->> 'sec_f/q25/q25_advocacy' as q25_advocacy_code,
        case when payload ->> 'sec_f/q25/q25_advocacy' ~ '^[1-5]$' then (payload ->> 'sec_f/q25/q25_advocacy')::numeric end as q25_advocacy_rating,
        payload ->> 'sec_f/q26_analysis' as q26_analysis_code,
        payload ->> 'sec_f/q27_presentation' as q27_presentation_codes,
        payload ->> 'sec_g/q28_decisions' as q28_decisions_code,
        payload ->> 'sec_g/q29_leadership' as q29_leadership_code,
        payload ->> 'sec_g/q30_accountability' as q30_accountability_code,
        payload ->> 'sec_g/q31_budget' as q31_budget_code,
        payload ->> 'sec_g/q32_barriers' as q32_barriers_codes,
        payload ->> 'sec_h/q33_internal' as q33_internal_code,
        payload ->> 'sec_h/q34_external' as q34_external_codes,
        payload ->> 'sec_i/q35_collect_tools' as q35_collect_tools_codes,
        payload ->> 'sec_i/q36_analysis_tools' as q36_analysis_tools_codes,
        payload ->> 'sec_i/q37_automation' as q37_automation_code,
        payload ->> 'sec_j/q38_roles' as q38_roles_codes,
        payload ->> 'sec_j/q39/q39_leadership' as q39_leadership_code,
        case when payload ->> 'sec_j/q39/q39_leadership' ~ '^[1-5]$' then (payload ->> 'sec_j/q39/q39_leadership')::numeric end as q39_leadership_rating,
        payload ->> 'sec_j/q39/q39_programme' as q39_programme_code,
        case when payload ->> 'sec_j/q39/q39_programme' ~ '^[1-5]$' then (payload ->> 'sec_j/q39/q39_programme')::numeric end as q39_programme_rating,
        payload ->> 'sec_j/q39/q39_field' as q39_field_code,
        case when payload ->> 'sec_j/q39/q39_field' ~ '^[1-5]$' then (payload ->> 'sec_j/q39/q39_field')::numeric end as q39_field_rating,
        payload ->> 'sec_j/q39/q39_finance' as q39_finance_code,
        case when payload ->> 'sec_j/q39/q39_finance' ~ '^[1-5]$' then (payload ->> 'sec_j/q39/q39_finance')::numeric end as q39_finance_rating,
        payload ->> 'sec_j/q40_training' as q40_training_codes,
        payload ->> 'sec_j/q41_difficulty' as q41_difficulty_codes,

        nullif(payload ->> 's_q12_toc', '')::numeric as score_q12,
        nullif(payload ->> 's_q13_indicators', '')::numeric as score_q13,
        nullif(payload ->> 's_q14_linked', '')::numeric as score_q14,
        nullif(payload ->> 's_q15_data_types', '')::numeric as score_q15,
        nullif(payload ->> 's_q16_capture', '')::numeric as score_q16,
        nullif(payload ->> 's_q17_frequency', '')::numeric as score_q17,
        nullif(payload ->> 's_q18_checks', '')::numeric as score_q18,
        nullif(payload ->> 's_q19', '')::numeric as score_q19,
        nullif(payload ->> 's_q20_storage', '')::numeric as score_q20,
        nullif(payload ->> 's_q21_consolidated', '')::numeric as score_q21,
        nullif(payload ->> 's_q22_protection', '')::numeric as score_q22,
        nullif(payload ->> 's_q23_law', '')::numeric as score_q23,
        nullif(payload ->> 's_q24_policy', '')::numeric as score_q24,
        nullif(payload ->> 's_q25', '')::numeric as score_q25,
        nullif(payload ->> 's_q26_analysis', '')::numeric as score_q26,
        nullif(payload ->> 's_q27_presentation', '')::numeric as score_q27,
        nullif(payload ->> 's_q28_decisions', '')::numeric as score_q28,
        nullif(payload ->> 's_q29_leadership', '')::numeric as score_q29,
        nullif(payload ->> 's_q30_accountability', '')::numeric as score_q30,
        nullif(payload ->> 's_q31_budget', '')::numeric as score_q31,
        nullif(payload ->> 's_q33_internal', '')::numeric as score_q33,
        nullif(payload ->> 's_q34_external', '')::numeric as score_q34,
        nullif(payload ->> 's_q35_collect_tools', '')::numeric as score_q35,
        nullif(payload ->> 's_q36_analysis_tools', '')::numeric as score_q36,
        nullif(payload ->> 's_q37_automation', '')::numeric as score_q37,
        nullif(payload ->> 's_q38_roles', '')::numeric as score_q38,
        nullif(payload ->> 's_q39', '')::numeric as score_q39,
        nullif(payload ->> 's_q40_training', '')::numeric as score_q40,

        nullif(payload ->> 'pts_c', '')::numeric as section_c_points,
        nullif(payload ->> 'pct_c', '')::numeric as section_c_pct,
        nullif(payload ->> 'pts_d', '')::numeric as section_d_points,
        nullif(payload ->> 'pct_d', '')::numeric as section_d_pct,
        nullif(payload ->> 'pts_e', '')::numeric as section_e_points,
        nullif(payload ->> 'pct_e', '')::numeric as section_e_pct,
        nullif(payload ->> 'pts_f', '')::numeric as section_f_points,
        nullif(payload ->> 'pct_f', '')::numeric as section_f_pct,
        nullif(payload ->> 'pts_g', '')::numeric as section_g_points,
        nullif(payload ->> 'pct_g', '')::numeric as section_g_pct,
        nullif(payload ->> 'pts_h', '')::numeric as section_h_points,
        nullif(payload ->> 'pct_h', '')::numeric as section_h_pct,
        nullif(payload ->> 'pts_i', '')::numeric as section_i_points,
        nullif(payload ->> 'pct_i', '')::numeric as section_i_pct,
        nullif(payload ->> 'pts_j', '')::numeric as section_j_points,
        nullif(payload ->> 'pct_j', '')::numeric as section_j_pct,
        nullif(payload ->> 'raw_total', '')::numeric as raw_total,
        nullif(payload ->> 'overall_score', '')::numeric as overall_score,
        nullif(payload ->> 'maturity_level', '')::integer as maturity_level,
        payload ->> 'maturity_band' as maturity_band,
        payload ->> 'band_meaning' as maturity_band_meaning,
        nullif(payload ->> 'infra_index', '')::numeric as infrastructure_index,
        nullif(payload ->> 'use_index', '')::numeric as data_use_index,
        nullif(payload ->> 'unsure_count', '')::integer as unsure_count,
        nullif(payload ->> 'unsure_pct', '')::numeric as unsure_pct,
        nullif(payload ->> 'flag_count', '')::integer as flag_count,
        coalesce((payload ->> 'flag_no_reach_tracking') = 'yes', false) as flag_no_reach_tracking,
        coalesce((payload ->> 'flag_low_confidence') = 'yes', false) as flag_low_confidence,
        coalesce((payload ->> 'flag_capability_gap') = 'yes', false) as flag_capability_gap,
        coalesce((payload ->> 'flag_inverse_gap') = 'yes', false) as flag_inverse_gap,
        coalesce((payload ->> 'flag_framework_gap') = 'yes', false) as flag_framework_gap,
        coalesce((payload ->> 'flag_single_point') = 'yes', false) as flag_single_point,
        coalesce((payload ->> 'flag_no_training') = 'yes', false) as flag_no_training,
        payload as raw_payload
    from ranked
    where submission_rank = 1
)

select
    *,
    case thematic_area_code
        when 'education' then 'Education'
        when 'health_nutrition' then 'Health & Nutrition'
        when 'livelihoods' then 'Livelihoods & Skilling'
        when 'agri_environment' then 'Agriculture & Environment'
        when 'wash' then 'WASH'
        when 'gender_justice' then 'Gender & Social Justice'
        when 'financial_inclusion' then 'Financial Inclusion'
        when 'disability' then 'Disability & Inclusion'
        when 'governance_civictech' then 'Governance & Civic Tech'
        when 'other' then coalesce(nullif(thematic_area_other, ''), 'Other')
    end as thematic_area,
    case staff_size_code
        when 'lt10' then 'Fewer than 10'
        when 's10_50' then '10-50'
        when 's51_200' then '51-200'
        when 's201_500' then '201-500'
        when 'gt500' then 'More than 500'
    end as staff_size,
    case expenditure_band_code
        when 'lt100k' then 'Less than US$100,000'
        when 'k100_500' then 'US$100,000 - 500,000'
        when 'k500_2m' then 'US$500,001 - 2 million'
        when 'm2_10' then 'US$2 - 10 million'
        when 'gt10m' then 'More than US$10 million'
        when 'prefer_not' then 'Prefer not to say'
    end as expenditure_band,
    case programme_count_code
        when 'one' then '1'
        when 'p2_5' then '2-5'
        when 'p6_15' then '6-15'
        when 'gt15' then 'More than 15'
    end as programme_count,
    case annual_reach_code
        when 'lt1k' then 'Under 1,000'
        when 'r1k_10k' then '1,000 - 10,000'
        when 'r10k_100k' then '10,001 - 100,000'
        when 'r100k_1m' then '100,001 - 1,000,000'
        when 'gt1m' then 'More than 1,000,000'
        when 'do_not_track' then 'We do not track this'
    end as annual_reach,
    case data_staff_count_code
        when 'none' then 'None'
        when 's1_10' then '1-10'
        when 's11_50' then '11-50'
        when 's51_200' then '51-200'
        when 'gt200' then 'More than 200'
    end as data_staff_count,
    case partner_delivery_code
        when 'routinely' then 'Yes, routinely'
        when 'occasionally' then 'Occasionally'
        when 'no' then 'No'
        when 'unsure' then 'Unsure'
    end as partner_delivery
from typed
