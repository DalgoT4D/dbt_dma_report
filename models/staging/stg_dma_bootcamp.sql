{{ config(materialized='view') }}

{% set source_relation = source('staging_dma_bootcamp', 'DMA_CSV_file_format___Sheet_1') %}
{% set columns = adapter.get_columns_in_relation(source_relation) %}

{% set integer_cols = [
    'id',
    'project_id',
    'form_version',
    'consent_q1',
    'consent_q2',
    'consent_q3',
    'consent_q4',
    'dma_con',
    'npo_type',
    'fcra_lic',
    'fcra_lic_year',
    'emp_count_total',
    'emp_count_voint',
    'total_employee',
    'org_est',
    'status'
] %}

{% set numeric_cols = [
    'annual_exp22_23',
    'annual_exp23_24',
    'annual_exp_latest',
    'sec_b_score',
    'sec_c_score',
    'sec_d_score',
    'sec_e_score',
    'sec_f_score',
    'sec_g_score',
    'progress_percentage'
] %}

{% set timestamp_cols_base = [
    'q12_timestamp',
    'q16_timestamp',
    'q17_timestamp',
    'q27_timestamp',
    'q39_timestamp',
    'q43_timestamp'
] %}
{% set timestamp_cols = timestamp_cols_base
    + timestamp_cols_base | map('replace', 'q', 'q_') | list %}

{% set question_integer_cols_base = [
    'q1','q2','q3',
    'q4_a','q4_b','q4_c',
    'q5_a','q5_b','q5_c',
    'q6_a','q6_b','q6_c','q6_d',
    'q7_a','q7_b','q7_c',
    'q8','q9','q10','q11',
    'q12_a','q12_b','q12_c','q12_d','q12_x','q12_z',
    'q13',
    'q14_a','q14_b','q14_c','q14_d','q14_e','q14_f','q14_x',
    'q15',
    'q16_a','q16_b','q16_c','q16_d','q16_e','q16_z',
    'q17_a','q17_b','q17_c','q17_d','q17_e','q17_f','q17_g','q17_h','q17_x',
    'q18',
    'q19_a','q19_b','q19_c','q19_d','q19_f','q19_g','q19_x','q19_z',
    'q20_a','q20_b','q20_c','q20_d','q20_e','q20_f','q20_g','q20_h','q20_x',
    'q21','q22','q23','q24','q25','q26',
    'q27_a','q27_b','q27_c','q27_d','q27_e','q27_x',
    'q28_a','q28_b','q28_c','q28_d','q28_e',
    'q29','q30',
    'q31_a','q31_b','q31_c','q31_d','q31_e',
    'q32','q33',
    'q34','q35','q36',
    'q37_a','q37_b','q37_c','q37_d','q37_e',
    'q38_a','q38_b','q38_c','q38_d','q38_e','q38_f','q38_x',
    'q39_a','q39_b','q39_c','q39_d','q39_e','q39_f','q39_g','q39_x',
    'q40','q41','q42',
    'q43_a','q43_b','q43_c','q43_d','q43_x','q43_z'
] %}
{% set question_integer_cols = question_integer_cols_base
    + question_integer_cols_base | map('replace', 'q', 'q_') | list %}

{% set question_text_cols_base = [
    'q12_oth','q16_oth','q19_oth','q43_oth'
] %}
{% set question_text_cols = question_text_cols_base
    + question_text_cols_base | map('replace', 'q', 'q_') | list %}

with source as (
    select * from {{ source_relation }}
),
cleaned as (
    select
    {%- for col in columns %}
        {%- set col_name = col.name %}
        {%- set col_name_lower = col_name | lower %}
        {%- set quoted_col = adapter.quote(col_name) %}

        {%- if col_name_lower == 'submission_date' %}
            {{ validate_date(quoted_col) }} as submission_date
        {%- elif col_name_lower in timestamp_cols %}
            nullif({{ quoted_col }}, '')::timestamp as {{ col_name_lower }}
        {%- elif col_name_lower in ['created_at', 'updated_at'] %}
            case
                when nullif({{ quoted_col }}, '') is not null
                then to_timestamp(nullif({{ quoted_col }}, '')::double precision)
            end as {{ col_name_lower }}
        {%- elif col_name_lower in numeric_cols %}
            nullif({{ quoted_col }}, '')::numeric as {{ col_name_lower }}
        {%- elif col_name_lower in integer_cols %}
            nullif({{ quoted_col }}, '')::integer as {{ col_name_lower }}
        {%- elif col_name_lower in question_integer_cols %}
            nullif({{ quoted_col }}, '')::integer as {{ col_name_lower }}
        {%- elif col_name_lower in question_text_cols %}
            nullif(trim({{ quoted_col }}::text), '') as {{ col_name_lower }}
        {%- elif col_name_lower.startswith('operation_states_') %}
            nullif({{ quoted_col }}, '')::integer as {{ col_name_lower }}
        {%- elif col_name_lower.startswith('work_area_') %}
            nullif({{ quoted_col }}, '')::integer as {{ col_name_lower }}
        {%- else %}
            nullif(trim({{ quoted_col }}::text), '') as {{ col_name_lower }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {% endfor %}
    from source
)

select * from cleaned
