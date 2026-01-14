{{ config(materialized='table') }}

with source as (
    select
        q_38_a,
        q_38_b,
        q_38_c,
        q_38_d,
        q_38_e,
        q_38_f,
        q_38_x
    from {{ ref('stg_dma_bootcamp') }}
),
counts as (
    select
        sum(case when coalesce(q_38_a, 0) = 1 then 1 else 0 end) as q38_a_count,
        sum(case when coalesce(q_38_b, 0) = 1 then 1 else 0 end) as q38_b_count,
        sum(case when coalesce(q_38_c, 0) = 1 then 1 else 0 end) as q38_c_count,
        sum(case when coalesce(q_38_d, 0) = 1 then 1 else 0 end) as q38_d_count,
        sum(case when coalesce(q_38_e, 0) = 1 then 1 else 0 end) as q38_e_count,
        sum(case when coalesce(q_38_f, 0) = 1 then 1 else 0 end) as q38_f_count,
        sum(case when coalesce(q_38_x, 0) = 1 then 1 else 0 end) as q38_x_count
    from source
),
final as (
    select *
    from counts, lateral (
        values
            ('Do not analyse Project data', 'A', q38_a_count),
            ('Excel/Google Sheets', 'B', q38_b_count),
            ('R Programming', 'C', q38_c_count),
            ('Python', 'D', q38_d_count),
            ('ATLAS.ti/NVivo', 'E', q38_e_count),
            ('SPSS/STATA/SAS', 'F', q38_f_count),
            ('Don’t Know', 'X', q38_x_count)
    ) as t(category, category_code, org_count)
)

select
    category_code,
    category,
    org_count
from final
order by category_code
