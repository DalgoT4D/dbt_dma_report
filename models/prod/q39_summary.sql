{{ config(materialized='table') }}

with source as (
    select
        q_39_a,
        q_39_b,
        q_39_c,
        q_39_d,
        q_39_e,
        q_39_f,
        q_39_g,
        q_39_x
    from {{ ref('stg_dma_bootcamp') }}
),
counts as (
    select
        sum(case when coalesce(q_39_a, 0) = 1 then 1 else 0 end) as q39_a_count,
        sum(case when coalesce(q_39_b, 0) = 1 then 1 else 0 end) as q39_b_count,
        sum(case when coalesce(q_39_c, 0) = 1 then 1 else 0 end) as q39_c_count,
        sum(case when coalesce(q_39_d, 0) = 1 then 1 else 0 end) as q39_d_count,
        sum(case when coalesce(q_39_e, 0) = 1 then 1 else 0 end) as q39_e_count,
        sum(case when coalesce(q_39_f, 0) = 1 then 1 else 0 end) as q39_f_count,
        sum(case when coalesce(q_39_g, 0) = 1 then 1 else 0 end) as q39_g_count,
        sum(case when coalesce(q_39_x, 0) = 1 then 1 else 0 end) as q39_x_count
    from source
),
final as (
    select *
    from counts, lateral (
        values
            ('Do not report and visualise Project data', 'A', q39_a_count),
            ('Tableau/Power BI', 'B', q39_b_count),
            ('Power Point', 'C', q39_c_count),
            ('Canva', 'D', q39_d_count),
            ('Excel', 'E', q39_e_count),
            ('R Programming', 'F', q39_f_count),
            ('STATA', 'G', q39_g_count),
            ('Don’t Know', 'X', q39_x_count)
    ) as t(category, category_code, org_count)
)

select
    category_code,
    category,
    org_count
from final
order by category_code
