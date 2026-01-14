{{ config(materialized='table') }}

with source as (
    select
        q_20_a,
        q_20_b,
        q_20_c,
        q_20_d,
        q_20_e,
        q_20_f,
        q_20_g,
        q_20_h,
        q_20_x
    from {{ ref('stg_dma_bootcamp') }}
),
counts as (
    select
        sum(case when coalesce(q_20_a, 0) = 1 then 1 else 0 end) as q20_a_count,
        sum(case when coalesce(q_20_b, 0) = 1 then 1 else 0 end) as q20_b_count,
        sum(case when coalesce(q_20_c, 0) = 1 then 1 else 0 end) as q20_c_count,
        sum(case when coalesce(q_20_d, 0) = 1 then 1 else 0 end) as q20_d_count,
        sum(case when coalesce(q_20_e, 0) = 1 then 1 else 0 end) as q20_e_count,
        sum(case when coalesce(q_20_f, 0) = 1 then 1 else 0 end) as q20_f_count,
        sum(case when coalesce(q_20_g, 0) = 1 then 1 else 0 end) as q20_g_count,
        sum(case when coalesce(q_20_h, 0) = 1 then 1 else 0 end) as q20_h_count,
        sum(case when coalesce(q_20_x, 0) = 1 then 1 else 0 end) as q20_x_count
    from source
),
final as (
    select *
    from counts, lateral (
        values
            ('For Client reporting purpose', 'A', q20_a_count),
            ('For Monitoring and Evaluation', 'B', q20_b_count),
            ('For Resource Allocation', 'C', q20_c_count),
            ('For Needs Assessment', 'D', q20_d_count),
            ('For Risk Management', 'E', q20_e_count),
            ('For evidence-based programme planning to decide future course of action', 'F', q20_f_count),
            ('To track the Progress', 'G', q20_g_count),
            ('To understand stakeholders/beneficiaries needs and preferences', 'H', q20_h_count),
            ('Don’t Know', 'X', q20_x_count)
    ) as t(category, category_code, org_count)
)

select
    category_code,
    category,
    org_count
from final
order by category_code
