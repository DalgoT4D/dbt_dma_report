{{ config(materialized='table') }}

with source as (
    select
        q_19_a,
        q_19_b,
        q_19_c,
        q_19_d,
        q_19_f,
        q_19_g,
        q_19_x,
        q_19_z
    from {{ ref('stg_dma_bootcamp') }}
),
counts as (
    select
        sum(case when coalesce(q_19_a, 0) = 1 then 1 else 0 end) as q19_a_count,
        sum(case when coalesce(q_19_b, 0) = 1 then 1 else 0 end) as q19_b_count,
        sum(case when coalesce(q_19_c, 0) = 1 then 1 else 0 end) as q19_c_count,
        sum(case when coalesce(q_19_d, 0) = 1 then 1 else 0 end) as q19_d_count,
        sum(case when coalesce(q_19_f, 0) = 1 then 1 else 0 end) as q19_f_count,
        sum(case when coalesce(q_19_g, 0) = 1 then 1 else 0 end) as q19_g_count,
        sum(case when coalesce(q_19_x, 0) = 1 then 1 else 0 end) as q19_x_count,
        sum(case when coalesce(q_19_z, 0) = 1 then 1 else 0 end) as q19_z_count
    from source
),
final as (
    select *
    from counts, lateral (
        values
            ('Senior Management', 'A', q19_a_count),
            ('Monitoring and Evaluation team', 'B', q19_b_count),
            ('Project Managers', 'C', q19_c_count),
            ('Finance team', 'D', q19_d_count),
            ('Fundraising team', 'F', q19_f_count),
            ('IT Department', 'G', q19_g_count),
            ('Don’t Know', 'X', q19_x_count),
            ('Others (Specify)', 'Z', q19_z_count)
    ) as t(category, category_code, org_count)
)

select
    category_code,
    category,
    org_count
from final
order by category_code
