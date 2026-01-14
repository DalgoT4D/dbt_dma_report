{{ config(materialized='table') }}

with source as (
    select
        q_28_a,
        q_28_b,
        q_28_c,
        q_28_d,
        q_28_e
    from {{ ref('stg_dma_bootcamp') }}
),
counts as (
    select
        sum(case when coalesce(q_28_a, 0) = 1 then 1 else 0 end) as q28_a_count,
        sum(case when coalesce(q_28_b, 0) = 1 then 1 else 0 end) as q28_b_count,
        sum(case when coalesce(q_28_c, 0) = 1 then 1 else 0 end) as q28_c_count,
        sum(case when coalesce(q_28_d, 0) = 1 then 1 else 0 end) as q28_d_count,
        sum(case when coalesce(q_28_e, 0) = 1 then 1 else 0 end) as q28_e_count
    from source
),
final as (
    select *
    from counts, lateral (
        values
            ('Do not secure data', '01', q28_a_count),
            ('Secure password protected servers', '02', q28_b_count),
            ('Enabling two-factor auth', '03', q28_c_count),
            ('Encryption to protect sensitive info', '04', q28_d_count),
            ('Regular security audits', '05', q28_e_count)
    ) as t(category, category_code, org_count)
)

select
    category_code,
    category,
    org_count
from final
order by category_code
