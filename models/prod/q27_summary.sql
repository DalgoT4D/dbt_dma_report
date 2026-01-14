{{ config(materialized='table') }}

with source as (
    select
        q_27_a,
        q_27_b,
        q_27_c,
        q_27_d,
        q_27_e,
        q_27_x
    from {{ ref('stg_dma_bootcamp') }}
),
counts as (
    select
        sum(case when coalesce(q_27_a, 0) = 1 then 1 else 0 end) as q27_a_count,
        sum(case when coalesce(q_27_b, 0) = 1 then 1 else 0 end) as q27_b_count,
        sum(case when coalesce(q_27_c, 0) = 1 then 1 else 0 end) as q27_c_count,
        sum(case when coalesce(q_27_d, 0) = 1 then 1 else 0 end) as q27_d_count,
        sum(case when coalesce(q_27_e, 0) = 1 then 1 else 0 end) as q27_e_count,
        sum(case when coalesce(q_27_x, 0) = 1 then 1 else 0 end) as q27_x_count
    from source
),
final as (
    select *
    from counts, lateral (
        values
            ('Do not store data', 'A', q27_a_count),
            ('Physical/hard copies', 'B', q27_b_count),
            ('Local storage in computers', 'C', q27_c_count),
            ('External storage in hard drives', 'D', q27_d_count),
            ('Cloud-based storage', 'E', q27_e_count),
            ('Don’t Know', 'X', q27_x_count)
    ) as t(category, category_code, org_count)
)

select
    category_code,
    category,
    org_count
from final
order by category_code
