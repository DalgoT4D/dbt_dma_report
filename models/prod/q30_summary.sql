{{ config(materialized='table') }}

with source as (
    select q_30
    from {{ ref('stg_dma_bootcamp') }}
),
mapped as (
    select
        case q_30
            when 1 then '01'
            when 2 then '02'
        end as category_code,
        case q_30
            when 1 then 'Yes'
            when 2 then 'No'
        end as category_label
    from source
)

select
    category_code,
    category_label,
    count(*) as org_count
from mapped
where category_code is not null
group by category_code, category_label
order by category_code
