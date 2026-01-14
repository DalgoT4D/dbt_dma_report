{{ config(materialized='table') }}

with source as (
    select q_24
    from {{ ref('stg_dma_bootcamp') }}
),
mapped as (
    select
        case q_24
            when 1 then '01'
            when 2 then '02'
            when 3 then '03'
            when 4 then '04'
            when 5 then '05'
            when 6 then '06'
        end as category_code,
        case q_24
            when 1 then 'Weekly'
            when 2 then 'Fortnightly'
            when 3 then 'Monthly'
            when 4 then 'Quarterly'
            when 5 then 'Half-Yearly'
            when 6 then 'Annually'
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
