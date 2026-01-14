{{ config(materialized='table') }}

with source as (
    select q_21
    from {{ ref('stg_dma_bootcamp') }}
),
mapped as (
    select
        case q_21
            when 1 then '01'
            when 2 then '02'
            when 3 then '03'
            when 4 then '04'
            when 5 then '05'
        end as category_code,
        case q_21
            when 1 then 'Manually on paper'
            when 2 then 'Inconsistent: paper & digital form'
            when 3 then 'Somewhat structured processes'
            when 4 then 'Systematically & consistently collected'
            when 5 then 'Automated Collection & well trained staff '
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
