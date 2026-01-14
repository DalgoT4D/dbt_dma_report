{{ config(materialized='table') }}

with source as (
    select q_18
    from {{ ref('stg_dma_bootcamp') }}
),
mapped as (
    select
        case q_18
            when 1 then '01'
            when 2 then '02'
            when 3 then '03'
            when 4 then '04'
            when 5 then '05'
            when 98 then '98'
        end as category_code,
        case q_18
            when 1 then 'NO skilled staff is present'
            when 2 then 'Only a few staff are skilled'
            when 3 then 'Dedicated person/team in charge is present (e.g. a data manager or senior administrator)'
            when 4 then 'Dedicated skilled analytics roles established with several people are responsible'
            when 5 then 'High level staff commitment across senior, specialist, technical, and administrative levels, with clearly defined roles'
            when 98 then 'Don’t Know'
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
