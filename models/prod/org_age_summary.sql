{{ config(materialized='table') }}

with source as (
    select
        org_est
    from {{ ref('stg_dma_bootcamp') }}
    where org_est is not null
),
ages as (
    select
        date_part('year', age(current_date, make_date(org_est, 1, 1)))::int as org_age_years
    from source
),
categorized as (
    select
        case
            when org_age_years < 2 then '0-2yrs'
            when org_age_years < 5 then '2-5yrs'
            when org_age_years < 10 then '5-10yrs'
            else '10yrs+'
        end as org_age_category
    from ages
)

select
    org_age_category,
    count(*) as org_count
from categorized
group by org_age_category
order by org_age_category
