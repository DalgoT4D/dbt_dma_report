{{ config(materialized='table', tags=['avpn_dma']) }}

select
    maturity_level,
    maturity_band,
    count(*) as organization_count,
    round(
        100.0 * count(*) / nullif(sum(count(*)) over (), 0),
        2
    ) as organization_pct,
    round(avg(overall_score), 2) as average_overall_score,
    round(avg(infrastructure_index), 2) as average_infrastructure_index,
    round(avg(data_use_index), 2) as average_data_use_index,
    round(avg(unsure_pct), 2) as average_unsure_pct
from {{ ref('stg_avpn_dma_assessments') }}
group by maturity_level, maturity_band

