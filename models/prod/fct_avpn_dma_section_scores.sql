{{ config(materialized='table', tags=['avpn_dma']) }}

select *
from {{ ref('int_avpn_dma_section_scores') }}

