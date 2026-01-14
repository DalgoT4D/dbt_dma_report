{{ config(materialized='table') }}

with source as (
    select * from {{ ref('stg_dma_bootcamp') }}
),
counts as (
    select
        sum(case when coalesce(work_area_a, 0) = 1 then 1 else 0 end) as work_area_a_count,
        sum(case when coalesce(work_area_b, 0) = 1 then 1 else 0 end) as work_area_b_count,
        sum(case when coalesce(work_area_c, 0) = 1 then 1 else 0 end) as work_area_c_count,
        sum(case when coalesce(work_area_d, 0) = 1 then 1 else 0 end) as work_area_d_count,
        sum(case when coalesce(work_area_e, 0) = 1 then 1 else 0 end) as work_area_e_count,
        sum(case when coalesce(work_area_f, 0) = 1 then 1 else 0 end) as work_area_f_count,
        sum(case when coalesce(work_area_g, 0) = 1 then 1 else 0 end) as work_area_g_count,
        sum(case when coalesce(work_area_h, 0) = 1 then 1 else 0 end) as work_area_h_count,
        sum(case when coalesce(work_area_i, 0) = 1 then 1 else 0 end) as work_area_i_count,
        sum(case when coalesce(work_area_j, 0) = 1 then 1 else 0 end) as work_area_j_count,
        sum(case when coalesce(work_area_k, 0) = 1 then 1 else 0 end) as work_area_k_count,
        sum(case when coalesce(work_area_l, 0) = 1 then 1 else 0 end) as work_area_l_count,
        sum(case when coalesce(work_area_m, 0) = 1 then 1 else 0 end) as work_area_m_count,
        sum(case when coalesce(work_area_n, 0) = 1 then 1 else 0 end) as work_area_n_count,
        sum(case when coalesce(work_area_o, 0) = 1 then 1 else 0 end) as work_area_o_count,
        sum(case when coalesce(work_area_p, 0) = 1 then 1 else 0 end) as work_area_p_count,
        sum(case when coalesce(work_area_q, 0) = 1 then 1 else 0 end) as work_area_q_count,
        sum(case when coalesce(work_area_r, 0) = 1 then 1 else 0 end) as work_area_r_count,
        sum(case when coalesce(work_area_s, 0) = 1 then 1 else 0 end) as work_area_s_count,
        sum(case when coalesce(work_area_t, 0) = 1 then 1 else 0 end) as work_area_t_count,
        sum(case when coalesce(work_area_u, 0) = 1 then 1 else 0 end) as work_area_u_count,
        sum(case when coalesce(work_area_v, 0) = 1 then 1 else 0 end) as work_area_v_count,
        sum(case when coalesce(work_area_z, 0) = 1 then 1 else 0 end) as work_area_z_count
    from source
),
final as (
    select *
    from counts, lateral (
        values
            ('Education & Literacy', 'A', work_area_a_count),
            ('Nutrition', 'B', work_area_b_count),
            ('Health & Family Welfare', 'C', work_area_c_count),
            ('Women''s Development & Empowerment', 'D', work_area_d_count),
            ('Climate, Environment & Forests', 'E', work_area_e_count),
            ('Livelihood and Rural Development', 'F', work_area_f_count),
            ('WASH', 'G', work_area_g_count),
            ('Financial Inclusion', 'H', work_area_h_count),
            ('Skills and Training', 'I', work_area_i_count),
            ('Art & Culture', 'J', work_area_j_count),
            ('Rural Development & Poverty Alleviation', 'K', work_area_k_count),
            ('Agriculture and Food Security', 'L', work_area_l_count),
            ('Urban Governance', 'M', work_area_m_count),
            ('Drinking Water', 'N', work_area_n_count),
            ('Human Rights', 'O', work_area_o_count),
            ('Aged/Elderly', 'P', work_area_p_count),
            ('Adolescent and Youth', 'Q', work_area_q_count),
            ('Child Protection and Early Child Development', 'R', work_area_r_count),
            ('Mental Health', 'S', work_area_s_count),
            ('Community Development', 'T', work_area_t_count),
            ('Animal Husbandry', 'U', work_area_u_count),
            ('Poultry', 'V', work_area_v_count),
            ('Others (Specify)', 'Z', work_area_z_count)
    ) as t(work_area, work_area_code, org_count)
)

select
    work_area_code,
    work_area,
    org_count
from final
order by work_area_code
