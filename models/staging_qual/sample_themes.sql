--DBT AUTOMATION has generated this model, please DO NOT EDIT 
--Please make sure you dont change the model name 

{{ config(materialized='table', schema='staging_qual') }}
WITH cte1 as (
SELECT sample_student_data."Response_ID" as response_id, sample_student_data."Response_Text" as response_text, sample_student_data."Stakeholder_Perspective" as stakeholder, CAST(NULL AS TEXT)                             AS theme_extracted  FROM {{source('staging_qual', 'sample_student_data')}})
-- Final SELECT statement combining the outputs of all CTEs
SELECT *
FROM cte1