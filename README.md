# dbt_dma_report
DMA dashboard DBT Repository 
This repository contains the dbt project for Data Maturity Assessment report dashboard generation, which is designed to transform and model question response data for aggregated reporting purposes.

### Using this project

Try running the following commands:
- dbt run
- dbt test


### AVPN Digital Data Maturity Assessment

The AVPN models read the Kobo JSON stored in
`staging."AVPN_Digital_Data_Maturity_Assessment"."data"`. Run the complete
model chain and its tests with:

```shell
dbt build --select tag:avpn_dma
```

Use these production models for dashboards:

- `fct_avpn_dma_organization_detail`: preferred wide dashboard table with one
  row per assessment/organisation, all labeled responses, scores and flags.
- `fct_avpn_dma_assessments`: one row per organisation, with profile segments,
  overall and section scores, maturity, indices and diagnostic flags.
- `fct_avpn_dma_responses`: one row per organisation, question and selected
  option. Multi-select responses are already split into rows and labeled.
- `fct_avpn_dma_question_scores`: one row per organisation and scored question.
- `fct_avpn_dma_section_scores`: one row per organisation and scored section.
- `fct_avpn_dma_flags`: one row per organisation and diagnostic flag.
- `avpn_dma_*_summary`: ready-made aggregate tables for response, question,
  section, maturity and flag overview charts.

Respondent name, respondent email and raw JSON remain only in restricted
staging. They are intentionally excluded from production dashboard models.

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

