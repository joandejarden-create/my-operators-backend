# D.4B Schema Cleanup Plan

**Do not delete Airtable fields yet** without founder authorization.

## Deprecation candidates (111)

- Consumers: Explorer may still read overview_*/op_*/brand_* JSON (those are PRESENTATION — hide from Setup view, keep columns for Explorer).
- Fit: do **not** bind Fit to geo_* / cap_kpi_* / locationType% grids.
- Automations: audit Airtable automations before delete.
- Timing: Phase 1 hide via views; Phase 2 archive; Phase 3 delete after 30 days with zero reads.

### Deprecate list (sample → full in dispositions JSON)

- `brand_signal_audit` — Legacy census/KPI/% without defensible methodology — hide then remove
- `brand_signal_reflag` — Legacy census/KPI/% without defensible methodology — hide then remove
- `brand_signal_franchise_align` — Legacy census/KPI/% without defensible methodology — hide then remove
- `brand_signal_soft_retention` — Legacy census/KPI/% without defensible methodology — hide then remove
- `locationTypeResort` — Legacy census/KPI/% without defensible methodology — hide then remove
- `locationTypeAirport` — Legacy census/KPI/% without defensible methodology — hide then remove
- `marketExpansionRampTimeMonths` — Legacy census/KPI/% without defensible methodology — hide then remove
- `brand_conversion_project_count` — Legacy census/KPI/% without defensible methodology — hide then remove
- `brandedVsIndependentMix` — Legacy census/KPI/% without defensible methodology — hide then remove
- `cap_kpi_operating_model` — Legacy census/KPI/% without defensible methodology — hide then remove
- `cap_kpi_execution_strength` — Legacy census/KPI/% without defensible methodology — hide then remove
- `cap_kpi_transition` — Legacy census/KPI/% without defensible methodology — hide then remove
- `cap_kpi_reporting` — Legacy census/KPI/% without defensible methodology — hide then remove
- `cap_signal_budget` — Legacy census/KPI/% without defensible methodology — hide then remove
- `cap_signal_lift` — Legacy census/KPI/% without defensible methodology — hide then remove
- `cap_signal_trans` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_na_existing_hotels` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_na_existing_rooms` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_na_pipeline_hotels` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_na_pipeline_rooms` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_na_total_hotels` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_na_total_rooms` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_cala_existing_hotels` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_cala_existing_rooms` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_cala_pipeline_hotels` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_cala_pipeline_rooms` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_cala_total_hotels` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_cala_total_rooms` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_eu_existing_hotels` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_eu_existing_rooms` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_eu_pipeline_hotels` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_eu_pipeline_rooms` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_eu_total_hotels` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_eu_total_rooms` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_mea_existing_hotels` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_mea_existing_rooms` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_mea_pipeline_hotels` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_mea_pipeline_rooms` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_mea_total_hotels` — Legacy census/KPI/% without defensible methodology — hide then remove
- `geo_mea_total_rooms` — Legacy census/KPI/% without defensible methodology — hide then remove

_…and 71 more (see field-dispositions.json)_

## MOVE TO CLAIMS

- managementPhilosophy, missionStatement, insuranceCoverage, cap_profile_transition, companyHistory, companyTagline
