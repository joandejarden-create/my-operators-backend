# D.4B Visible Empty Audit

Production operators: **36**. Completeness is NOT measured on legacy 67/130 columns.

| Table | Field | Type | Production Fill | Disposition | Why Blank | Should Exist? |
| ----- | ----- | ---- | --------------- | ----------- | --------- | ------------- |
| Profile & Positioning | company_name | singleLineText | 100% (36/36) | CORE REQUIRED | — | Yes/Retain or Master |
| Profile & Positioning | companyDescription | multilineText | 66.7% (24/36) | WRITER V2 OPTIONAL | Narrative awaits Writer v2 evidence | Yes/Retain or Master |
| Profile & Positioning | website | url | 100% (36/36) | CORE REQUIRED | — | Yes/Retain or Master |
| Profile & Positioning | headquarters | multilineText | 66.7% (24/36) | CORE REQUIRED | MUST gap — research/derive required | Yes/Retain or Master |
| Profile & Positioning | companySize | singleSelect | 66.7% (24/36) | CORE REQUIRED | MUST gap — research/derive required | Yes/Retain or Master |
| Profile & Positioning | companyTagline | singleLineText | 63.9% (23/36) | MOVE TO CLAIMS | Claims-bound; not Setup truth | Yes/Retain or Master |
| Profile & Positioning | companyHistory | multilineText | 63.9% (23/36) | MOVE TO CLAIMS | Claims-bound; not Setup truth | Yes/Retain or Master |
| Profile & Positioning | differentiators | multilineText | 66.7% (24/36) | WRITER V2 OPTIONAL | Narrative awaits Writer v2 evidence | Yes/Retain or Master |
| Profile & Positioning | managementPhilosophy | multilineText | 66.7% (24/36) | MOVE TO CLAIMS | Claims-bound; not Setup truth | Yes/Retain or Master |
| Profile & Positioning | missionStatement | multilineText | 66.7% (24/36) | MOVE TO CLAIMS | Claims-bound; not Setup truth | Yes/Retain or Master |
| Profile & Positioning | yearEstablished | number | 66.7% (24/36) | DERIVED — DATA NOT YET SUFFICIENT | Valid concept; OE coverage insufficient | Yes/Retain or Master |
| Profile & Positioning | yearsInBusiness | number | 66.7% (24/36) | DERIVED — DATA NOT YET SUFFICIENT | Valid concept; OE coverage insufficient | Yes/Retain or Master |
| Profile & Positioning | primaryServiceModel | singleSelect | 66.7% (24/36) | CORE OPTIONAL | Optional / unknown | Yes/Retain or Master |
| Profile & Positioning | brands | multipleRecordLinks | 58.3% (21/36) | DERIVED — DATA NOT YET SUFFICIENT | Valid concept; OE coverage insufficient | Yes/Retain or Master |
| Profile & Positioning | additionalBrands | multilineText | 0% (0/36) | MOVE TO CLAIMS | Claims-bound; not Setup truth | Yes/Retain or Master |
| Profile & Positioning | chainScalesSupported | multipleSelects | 88.9% (32/36) | CORE OPTIONAL | Optional / unknown | Yes/Retain or Master |
| Profile & Positioning | companyLogo | multipleAttachments | 66.7% (24/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | propertyTypes | multipleSelects | 69.4% (25/36) | CORE OPTIONAL | Optional / unknown | Yes/Retain or Master |
| Profile & Positioning | additionalExperience | multipleSelects | 69.4% (25/36) | CORE OPTIONAL | Optional / unknown | Yes/Retain or Master |
| Profile & Positioning | emergencyResponse | singleSelect | 50% (18/36) | DERIVED — DATA NOT YET SUFFICIENT | Valid concept; OE coverage insufficient | Yes/Retain or Master |
| Profile & Positioning | insuranceCoverage | multilineText | 5.6% (2/36) | MOVE TO CLAIMS | Claims-bound; not Setup truth | Yes/Retain or Master |
| Profile & Positioning | sustainabilityPrograms | singleSelect | 47.2% (17/36) | DERIVED — DATA NOT YET SUFFICIENT | Valid concept; OE coverage insufficient | Yes/Retain or Master |
| Profile & Positioning | esgReporting | singleSelect | 47.2% (17/36) | DERIVED — DATA NOT YET SUFFICIENT | Valid concept; OE coverage insufficient | Yes/Retain or Master |
| Profile & Positioning | carbonTracking | singleSelect | 8.3% (3/36) | DERIVED — DATA NOT YET SUFFICIENT | Valid concept; OE coverage insufficient | Yes/Retain or Master |
| Profile & Positioning | energyEfficiency | multilineText | 5.6% (2/36) | MOVE TO CLAIMS | Claims-bound; not Setup truth | Yes/Retain or Master |
| Profile & Positioning | wasteReduction | multilineText | 5.6% (2/36) | MOVE TO CLAIMS | Claims-bound; not Setup truth | Yes/Retain or Master |
| Profile & Positioning | overview_bestat_1_headline | singleLineText | 63.9% (23/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | overview_bestat_1_story | multilineText | 63.9% (23/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | overview_bestat_2_headline | singleLineText | 63.9% (23/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | overview_bestat_2_story | multilineText | 63.9% (23/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | overview_bestat_3_headline | singleLineText | 55.6% (20/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | overview_bestat_3_story | multilineText | 55.6% (20/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | overview_why_1_headline | singleLineText | 63.9% (23/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | overview_why_1_story | multilineText | 63.9% (23/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | overview_why_2_headline | singleLineText | 63.9% (23/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | overview_why_2_story | multilineText | 63.9% (23/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | overview_why_3_headline | singleLineText | 63.9% (23/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | overview_why_3_story | multilineText | 63.9% (23/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | overview_signal_1_value | singleLineText | 50% (18/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | overview_signal_2_value | singleLineText | 50% (18/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | overview_signal_3_value | singleLineText | 50% (18/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | brand_narrative_compliance | multilineText | 63.9% (23/36) | MOVE TO CLAIMS | Claims-bound; not Setup truth | Yes/Retain or Master |
| Profile & Positioning | brand_narrative_relationship | multilineText | 63.9% (23/36) | WRITER V2 OPTIONAL | Narrative awaits Writer v2 evidence | Yes/Retain or Master |
| Profile & Positioning | brand_signal_audit | singleSelect | 63.9% (23/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Profile & Positioning | brand_signal_reflag | singleSelect | 63.9% (23/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Profile & Positioning | brand_signal_franchise_align | singleSelect | 63.9% (23/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Profile & Positioning | brand_signal_soft_retention | singleSelect | 63.9% (23/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Profile & Positioning | figuresAsOf | singleLineText | 63.9% (23/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | businessContinuity | singleSelect | 50% (18/36) | DERIVED — DATA NOT YET SUFFICIENT | Valid concept; OE coverage insufficient | Yes/Retain or Master |
| Profile & Positioning | support24x7 | singleSelect | 50% (18/36) | DERIVED — DATA NOT YET SUFFICIENT | Valid concept; OE coverage insufficient | Yes/Retain or Master |
| Profile & Positioning | crisisExperience | multilineText | 8.3% (3/36) | MOVE TO CLAIMS | Claims-bound; not Setup truth | Yes/Retain or Master |
| Profile & Positioning | capitalStatus | multipleSelects | 5.6% (2/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | numberOfBrands | number | 66.7% (24/36) | DERIVED — DATA NOT YET SUFFICIENT | Valid concept; OE coverage insufficient | Yes/Retain or Master |
| Profile & Positioning | locationTypeResort | number | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Profile & Positioning | locationTypeAirport | number | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Profile & Positioning | marketExpansionRampTimeMonths | number | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Profile & Positioning | readyForInvestorPublication | checkbox | 5.6% (2/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | Service Models Supported | multipleSelects | 77.8% (28/36) | CORE OPTIONAL | Optional / unknown | Yes/Retain or Master |
| Profile & Positioning | Brand Families Operated | multipleSelects | 100% (36/36) | CORE REQUIRED | — | Yes/Retain or Master |
| Profile & Positioning | Soft Brand / Lifestyle Experience | singleSelect | 0% (0/36) | CORE OPTIONAL | Optional / unknown | Yes/Retain or Master |
| Profile & Positioning | brand_portfolio_mix_json | multilineText | 66.7% (24/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | brand_relationship_depth_json | multilineText | 66.7% (24/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | brand_execution_capabilities_json | multilineText | 66.7% (24/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | brand_governance_compliance_json | multilineText | 66.7% (24/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Profile & Positioning | brand_soft_independent_narrative | multilineText | 66.7% (24/36) | MOVE TO CLAIMS | Claims-bound; not Setup truth | Yes/Retain or Master |
| Profile & Positioning | brand_conversion_project_count | singleLineText | 66.7% (24/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Profile & Positioning | brandedVsIndependentMix | singleLineText | 66.7% (24/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | company_name | singleLineText | 100% (36/36) | CORE REQUIRED | — | Yes/Retain or Master |
| Platform & Markets | cap_profile_operational | multilineText | 8.3% (3/36) | WRITER V2 OPTIONAL | Narrative awaits Writer v2 evidence | Yes/Retain or Master |
| Platform & Markets | cap_kpi_operating_model | singleSelect | 11.1% (4/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | cap_kpi_execution_strength | singleSelect | 11.1% (4/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | cap_kpi_transition | singleSelect | 11.1% (4/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | cap_kpi_reporting | singleSelect | 11.1% (4/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | cap_profile_commercial | multilineText | 8.3% (3/36) | WRITER V2 OPTIONAL | Narrative awaits Writer v2 evidence | Yes/Retain or Master |
| Platform & Markets | cap_profile_transition | multilineText | 8.3% (3/36) | MOVE TO CLAIMS | Claims-bound; not Setup truth | Yes/Retain or Master |
| Platform & Markets | cap_card_asset_positioning | multilineText | 8.3% (3/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Platform & Markets | cap_card_service_diff | multilineText | 8.3% (3/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Platform & Markets | cap_card_execution_rel | multilineText | 8.3% (3/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Platform & Markets | cap_card_governance | multilineText | 8.3% (3/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Platform & Markets | cap_deep_revenue_systems | multilineText | 8.3% (3/36) | MOVE TO CLAIMS | Claims-bound; not Setup truth | Yes/Retain or Master |
| Platform & Markets | cap_deep_execution_infra | multilineText | 8.3% (3/36) | MOVE TO CLAIMS | Claims-bound; not Setup truth | Yes/Retain or Master |
| Platform & Markets | cap_signal_budget | singleSelect | 11.1% (4/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | cap_signal_lift | singleSelect | 11.1% (4/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | cap_signal_trans | singleSelect | 11.1% (4/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_na_existing_hotels | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_na_existing_rooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_na_pipeline_hotels | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_na_pipeline_rooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_na_total_hotels | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_na_total_rooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_cala_existing_hotels | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_cala_existing_rooms | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_cala_pipeline_hotels | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_cala_pipeline_rooms | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_cala_total_hotels | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_cala_total_rooms | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_eu_existing_hotels | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_eu_existing_rooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_eu_pipeline_hotels | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_eu_pipeline_rooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_eu_total_hotels | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_eu_total_rooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_mea_existing_hotels | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_mea_existing_rooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_mea_pipeline_hotels | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_mea_pipeline_rooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_mea_total_hotels | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_mea_total_rooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_apac_existing_hotels | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_apac_existing_rooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_apac_pipeline_hotels | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_apac_pipeline_rooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_apac_total_hotels | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_apac_total_rooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_total_existing_hotels | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_total_existing_rooms | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_total_pipeline_hotels | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_total_pipeline_rooms | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_total_total_hotels | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | geo_total_total_rooms | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | numberOfMarkets | number | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | specificMarkets | multilineText | 66.7% (24/36) | CORE OPTIONAL | Optional / unknown | Yes/Retain or Master |
| Platform & Markets | luxuryExistingProperties | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | luxuryExistingRooms | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | luxuryPipelineProperties | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | luxuryPipelineRooms | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | luxuryProperties | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | luxuryRooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | luxuryAvgStaff | number | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upperUpscaleExistingProperties | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upperUpscaleExistingRooms | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upperUpscalePipelineProperties | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upperUpscalePipelineRooms | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upperUpscaleProperties | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upperUpscaleRooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upperUpscaleAvgStaff | number | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upscaleExistingProperties | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upscaleExistingRooms | singleLineText | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upscalePipelineProperties | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upscalePipelineRooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upscaleProperties | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upscaleRooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upscaleAvgStaff | number | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upperMidscaleExistingProperties | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upperMidscaleExistingRooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upperMidscalePipelineProperties | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upperMidscalePipelineRooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upperMidscaleProperties | singleLineText | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upperMidscaleRooms | singleLineText | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | upperMidscaleAvgStaff | number | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | midscaleExistingProperties | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | midscaleExistingRooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | midscalePipelineProperties | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | midscalePipelineRooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | midscaleProperties | singleLineText | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | midscaleRooms | singleLineText | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | midscaleAvgStaff | number | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | economyExistingProperties | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | economyExistingRooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | economyPipelineProperties | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | economyPipelineRooms | singleLineText | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | economyProperties | singleLineText | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | economyRooms | singleLineText | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | economyAvgStaff | number | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | totalProperties | singleLineText | 19.4% (7/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | totalRooms | singleLineText | 16.7% (6/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | chainScale | singleLineText | 8.3% (3/36) | DERIVED — DATA NOT YET SUFFICIENT | Valid concept; OE coverage insufficient | Yes/Retain or Master |
| Platform & Markets | newBuildExperience | number | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | conversionExperience | number | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | turnaroundExperience | number | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | preOpeningExperience | number | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | preOpeningRampLeadTimeMonths | number | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | transitionExperience | number | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | stabilizedExperience | number | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | renovationExperience | number | 2.8% (1/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | locationTypeUrban | number | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | locationTypeSuburban | number | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | locationTypeSmallMetro | number | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | locationTypeInterstate | number | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | locationTypeTotal | number | 8.3% (3/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | exitsDeflaggings | number | 5.6% (2/36) | DEPRECATE | Legacy field — not product-required | No — hide/deprecate |
| Platform & Markets | marketDepthOptIn | checkbox | 2.8% (1/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Platform & Markets | mkt_narrative_depth | multilineText | 8.3% (3/36) | MOVE TO CLAIMS | Claims-bound; not Setup truth | Yes/Retain or Master |
| Platform & Markets | mkt_signal_years | singleLineText | 8.3% (3/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Platform & Markets | mkt_signal_gateway | singleLineText | 8.3% (3/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Platform & Markets | mkt_signal_mix | singleLineText | 8.3% (3/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Platform & Markets | Brands Portfolio Detail | multilineText | 11.1% (4/36) | MOVE TO CLAIMS | Claims-bound; not Setup truth | Yes/Retain or Master |
| Platform & Markets | Active Countries | multipleSelects | 97.2% (35/36) | CORE REQUIRED | MUST gap — research/derive required | Yes/Retain or Master |
| Platform & Markets | Active Markets / Cities | multipleSelects | 5.6% (2/36) | CORE OPTIONAL | Optional / unknown | Yes/Retain or Master |
| Platform & Markets | Market Presence Type | multipleSelects | 97.2% (35/36) | CORE REQUIRED | MUST gap — research/derive required | Yes/Retain or Master |
| Platform & Markets | op_commercial_engine_json | multilineText | 11.1% (4/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Platform & Markets | op_owner_reporting_json | multilineText | 11.1% (4/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Platform & Markets | op_preopening_transition_json | multilineText | 11.1% (4/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Platform & Markets | op_conversion_repositioning_json | multilineText | 11.1% (4/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Platform & Markets | op_fb_lifestyle_resort_json | multilineText | 11.1% (4/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Platform & Markets | mkt_regional_expertise_json | multilineText | 11.1% (4/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
| Platform & Markets | mkt_market_fit_signals_json | multilineText | 11.1% (4/36) | PRESENTATION ONLY | Presentation/scaffold — not Setup truth | No — hide/deprecate |
