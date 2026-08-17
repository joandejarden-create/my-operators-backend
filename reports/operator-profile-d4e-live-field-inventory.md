# D.4E Live Profile Field Inventory

Physical fields: **68**. Production operators: **36**.

| # | Field | Field ID | Type | Fill | Blank | Action | Purpose |
| - | ----- | -------- | ---- | ---- | ----- | ------ | ------- |
| 1 | company_name | `fldNzw2TOwRlVFMqR` | singleLineText | 36/36 | 0 | POPULATE | Active Core Product / identity |
| 2 | Operator | `fldEjrDeUzEpYSKNl` | multipleRecordLinks | 36/36 | 0 | POPULATE | Active Core Product / identity |
| 3 | companyDescription | `fldvFhTCquGafCtAi` | multilineText | 36/36 | 0 | POPULATE | Active Core Product / identity |
| 4 | website | `fldUUNUYleuseFYmH` | url | 36/36 | 0 | POPULATE | Active Core Product / identity |
| 5 | headquarters | `fldP7lCHTMToePlP1` | multilineText | 36/36 | 0 | POPULATE | Active Core Product / identity |
| 6 | companySize | `fldz5ghaGvL50YPeV` | singleSelect | 36/36 | 0 | POPULATE | Active Core Product / identity |
| 7 | companyTagline | `fld8loEGpPId9scBF` | singleLineText | 23/36 | 13 | REMOVE FROM ACTIVE PRODUCT | Marketing slogan — previously deprecated; no decision value |
| 8 | companyHistory | `fldyZdtwGfeZDwNZ9` | multilineText | 36/36 | 0 | POPULATE | Active Core Product / identity |
| 9 | differentiators | `fldiZGr5vWOpK1c3h` | multilineText | 36/36 | 0 | POPULATE | Active Core Product / identity |
| 10 | managementPhilosophy | `fldYnmA3FJOgLC0C5` | multilineText | 24/36 | 12 | KEEP — POPULATE | Explorer Company Story field; distinct from differentiators — how the company describes its approach to operating hotels. |
| 11 | missionStatement | `fldcxzT6hvVbLsNyD` | multilineText | 24/36 | 12 | KEEP — POPULATE | Explorer Company Story field; official mission/purpose/vision hierarchy — not invented. |
| 12 | yearEstablished | `fldIdNlCYj3SPA94S` | number | 24/36 | 12 | POPULATE | Year the current operator/company traces its operating origin to (not brand launch alone, not CALA division start). |
| 13 | yearsInBusiness | `fldNvJ6kpaA81UuUD` | number | 24/36 | 12 | DERIVE | yearsInBusiness = 2026 - yearEstablished (deterministic; never free-typed). |
| 14 | primaryServiceModel | `fldNCwIW4yTxcIpBb` | singleSelect | 24/36 | 12 | KEEP — POPULATE | Explorer/alignment still consume it; distinct from Operating Model (legal/commercial structure) — portfolio service orientation. |
| 15 | brands | `fldGXNbf6Ngb65EJF` | multipleRecordLinks | 21/36 | 15 | DERIVE | Linked Brand Basics records for brands with documented current/operating experience via Brand Relationships ∪ Current Assignments (not brand ownership alone). |
| 16 | additionalBrands | `fldbnGnnM04zwpHBH` | multilineText | 0/36 | 36 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 17 | chainScalesSupported | `fldzk1SloA6Rd0N1h` | multipleSelects | 36/36 | 0 | POPULATE | Active Core Product / identity |
| 18 | companyLogo | `fldoU2Ygs8wCG4csQ` | multipleAttachments | 24/36 | 12 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 19 | propertyTypes | `fld88xJ3ZxBcbruIM` | multipleSelects | 36/36 | 0 | POPULATE | Active Core Product / identity |
| 20 | additionalExperience | `fldiyhoio4HoXJqZo` | multipleSelects | 36/36 | 0 | POPULATE | Active Core Product / identity |
| 21 | emergencyResponse | `fldwlYyzIzX6R8DKu` | singleSelect | 18/36 | 18 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 22 | insuranceCoverage | `fldSxpmYApm4SBDko` | multilineText | 2/36 | 34 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 23 | sustainabilityPrograms | `fldwYbqMhJuCbCHZA` | singleSelect | 17/36 | 19 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 24 | esgReporting | `fldLpVNfwWJXzVnYx` | singleSelect | 17/36 | 19 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 25 | carbonTracking | `fldmFCUbAJHnj2suV` | singleSelect | 3/36 | 33 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 26 | energyEfficiency | `fldO3GQ5SKKbuIgwc` | multilineText | 2/36 | 34 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 27 | wasteReduction | `fldf5paimrRLjbL39` | multilineText | 2/36 | 34 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 28 | overview_bestat_1_headline | `fld2dKGSnIIT2U58r` | singleLineText | 23/36 | 13 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 29 | overview_bestat_1_story | `fldbcIdr1QvwOQ4Oy` | multilineText | 23/36 | 13 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 30 | overview_bestat_2_headline | `fldALGAIDnETPiE5p` | singleLineText | 23/36 | 13 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 31 | overview_bestat_2_story | `fldHIITRFbdtdQ35M` | multilineText | 23/36 | 13 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 32 | overview_bestat_3_headline | `fldMlPIryCMieRQ8c` | singleLineText | 20/36 | 16 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 33 | overview_bestat_3_story | `fldSjGg2XbS9kBltU` | multilineText | 20/36 | 16 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 34 | overview_why_1_headline | `fldq7qpEwHS9uSpO9` | singleLineText | 23/36 | 13 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 35 | overview_why_1_story | `fldE991QtE9tqHbar` | multilineText | 23/36 | 13 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 36 | overview_why_2_headline | `fldjrTwUSoajMOSKn` | singleLineText | 23/36 | 13 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 37 | overview_why_2_story | `fld7wVs5xE6qNEyVy` | multilineText | 23/36 | 13 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 38 | overview_why_3_headline | `fld9LWiQbtolAawv7` | singleLineText | 23/36 | 13 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 39 | overview_why_3_story | `fldMVVnP5KufWR6Yk` | multilineText | 23/36 | 13 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 40 | overview_signal_1_value | `fldJmNhIyskQxiiUH` | singleLineText | 18/36 | 18 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 41 | overview_signal_2_value | `fldwMylQ9EN9eRzA9` | singleLineText | 18/36 | 18 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 42 | overview_signal_3_value | `fldniyjXoRFiDuFBX` | singleLineText | 18/36 | 18 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 43 | brand_narrative_compliance | `fldwADmUylekcpvm3` | multilineText | 23/36 | 13 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 44 | brand_narrative_relationship | `fldTtLWPBa0Rm1FIe` | multilineText | 23/36 | 13 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 45 | brand_signal_audit | `fldgt8ubSe4rW1lQT` | singleSelect | 23/36 | 13 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 46 | brand_signal_reflag | `fld8pNWHPRyut8nE9` | singleSelect | 23/36 | 13 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 47 | brand_signal_franchise_align | `fldnfefJVHuPVeOiD` | singleSelect | 23/36 | 13 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 48 | brand_signal_soft_retention | `fldfEkLFWtl4vQr1s` | singleSelect | 23/36 | 13 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 49 | figuresAsOf | `fldwPEYr38tAqqbSc` | singleLineText | 23/36 | 13 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 50 | businessContinuity | `fldcil3uETISXaAid` | singleSelect | 18/36 | 18 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 51 | support24x7 | `fldh4ayPsSyTClEd6` | singleSelect | 18/36 | 18 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 52 | crisisExperience | `fldRXY0vqFrJjjSLw` | multilineText | 3/36 | 33 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 53 | capitalStatus | `fldgtvDJjyUA80PuE` | multipleSelects | 2/36 | 34 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 54 | numberOfBrands | `fldmgzcPo6noJ5aWg` | number | 24/36 | 12 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 55 | locationTypeResort | `fld9VBjm5rCRK4afn` | number | 2/36 | 34 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 56 | locationTypeAirport | `fldqyv6YSOj2NpFVS` | number | 2/36 | 34 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 57 | marketExpansionRampTimeMonths | `fldwZMgbuVx5jAcRq` | number | 1/36 | 35 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 58 | readyForInvestorPublication | `fldO9InVasdfBVDQt` | checkbox | 2/36 | 34 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 59 | Service Models Supported | `fldYTHVkG5Eo0MP3T` | multipleSelects | 36/36 | 0 | POPULATE | Active Core Product / identity |
| 60 | Brand Families Operated | `fldSiz2QqXCMpDlWS` | multipleSelects | 36/36 | 0 | POPULATE | Active Core Product / identity |
| 61 | Soft Brand / Lifestyle Experience | `fldrDgenikKskZfKk` | singleSelect | 36/36 | 0 | POPULATE | Active Core Product / identity |
| 62 | brand_portfolio_mix_json | `fldHi4mARlepoSReT` | multilineText | 24/36 | 12 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 63 | brand_relationship_depth_json | `fldD9DI6BmCuFva0c` | multilineText | 24/36 | 12 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 64 | brand_execution_capabilities_json | `fldD4bBsvuftnDDlL` | multilineText | 24/36 | 12 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 65 | brand_governance_compliance_json | `fldJztmbb0t1hlGWM` | multilineText | 24/36 | 12 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 66 | brand_soft_independent_narrative | `fldgaZ5a648a1YqeL` | multilineText | 24/36 | 12 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 67 | brand_conversion_project_count | `fldAnMlUrmwpeSFJv` | singleLineText | 24/36 | 12 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |
| 68 | brandedVsIndependentMix | `fldjxfUeqVflprVqW` | singleLineText | 24/36 | 12 | REMOVE FROM ACTIVE PRODUCT | Legacy / presentation / non-core |

## Six-field decisions

### yearEstablished

- **Action:** POPULATE
- **Rule:** Year the current operator/company traces its operating origin to (not brand launch alone, not CALA division start).

### yearsInBusiness

- **Action:** DERIVE
- **Rule:** yearsInBusiness = 2026 - yearEstablished (deterministic; never free-typed).

### brands

- **Action:** DERIVE
- **Rule:** Linked Brand Basics records for brands with documented current/operating experience via Brand Relationships ∪ Current Assignments (not brand ownership alone).

### primaryServiceModel

- **Action:** KEEP — POPULATE
- **Rule:** Explorer/alignment still consume it; distinct from Operating Model (legal/commercial structure) — portfolio service orientation.

### managementPhilosophy

- **Action:** KEEP — POPULATE
- **Rule:** Explorer Company Story field; distinct from differentiators — how the company describes its approach to operating hotels.

### missionStatement

- **Action:** KEEP — POPULATE
- **Rule:** Explorer Company Story field; official mission/purpose/vision hierarchy — not invented.

