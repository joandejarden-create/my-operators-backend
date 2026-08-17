# Operator Profile — Full Live Schema Audit

Engine: `operator-setup-live-field-completion-v1`. Live Airtable is authority.

Production: **36**. Physical fields: **68**.

| Field | ID | Type | Prod Pop | Blank | Fix Pop | Active? | Disposition | Strategy | Reason |
| ----- | -- | ---- | -------- | ----- | ------- | ------- | ----------- | -------- | ------ |
| company_name | `fldNzw2TOwRlVFMqR` | singleLineText | 36 | 0 | 9 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| Operator | `fldEjrDeUzEpYSKNl` | multipleRecordLinks | 36 | 0 | 9 | Y | COMPLETE ALREADY | NONE | Identity/link/system field |
| companyDescription | `fldvFhTCquGafCtAi` | multilineText | 36 | 0 | 9 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| website | `fldUUNUYleuseFYmH` | url | 36 | 0 | 9 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| headquarters | `fldP7lCHTMToePlP1` | multilineText | 36 | 0 | 9 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| companySize | `fldz5ghaGvL50YPeV` | singleSelect | 36 | 0 | 9 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| companyTagline | `fld8loEGpPId9scBF` | singleLineText | 23 | 13 | 0 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (23/36); product consumer present |
| companyHistory | `fldyZdtwGfeZDwNZ9` | multilineText | 36 | 0 | 0 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| differentiators | `fldiZGr5vWOpK1c3h` | multilineText | 36 | 0 | 9 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| managementPhilosophy | `fldYnmA3FJOgLC0C5` | multilineText | 36 | 0 | 9 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| missionStatement | `fldcxzT6hvVbLsNyD` | multilineText | 36 | 0 | 9 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| yearEstablished | `fldIdNlCYj3SPA94S` | number | 36 | 0 | 9 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| yearsInBusiness | `fldNvJ6kpaA81UuUD` | number | 36 | 0 | 9 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| primaryServiceModel | `fldNCwIW4yTxcIpBb` | singleSelect | 36 | 0 | 9 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| brands | `fldGXNbf6Ngb65EJF` | multipleRecordLinks | 36 | 0 | 9 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| additionalBrands | `fldbnGnnM04zwpHBH` | multilineText | 0 | 36 | 0 | N | REMOVE | REMOVE | Empty; redundant with brands / Brand Families |
| chainScalesSupported | `fldzk1SloA6Rd0N1h` | multipleSelects | 36 | 0 | 9 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| companyLogo | `fldoU2Ygs8wCG4csQ` | multipleAttachments | 24 | 12 | 9 | N | REMOVE | REMOVE | Partial Production attachments — cannot auto-fabricate; resolve logo via website/CDN in UI |
| propertyTypes | `fld88xJ3ZxBcbruIM` | multipleSelects | 36 | 0 | 9 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| additionalExperience | `fldiyhoio4HoXJqZo` | multipleSelects | 36 | 0 | 9 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| emergencyResponse | `fldwlYyzIzX6R8DKu` | singleSelect | 18 | 18 | 9 | Y | PARTIAL — POPULATE | CONTROLLED TAXONOMY | Production values exist (18/36) |
| insuranceCoverage | `fldSxpmYApm4SBDko` | multilineText | 2 | 34 | 9 | N | REMOVE | REMOVE | Thin Production fill (2/36) without defensible product methodology |
| sustainabilityPrograms | `fldwYbqMhJuCbCHZA` | singleSelect | 17 | 19 | 9 | Y | PARTIAL — POPULATE | CONTROLLED TAXONOMY | Production values exist (17/36) |
| esgReporting | `fldLpVNfwWJXzVnYx` | singleSelect | 17 | 19 | 9 | Y | PARTIAL — POPULATE | CONTROLLED TAXONOMY | Production values exist (17/36) |
| carbonTracking | `fldmFCUbAJHnj2suV` | singleSelect | 3 | 33 | 9 | N | REMOVE | REMOVE | Thin Production fill (3/36) without defensible product methodology |
| energyEfficiency | `fldO3GQ5SKKbuIgwc` | multilineText | 2 | 34 | 9 | N | REMOVE | REMOVE | Thin Production fill (2/36) without defensible product methodology |
| wasteReduction | `fldf5paimrRLjbL39` | multilineText | 2 | 34 | 9 | N | REMOVE | REMOVE | Thin Production fill (2/36) without defensible product methodology |
| overview_bestat_1_headline | `fld2dKGSnIIT2U58r` | singleLineText | 23 | 13 | 9 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (23/36); product consumer present |
| overview_bestat_1_story | `fldbcIdr1QvwOQ4Oy` | multilineText | 23 | 13 | 9 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (23/36); product consumer present |
| overview_bestat_2_headline | `fldALGAIDnETPiE5p` | singleLineText | 23 | 13 | 9 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (23/36); product consumer present |
| overview_bestat_2_story | `fldHIITRFbdtdQ35M` | multilineText | 23 | 13 | 9 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (23/36); product consumer present |
| overview_bestat_3_headline | `fldMlPIryCMieRQ8c` | singleLineText | 20 | 16 | 9 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (20/36); product consumer present |
| overview_bestat_3_story | `fldSjGg2XbS9kBltU` | multilineText | 20 | 16 | 9 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (20/36); product consumer present |
| overview_why_1_headline | `fldq7qpEwHS9uSpO9` | singleLineText | 23 | 13 | 9 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (23/36); product consumer present |
| overview_why_1_story | `fldE991QtE9tqHbar` | multilineText | 23 | 13 | 9 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (23/36); product consumer present |
| overview_why_2_headline | `fldjrTwUSoajMOSKn` | singleLineText | 23 | 13 | 9 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (23/36); product consumer present |
| overview_why_2_story | `fld7wVs5xE6qNEyVy` | multilineText | 23 | 13 | 9 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (23/36); product consumer present |
| overview_why_3_headline | `fld9LWiQbtolAawv7` | singleLineText | 23 | 13 | 9 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (23/36); product consumer present |
| overview_why_3_story | `fldMVVnP5KufWR6Yk` | multilineText | 23 | 13 | 9 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (23/36); product consumer present |
| overview_signal_1_value | `fldJmNhIyskQxiiUH` | singleLineText | 18 | 18 | 9 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (18/36); product consumer present |
| overview_signal_2_value | `fldwMylQ9EN9eRzA9` | singleLineText | 18 | 18 | 9 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (18/36); product consumer present |
| overview_signal_3_value | `fldniyjXoRFiDuFBX` | singleLineText | 18 | 18 | 9 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (18/36); product consumer present |
| brand_narrative_compliance | `fldwADmUylekcpvm3` | multilineText | 23 | 13 | 9 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (23/36); product consumer present |
| brand_narrative_relationship | `fldTtLWPBa0Rm1FIe` | multilineText | 23 | 13 | 9 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (23/36); product consumer present |
| brand_signal_audit | `fldgt8ubSe4rW1lQT` | singleSelect | 23 | 13 | 9 | Y | PARTIAL — POPULATE | CONTROLLED TAXONOMY | Production values exist (23/36); product consumer present |
| brand_signal_reflag | `fld8pNWHPRyut8nE9` | singleSelect | 23 | 13 | 9 | Y | PARTIAL — POPULATE | CONTROLLED TAXONOMY | Production values exist (23/36); product consumer present |
| brand_signal_franchise_align | `fldnfefJVHuPVeOiD` | singleSelect | 23 | 13 | 9 | Y | PARTIAL — POPULATE | CONTROLLED TAXONOMY | Production values exist (23/36); product consumer present |
| brand_signal_soft_retention | `fldfEkLFWtl4vQr1s` | singleSelect | 23 | 13 | 9 | Y | PARTIAL — POPULATE | CONTROLLED TAXONOMY | Production values exist (23/36); product consumer present |
| figuresAsOf | `fldwPEYr38tAqqbSc` | singleLineText | 23 | 13 | 9 | Y | PARTIAL — POPULATE | CONTROLLED TAXONOMY | Production values exist (23/36) |
| businessContinuity | `fldcil3uETISXaAid` | singleSelect | 18 | 18 | 9 | Y | PARTIAL — POPULATE | CONTROLLED TAXONOMY | Production values exist (18/36) |
| support24x7 | `fldh4ayPsSyTClEd6` | singleSelect | 18 | 18 | 9 | Y | PARTIAL — POPULATE | CONTROLLED TAXONOMY | Production values exist (18/36) |
| crisisExperience | `fldRXY0vqFrJjjSLw` | multilineText | 3 | 33 | 9 | N | REMOVE | REMOVE | Thin Production fill (3/36) without defensible product methodology |
| capitalStatus | `fldgtvDJjyUA80PuE` | multipleSelects | 2 | 34 | 9 | N | REMOVE | REMOVE | Thin Production fill (2/36) without defensible product methodology |
| numberOfBrands | `fldmgzcPo6noJ5aWg` | number | 24 | 12 | 9 | Y | PARTIAL — POPULATE | DERIVED | Production values exist (24/36) |
| locationTypeResort | `fld9VBjm5rCRK4afn` | number | 2 | 34 | 9 | N | REMOVE | REMOVE | Thin Production fill (2/36) without defensible product methodology |
| locationTypeAirport | `fldqyv6YSOj2NpFVS` | number | 2 | 34 | 9 | N | REMOVE | REMOVE | Thin Production fill (2/36) without defensible product methodology |
| marketExpansionRampTimeMonths | `fldwZMgbuVx5jAcRq` | number | 1 | 35 | 9 | N | REMOVE | REMOVE | Thin Production fill (1/36) without defensible product methodology |
| readyForInvestorPublication | `fldO9InVasdfBVDQt` | checkbox | 2 | 34 | 9 | N | REMOVE | REMOVE | Thin Production fill (2/36) without defensible product methodology |
| Service Models Supported | `fldYTHVkG5Eo0MP3T` | multipleSelects | 36 | 0 | 8 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| Brand Families Operated | `fldSiz2QqXCMpDlWS` | multipleSelects | 36 | 0 | 9 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| Soft Brand / Lifestyle Experience | `fldrDgenikKskZfKk` | singleSelect | 36 | 0 | 0 | Y | COMPLETE ALREADY | NONE | Already 36/36 Production |
| brand_portfolio_mix_json | `fldHi4mARlepoSReT` | multilineText | 24 | 12 | 9 | Y | PARTIAL — POPULATE | DERIVED | Production values exist (24/36); product consumer present |
| brand_relationship_depth_json | `fldD9DI6BmCuFva0c` | multilineText | 24 | 12 | 9 | Y | PARTIAL — POPULATE | DERIVED | Production values exist (24/36); product consumer present |
| brand_execution_capabilities_json | `fldD4bBsvuftnDDlL` | multilineText | 24 | 12 | 9 | Y | PARTIAL — POPULATE | DERIVED | Production values exist (24/36); product consumer present |
| brand_governance_compliance_json | `fldJztmbb0t1hlGWM` | multilineText | 24 | 12 | 9 | Y | PARTIAL — POPULATE | DERIVED | Production values exist (24/36); product consumer present |
| brand_soft_independent_narrative | `fldgaZ5a648a1YqeL` | multilineText | 24 | 12 | 9 | Y | PARTIAL — POPULATE | WRITER V2 | Production values exist (24/36); product consumer present |
| brand_conversion_project_count | `fldAnMlUrmwpeSFJv` | singleLineText | 24 | 12 | 9 | Y | PARTIAL — POPULATE | DERIVED | Production values exist (24/36) |
| brandedVsIndependentMix | `fldjxfUeqVflprVqW` | singleLineText | 24 | 12 | 9 | Y | PARTIAL — POPULATE | DERIVED | Production values exist (24/36) |
