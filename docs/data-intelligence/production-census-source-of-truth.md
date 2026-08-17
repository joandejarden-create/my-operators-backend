# Production Census Source of Truth (Autopilot)

**Status:** `production_census_source_of_truth_locked_ready_for_autopilot`  
**Module:** `lib/research-engine-v2/production-census-source-of-truth.js`

## Binding production target

| Key | Value |
| --- | --- |
| Base | Deal Capture Platform (`AIRTABLE_BASE_ID_ALT`) |
| Table | **Hotel Property Census** |
| Table ID | `tbl9aY5ijiuIzzWam` |
| Role | `production_property_table` |
| Autopilot write | **allowed** (allowlisted fields only) |

Supporting production tables (read / explicit queue-approved only — **not** default Autopilot write):

- Hotel Property Brand Affiliations
- Hotel Property Source Evidence
- Hotel Property Steward Review

## Terminology (use these exact phrases)

| Say | Meaning |
| --- | --- |
| **Hotel Property Census** | Production property data table (only Autopilot write target) |
| **VIC source claims** | Verified Independent Census evidence / claim library (read-only lineage) |
| **legacy Census** | Old / deprecated Hotel Census (not production; migration/audit only) |
| **Brand Setup active control list** | Active / Live brands (read-only for Autopilot) |

Run summaries must say:

> Matched Active / Live Brand Setup brands to production Hotel Property Census records.

Not: “Matched brands to Census.”

## Read vs write rules

**Allowed reads**

- Brand Setup / Brand Basics — Active / Live control list only
- Hotel Property Census — read + write allowlisted fields
- VIC claim files — evidence / source lineage only
- Supporting evidence tables — read; write only if explicitly approved by queue logic

**Allowed writes**

- Hotel Property Census only

**Forbidden writes**

- legacy Hotel Census / old Census
- Verified Independent Census source files / VIC staging
- Candidates / Evidence staging tables
- Brand Setup / Brand Explorer / Brand Presentation
- Company Validated / Brand Verified / Brand Status
- Owner / operator / date fields unless a separate approved lane

## Apply guard (fail closed)

Every production write validates:

1. Target base = Deal Capture Platform  
2. Target table = Hotel Property Census  
3. Target table ID = `tbl9aY5ijiuIzzWam`  
4. Fields are allowlisted  
5. Brand Setup / Brand Explorer / legacy / VIC / staging writes blocked  

If the target cannot be verified → stop apply with `blocked_wrong_census_target`.

## Related

- `docs/data-intelligence/production-census-autopilot-runner.md`
- `docs/data-intelligence/production-census-autopilot-operating-model.md`
- `reports/research-engine-v2/production-census-source-of-truth-audit.md`
