# Full CALA 15K Shell Format + Source/Candidate Brand Backfill v1

**Status:** `production_census_full_cala_15k_shell_format_source_brand_backfill_v1_complete`  
**Secondary:** `production_census_full_cala_15k_shell_format_source_brand_backfill_v1_partial_brand_validation_needed`  
**Objective:** `full-cala-15k-shell-format-source-brand-backfill-v1`

## Results
| Metric | Count |
| --- | ---: |
| Shells reviewed (DR/CR/PA) | 1194 |
| Records updated | 1194 |
| Canonical Property Names fixed | 562 |
| Provenance writes | 1194 |
| Candidate Brand Text | 83 |
| Candidate Brand Family | 0 |
| Candidate Brand Source | 83 |
| Brand Validation Status | 83 |
| Current Brand writes | **0** |
| Brand Family / Family·Source Family writes | **0** |
| Schema fields created | 14 |
| Schema missing | 0 |

## Family / Source investigation
- Requested name `Family / Source` does **not** exist.
- Actual field: **`Family / Source Family`** (singleSelect of brand families: IHG, Hilton, Marriott, Independent, …).
- Semantics: brand/source-family affiliation — **not** discovery provenance.
- Handling this mission: **not written**. Use `Candidate Brand Family` for unvalidated signals.

## What was written
- Smart proper-case into **Canonical Property Name** (Property Name left as raw source).
- Discovery Source / Source Candidate Type / Candidate Source Count / Review Status / Shell Insert* fields.
- Candidate Brand* only when Cvent brand text or HBX chain text available.
- Current Brand / Brand Family intentionally blank pending validation.

## Confirmations
Hotel Property Census only · no Brand Explorer/Setup · no Rooms/Keys · no coords/media · no owner/operator/dates · no public display changes · shells remain Hold / HR Required · Cvent remains candidate identity only.

## Artifacts
- `reports/research-engine-v2/full-cala-15k-shell-format-source-brand-backfill-v1.{md,json}`
- Module: `lib/research-engine-v2/full-cala-15k-shell-format-source-brand-backfill-v1.js`
