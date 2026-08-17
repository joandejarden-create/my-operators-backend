# Brand Explorer Wave 14 — Tab Factory Build

Generated: 2026-07-28T15:28:35.352Z
Dry-run: **false** · Applied: **true**
Scope: **marriott-hotels, sheraton, westin, residence-inn-by-marriott, springhill-suites-by-marriott, towneplace-suites-by-marriott, aloft-hotels, four-points-flex-by-sheraton, studiores**
Presentation writes planned: **28** (remediation re-apply after initial full POSTs)
Brand Basics writes planned: **9**
TGS validated: **true**
Blocked: **none**

## Stage 4 acceptance

Ready: **`wave14_stage4_content_clean_ready_for_image_materialization`**

| Gate | Result |
| --- | --- |
| Nine brands Under Review / factory preview only | PASS |
| No images / Brand Status / release / CV / Source / Registry writes | PASS |
| Protected 46 baseline | PASS |
| PVQL public-full-only | PASS (`allPass=true`) |
| Tab-factory audit (9 brands) | Runs; golden + no-empty PASS; only fails = scenario missing images (Stage 5) |
| Golden content quality (9) | PASS |
| No-empty rendered components (9) | PASS |
| AI-Assisted footnote audit | Ran (global enricher; no manual footnote rows) |

### Accepted Stage 4 steward / Stage 5 gaps

- All nine: `overview.scenario.1–3` missing images → Stage 5 image materialization
- SpringHill / TownePlace: International Reference market-archetype openings until steward-matched property URLs
- Four Points Flex / StudioRes: hold named property gallery until Flex / StudioRes Fort Myers overview URLs steward-matched

## Scope controls

- the-house-of-originals: excluded_from_stage4
- morgans-originals: not_created_not_modified
- radisson-collection: excluded_from_stage4
- protected_46_active_brands: read_only_validation_only

## Brands

| Slug | Name | Rows | Pres writes | Basics writes | Blocked |
| --- | --- | ---: | ---: | ---: | --- |
| `marriott-hotels` | Marriott Hotels | 93 | 3 | 1 | false |
| `sheraton` | Sheraton | 93 | 3 | 1 | false |
| `westin` | Westin | 92 | 2 | 1 | false |
| `residence-inn-by-marriott` | Residence Inn by Marriott | 92 | 3 | 1 | false |
| `springhill-suites-by-marriott` | SpringHill Suites by Marriott | 93 | 5 | 1 | false |
| `towneplace-suites-by-marriott` | TownePlace Suites by Marriott | 93 | 4 | 1 | false |
| `aloft-hotels` | Aloft Hotels | 92 | 3 | 1 | false |
| `four-points-flex-by-sheraton` | Four Points Flex by Sheraton | 92 | 2 | 1 | false |
| `studiores` | StudioRes | 92 | 3 | 1 | false |
