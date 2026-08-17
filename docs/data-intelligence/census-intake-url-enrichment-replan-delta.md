# Census intake URL enrichment — re-plan / controlled delta

**Batch:** `osm-dominican-republic-hotel-focused-2026-08-07-url-enriched`  
**Mode:** report-only (no Airtable writes)  
**Merge policy:** high confidence + Google website on brand domain only (15 URLs)

## Plan delta vs baseline

| Metric | Baseline | URL-enriched | Δ |
| --- | ---: | ---: | ---: |
| auto_insert | 62 | 76 | +14 |
| — no Human Review | 19 | 21 | +2 |
| — with Human Review | 43 | 55 | +12 |
| steward_hold | 104 | 90 | −14 |
| reject | 7 | 7 | 0 |
| missing_official_property_url (top reason) | 80 | 65 | −15 |

One of 15 merged URLs remains `steward_hold`: **Donoma Las Terrenas (Autograph)** — `known_brand_missing_city` (URL present; city still Unknown).

## Controlled dry-run

| Cohort | Proposals | Validation pass | Approval bundle |
| --- | ---: | ---: | --- |
| `all` | 76 | 76/76 | ready |
| `no_hr` | 21 | 21/21 | ready |

## Artifacts

- Dual-lane merge: `reports/dual-lane-census-intake-plan-…-url-enriched.json`
- Plan: `reports/census-intake-autopilot-plan-…-url-enriched.json`
- Controlled: `reports/census-intake-autopilot-controlled-…-url-enriched-all.json`
- Approval bundle (prefer first apply): `reports/census-intake-autopilot-approval-bundle-…-url-enriched-no_hr.json`

## Next (apply still blocked)

1. Spot-check the 15 brand-domain Official URLs in the merge summary
2. Prefer first apply cohort `no_hr` (21) after founder confirms
3. Apply requires dedicated path + confirms (`--confirm-intake-inserts`, `--confirm-no-legacy-hotel-census`, …) — not enabled yet
