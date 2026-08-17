# Brand Explorer Wave 13 — Preflight

Generated: 2026-07-26T11:09:28.118Z
Pass: **true**
Ready: **protected_39_live_clean_wave13_may_resume**

| Metric | Value |
| --- | --- |
| Protected baseline | 39-active-public-full-baseline-v1 |
| Live Active/Live count | 39 (expected 39) |
| Fresh live PVQL required | true |
| Cached PVQL accepted | false |
| Public-full clean | true (39/39) |
| Quality approve_for_baseline_freeze | 39/39 |
| Quality decision | ready_to_freeze_39_active_public_full_baseline |
| Airtable writes | false |
| Wave 13 source packs | not started |

## Gates

- PASS `reuse-fresh-reports:pvql+quality`
  - qualityAgeMin=8 / pvqlAt=2026-07-26T10:32:17.329Z / qualityAt=2026-07-26T10:54:18.774Z
- PASS `npm run test:brand-explorer-39-active-public-full-baseline -- --allow-cached-pvql-if-pass`
  - [PASS] woodspring-suites status=Active pvql=true quality=approve_for_baseline_freeze evidence=null / [PASS] baseline regression aggregate / All 39 Active/Live public-full baseline checks passed. writePerformed=false.
- PASS `npm run test:brand-explorer-recent-momentum-evidence-quality`
  - [Brand Library] Brand Setup - Deal Terms found via Basics link "Brand Setup - Deal Terms" / [PASS] tapestry-collection-by-hilton recent-momentum-evidence-quality / All 3 brand(s) passed recent-momentum-evidence-quality.
- PASS `npm run test:brand-explorer-mandatory-release-gates`
  - > top100-projects-scraper@1.0.0 test:brand-explorer-mandatory-release-gates / > node scripts/test-brand-explorer-mandatory-release-gates.mjs / [PASS] brand-explorer mandatory release gates (source + auditPass + OS state)
- PASS `npm run brand-explorer-os -- --stage release-readiness --dry-run --skip-regression`
  -   mgallery-collection: active_profile_ready → no_action | founder=false active=false /   small-luxury-hotels-of-the-world: active_profile_ready → no_action | founder=false active=false / v41 OS complete — read-only; no Airtable writes; no unlock; no active release.

Preflight clean — Wave 13 may resume (source packs still require an explicit later stage).
