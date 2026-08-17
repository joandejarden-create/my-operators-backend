# Brand Explorer Wave 14 — Preflight

Generated: 2026-07-28T13:11:57.293Z
Pass: **true**
Ready: **protected_46_live_clean_wave14_may_resume**

| Metric | Value |
| --- | --- |
| Protected baseline | 46-active-public-full-baseline-v1 |
| Live Active/Live count | 46 (expected 46) |
| Public-full clean | true (46/46) |
| Quality approve_for_baseline_freeze | 46/46 |
| Quality decision | ready_to_freeze_45_active_public_full_baseline |
| Freeze decision | frozen_46_active_public_full_baseline |
| AI-Assisted footnote complete | true |
| Wave 14 Active/Live drift | none |
| Reused fresh PVQL/quality | true |
| Airtable writes | false |

## Gates

- PASS `reuse-fresh-reports:pvql+quality`
  - pvqlAt=2026-07-28T11:37:20.753Z / qualityAt=2026-07-28T11:57:30.147Z / baseline46At=2026-07-28T12:37:19.486Z
- PASS `npm run test:brand-explorer-46-active-public-full-baseline -- --allow-cached-pvql-if-pass`
  - [PASS] woodspring-suites status=Active pvql=true quality=approve_for_baseline_freeze evidence=null / [PASS] baseline regression aggregate / All 46 Active/Live public-full baseline checks passed. writePerformed=false.
- PASS `npm run brand-explorer-ai-assisted-footnote-standardization -- --audit`
  - ] / Enriched audit: pass=47 fail=0 / Wrote C:\Users\joand\OneDrive\Documents\deal-capture-proxy\reports\brand-explorer-ai-assisted-footnote-audit-enriched.json
- PASS `npm run test:brand-explorer-recent-momentum-evidence-quality`
  - [Brand Library] Brand Setup - Loyalty & Commercial found via Basics link "Brand Setup - Loyalty & Commercial" / [PASS] tapestry-collection-by-hilton recent-momentum-evidence-quality / All 3 brand(s) passed recent-momentum-evidence-quality.
- PASS `npm run test:brand-explorer-mandatory-release-gates`
  - > top100-projects-scraper@1.0.0 test:brand-explorer-mandatory-release-gates / > node scripts/test-brand-explorer-mandatory-release-gates.mjs / [PASS] brand-explorer mandatory release gates (source + auditPass + OS state)
- PASS `npm run brand-explorer-os -- --stage release-readiness --dry-run --skip-regression`
  -   mgallery-collection: active_profile_ready → no_action | founder=false active=false /   small-luxury-hotels-of-the-world: active_profile_ready → no_action | founder=false active=false / v41 OS complete — read-only; no Airtable writes; no unlock; no active release.

## Issues

- none

Preflight clean — Wave 14 may proceed to Stage 1 manifest (dry-run).
