# Brand Explorer Wave 12 — Preflight

Generated: 2026-07-24T13:56:29.529Z
Pass: **true**

| Metric | Value |
| --- | --- |
| Protected baseline | 27-active-public-full-baseline-v2 |
| Live Active/Live count | 27 (expected 27) |
| Wave12 in Active universe | none |
| Airtable writes | false |

## Gates

- PASS `npm run test:brand-explorer-27-active-public-full-baseline`
  - [PASS] woodspring-suites status=Active pvql=true quality=approve_for_baseline_freeze evidence=null / [PASS] baseline regression aggregate / All 27 Active/Live public-full baseline checks passed. writePerformed=false.
- PASS `npm run test:brand-explorer-recent-momentum-evidence-quality`
  - [Brand Library] Brand Setup - Brand Standards found via Basics link "Brand Setup - Brand Standards" / [PASS] tapestry-collection-by-hilton recent-momentum-evidence-quality / All 3 brand(s) passed recent-momentum-evidence-quality.
- PASS `npm run test:brand-explorer-mandatory-release-gates`
  - > top100-projects-scraper@1.0.0 test:brand-explorer-mandatory-release-gates / > node scripts/test-brand-explorer-mandatory-release-gates.mjs / [PASS] brand-explorer mandatory release gates (source + auditPass + OS state)
- PASS `npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only`
  -   [PASS] woodspring-suites cohort=restored_legacy_public full=true failures=— / Public-full-only: count=27 allPass=true / [Brand Library] Basics link "Brand Setup - Deal Terms" → Brand Setup - Deal Terms failed: An unexpected error occurred
- PASS `npm run brand-explorer-os -- --stage release-readiness --dry-run --skip-regression`
  -   mgallery-collection: active_profile_ready → no_action | founder=false active=false /   small-luxury-hotels-of-the-world: active_profile_ready → no_action | founder=false active=false / v41 OS complete — read-only; no Airtable writes; no unlock; no active release.
- PASS `npm run brand-explorer-24-tab-section-quality-audit -- --dry-run`
  -   tribute-portfolio: approve_for_baseline_freeze composite=97 blockers=0 images=9 /   vignette-collection: approve_for_baseline_freeze composite=97 blockers=0 images=4 /   woodspring-suites: approve_for_baseline_freeze composite=95 blockers=0 images=11

Preflight clean — proceed to manifest.
