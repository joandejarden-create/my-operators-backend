# Brand Explorer — 24 pre-baseline minor cleanup

**Status:** Complete · **Protected baseline freeze:** **YES — `ready_to_freeze_24_brand_baseline`**

## Purpose

Targeted Presentation-only patches for the 11 brands flagged `approve_after_minor_cleanup` after the 24-brand tab/section quality audit. Not a rebuild, release, Company Validated, Source Library, or Registry task.

## Result

| Metric | Value |
|--------|-------|
| Cleanup brands | 11 |
| Presentation patches | 100 (97 + 3 Preferred residual) |
| Image actions | 46 (titles/captions; images preserved) |
| Post-audit freeze | **24/24** `approve_for_baseline_freeze` |
| Remediation / blockers | **0 / 0** |
| Cross-brand image reuse | **0** |
| PVQL public-full | **24/24 PASS** |
| OS release-readiness | aligned (`no_action`) |
| Mandatory release gates | **PASS** |
| CV / Source / Registry / Brand Status | untouched |

## Scripts

```bash
npm run brand-explorer-24-pre-baseline-minor-cleanup -- --brands … --dry-run
npm run brand-explorer-24-pre-baseline-minor-cleanup -- --brands … --apply \
  --approve-pre-baseline-minor-cleanup \
  --confirm-targeted-findings-only \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-brand-status-changes \
  --confirm-no-release-field-changes \
  --confirm-no-public-restore-changes \
  --confirm-no-broad-rewrites \
  --confirm-active-live-brands-only \
  --confirm-radisson-collection-and-tapestry-excluded
```

Lib: `lib/partner-intelligence/brand-explorer-24-pre-baseline-minor-cleanup.js`  
CLI: `scripts/brand-explorer-24-pre-baseline-minor-cleanup.mjs`

## Reports

- `reports/brand-explorer-24-pre-baseline-minor-cleanup.json`
- `reports/brand-explorer-24-pre-baseline-minor-cleanup.md`
- `reports/brand-explorer-24-pre-baseline-minor-cleanup-image-actions.md`
- Re-audit: `reports/brand-explorer-24-tab-section-quality-audit.*`

## Exclusions

- Radisson Collection (Draft)
- Tapestry Collection by Hilton (Under Review)
- Brands already `approve_for_baseline_freeze` before this pass (no unnecessary writes)

## Note on Preferred residual

Wave 1 replaced Marriott Bonvoy proof.2 copy but shortened Body under the 35-word PVQL/golden gate. Wave 2 restored ≥35-word commercial representation copy and thickened operator tags. Preferred then passed PVQL and freeze.
