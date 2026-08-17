# Brand Explorer Everhome Final Fact Stewardship + Formatting Cleanup Writer v32I

**Batch:** v32I  
**Brand:** Everhome Suites (`everhome-suites`, `recqkkrsevi4r9ibj`)  
**Default:** dry-run / read-only

## Purpose

After v32H, Everhome is Final QA **ready (96)** but `readyForActiveProfile` remains false due to:

1. Pending Explorer facts (`be.footprint.geoIntro`, `be.positioning.guestPromise`)
2. `overview.why_value` line-broken bullet formatting
3. Low-priority sort-order hygiene (deferred unless blocking)

v32I performs the smallest final cleanup without touching images, openings, momentum, or Company Validated.

## Scoped facts

| Field | Fact ID | Stewardship |
|-------|---------|-------------|
| `be.footprint.geoIntro` | `recDp9fzAP5TJYBXJ` | Reject/archive Internal Only (superseded by `footprint.geo_intro`) |
| `be.positioning.guestPromise` | `recwPnALiOcly82bA` | Approve Public if source-backed positioning label |

## Apply gates

```bash
npm run brand-explorer-everhome-final-fact-formatting-cleanup-writer -- \
  --brand everhome-suites \
  --apply \
  --approve-brand-explorer-v32I-everhome-final-fact-formatting-cleanup \
  --confirm-no-company-validation-claim \
  --confirm-no-image-field-changes \
  --confirm-no-openings-or-momentum-changes \
  --confirm-everhome-only
```

## Change impact

**High** — Partner Fact stewardship + `overview.why_value` Body formatting only. Rollback via report patch lists.

## Post-run validation

```bash
npm run brand-explorer-everhome-final-fact-formatting-cleanup-writer -- --brand everhome-suites --dry-run
npm run brand-explorer-final-qa-auditor -- --brand everhome-suites --dry-run
npm run brand-explorer-complete-build -- --brand everhome-suites --dry-run --target-quality active-profile
npm run brand-explorer-visual-display-defect-audit -- --brand everhome-suites --dry-run
npm run test:partner-intelligence-publish-readiness
npm run test:partner-intelligence-profile-governance-publish
```
