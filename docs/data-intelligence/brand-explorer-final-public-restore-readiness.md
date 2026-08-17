# Brand Explorer — Final Public Restore Readiness

**Version:** `final-public-restore-readiness-v1`  
**Mode:** Read-only audit (no Airtable writes, no public restore)

## Purpose

One final gate suite across all remaining restore candidates **before** any public restore is applied.

### Cohort

**Lane 1 (built-blocked):** Country Inn & Suites, Quality Inn, Radisson, Radisson Blu, Radisson RED, Suburban Studios, WoodSpring Suites

**Lane 2 (full-build drafts):** Autograph Collection, Handwritten Collection, Radisson Collection, Tapestry Collection by Hilton, Vignette Collection

### Protected (refused / untouched)

Ascend, Comfort, Curio, Design Hotels, Everhome Suites, Hotel Indigo, Kimpton, MGallery, Radisson Individuals, SLH, Tribute Portfolio

## Checks per brand

- Tab Factory / rendered completeness / no-empty / source provenance
- Image uniqueness + role-match
- Section pattern parity
- Golden content quality
- Founder recommendation = `approve_for_active_release`
- Visibility held unless intentional/already public
- No accidental public-full unlock
- Company Validated snapshot only (untouched)
- No raw URLs in owner-facing copy (momentum URL slots excluded by contract)
- No forbidden / high-medium mechanical owner-facing language

## npm

```bash
npm run brand-explorer-final-public-restore-readiness -- --brands country-inn-suites,quality-inn,radisson,radisson-blu,radisson-red,suburban-studios,woodspring-suites,autograph-collection,handwritten-collection,radisson-collection,tapestry-collection-by-hilton,vignette-collection --dry-run
```

`--apply` is refused on this script. After acceptance pass + founder approval, use:

```bash
npm run brand-explorer-public-restore-governance -- --brands … --apply \
  --approve-public-restore-governance \
  --confirm-founder-visual-review-passed \
  --confirm-fully-ready \
  --confirm-public-visibility-quality-lock-passed \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-content-rewrites \
  --confirm-no-image-writes
```

## Reports

- `reports/brand-explorer-final-public-restore-readiness.json`
- `reports/brand-explorer-final-public-restore-readiness.md`
- `reports/brand-explorer-final-public-restore-lane1.md`
- `reports/brand-explorer-final-public-restore-lane2.md`

## Acceptance

- All 12 → `approve_for_active_release`
- All 12 gate suite PASS
- All held from public **or** intentional/already public
- No accidental legacy unlock
- Protected public-full baseline untouched

## Change impact

**Low** — report-only / read-only.

## Notes

- Gate suite uses the same render mode as official tests (`internalPreview=false`).
- Owner-facing forbidden-language scan uses **rendered HTML + Basics chips**. Residual Airtable economics rows that still mention FDD/LOI but do not render are reported as `airtableResidualForbiddenCount` for stewardship — they do not fail acceptance when not surfaced.
- This audit does **not** apply public restore. Accidental legacy unlock holds remain until intentional restore governance is approved.

## Modules

- `lib/partner-intelligence/brand-explorer-final-public-restore-readiness.js`
- `scripts/brand-explorer-final-public-restore-readiness.mjs`
