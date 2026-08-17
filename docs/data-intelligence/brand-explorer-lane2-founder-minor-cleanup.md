# Brand Explorer — Lane 2 Founder Minor Cleanup

**Version:** `lane2-founder-minor-cleanup-v1`  
**Cohort:** Autograph Collection, Handwritten Collection, Radisson Collection, Tapestry Collection by Hilton, Vignette Collection  
**Scope:** Targeted Presentation + Brand Basics patches only (no rebuild, no public restore).

## Purpose

Clear residual founder-packet / gate failures after Lane 2 image materialization:

- thin Brand Basics positioning / audience
- `overview.why_value` bullet depth
- thin duplicate proof / bestAt rows
- openings Case Summary Overview gaps
- Recent Momentum diligence filler → structured dated cards with linked URLs

## Guardrails (hard)

| Forbidden | Status |
|-----------|--------|
| Public restore | Never |
| Active Profile Approved / Ready for Active Profile / Founder Visual Review Pass | Never |
| Company Validated / Company Validation Date | Never |
| Source Library status | Never |
| Registry approval/status | Never |
| Image field rewrites (except caption-only if a specific render failure requires it) | Default never |
| Public-full baseline brands | Untouched |
| Lane 1 restore-lane brands | Untouched |
| Accidental legacy unlock hold | Remains (`shouldRenderFullProfile=false`) |

## Autograph image provenance note

`wsrv.nl` is an Airtable fetch proxy only (Marriott CDN blocks Airtable’s bot). Owner-facing copy must not expose `wsrv.nl`. Provenance remains Marriott / Autograph official origin. Image uniqueness and role-match must stay pass.

## npm script

```bash
# Dry-run
npm run brand-explorer-lane2-founder-minor-cleanup -- --brands autograph-collection,handwritten-collection,radisson-collection,tapestry-collection-by-hilton,vignette-collection --dry-run

# Apply (all flags required)
npm run brand-explorer-lane2-founder-minor-cleanup -- --brands autograph-collection,handwritten-collection,radisson-collection,tapestry-collection-by-hilton,vignette-collection --apply \
  --approve-lane2-founder-minor-cleanup \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-release-field-writes \
  --confirm-no-public-restore \
  --confirm-targeted-field-fixes-only \
  --confirm-public-baseline-untouched \
  --confirm-accidental-legacy-unlock-hold-remains
```

## Reports

- `reports/brand-explorer-lane2-founder-minor-cleanup.json`
- `reports/brand-explorer-lane2-founder-minor-cleanup.md`
- Per-brand: `reports/brand-explorer-lane2-founder-minor-cleanup-{autograph|handwritten|radisson-collection|tapestry|vignette}.md`

## Post-apply validation

```bash
npm run brand-explorer-tab-factory-audit -- --brands autograph-collection,handwritten-collection,radisson-collection,tapestry-collection-by-hilton,vignette-collection --dry-run
npm run test:brand-explorer-rendered-field-completeness -- --brands …
npm run test:brand-explorer-no-empty-rendered-components -- --brands …
npm run test:brand-explorer-section-pattern-parity -- --brands …
npm run test:brand-explorer-golden-content-quality -- --brands …
npm run brand-explorer-image-uniqueness-audit -- --brands … --dry-run
npm run brand-explorer-image-role-match-audit -- --brands … --dry-run
npm run brand-explorer-built-blocked-founder-review-prep -- --brands … --dry-run
```

Expected founder recommendation: `approve_for_active_release` (still locked / hold remains; no public restore).

## Data contract snapshot

| Item | Value |
|------|--------|
| Tables | `Brand Setup - Brand Explorer Presentation`, `Brand Setup - Brand Basics` |
| Mapping | Content pack `getFullBuildContent` + live Presentation rows |
| Allowed fields | Presentation Title/Body/Case Summary Overview/Active/External Display Status/Sort Order; Basics Brand Positioning + Guest Psychographics Description |
| Forbidden fields | CV, Source, Registry, release, Image, restore |
| Linked records | Presentation `Brand` link to Basics record (creates only) |

## Change impact

**High** — Airtable Presentation + Brand Basics writes.

**Rollback:** Re-hide created momentum rows; restore prior Active on hidden duplicates; revert Basics positioning/audience from backup; keep accidental unlock hold.

## Modules

- `lib/partner-intelligence/brand-explorer-lane2-founder-minor-cleanup.js`
- `scripts/brand-explorer-lane2-founder-minor-cleanup.mjs`
