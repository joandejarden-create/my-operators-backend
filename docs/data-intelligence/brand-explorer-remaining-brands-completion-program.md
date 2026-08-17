# Brand Explorer — Remaining Brands Completion Program

Controlled two-lane orchestration for all remaining Brand Explorer profiles. Does **not** blind-apply across all brands. Does **not** modify the public-full clean baseline. Does **not** write Company Validated, Company Validation Date, Source Library status, or Registry approval/status.

## Lanes

### Lane 1 — FullyReady built-blocked restore

Targets: `country-inn-suites`, `quality-inn`, `radisson`, `radisson-blu`, `radisson-red`, `suburban-studios`, `woodspring-suites`

1. Verify-only (no rebuild)
2. Founder visual review packets
3. Visibility formalization — end accidental legacy unlock for Country / Suburban / WoodSpring (founder-preview-only until intentional restore)
4. Public restore **only** after founder approval via `brand-explorer-public-restore-governance`

### Lane 2 — True incomplete full Tab Factory build

Targets: `autograph-collection`, `handwritten-collection`, `radisson-collection`, `tapestry-collection-by-hilton`, `vignette-collection`

Full source pack → brand lens → tab-by-tab Presentation draft → gates → founder review → **no** active release until separate restore/release command.

## Commands

```bash
npm run brand-explorer-remaining-brands-completion-program -- --dry-run

npm run brand-explorer-remaining-brands-completion-program -- --brands autograph,handwritten,radisson-collection,tapestry,vignette --lane full-tab-factory-build --dry-run

npm run brand-explorer-public-restore-governance -- --brands country-inn-suites,quality-inn,radisson,radisson-blu,radisson-red,suburban-studios,woodspring-suites --dry-run
```

Public restore apply (founder approval required):

```bash
npm run brand-explorer-public-restore-governance -- --brands country-inn-suites,quality-inn,radisson,radisson-blu,radisson-red,suburban-studios,woodspring-suites --apply \
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

Lane 2 apply (after clean dry-run; still **not** public-full):

```bash
npm run brand-explorer-remaining-brands-completion-program -- --brands autograph,handwritten,radisson-collection,tapestry,vignette --lane full-tab-factory-build --apply \
  --approve-full-tab-factory-build \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-release-field-writes \
  --confirm-tab-factory-contracts \
  --confirm-source-provenance-by-tab \
  --confirm-image-uniqueness \
  --confirm-image-role-match \
  --confirm-section-pattern-parity
```

## Accidental legacy unlock hold

`country-inn-suites`, `suburban-studios`, and `woodspring-suites` are held out of `legacyVisibilityUnlock` until listed in `data/brand-explorer-public-restore-intentional.json` by founder-approved public-restore governance.

## Reports

- `reports/brand-explorer-remaining-brands-completion-program.json|.md`
- `reports/brand-explorer-remaining-lane-1-restore.md`
- `reports/brand-explorer-remaining-lane-2-full-build.md`
- `reports/brand-explorer-full-build-<brand>.md` (Lane 2 per brand)
- `reports/brand-explorer-public-restore-governance.json|.md`

## Change impact

**High** — visibility gating + Presentation draft writes on Lane 2 apply; Lane 1 restore apply writes Brand Basics release fields only when founder flags are present.

Rollback:

- Empty `data/brand-explorer-public-restore-intentional.json` slugs to re-hold accidental unlock brands
- Revert Presentation rows from Lane 2 apply JSON
