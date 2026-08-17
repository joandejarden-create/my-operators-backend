# Brand Explorer — Legacy Approved Profile Reconciliation

## Problem

Previously finished Brand Explorer profiles (Autograph, Ascend, Comfort, Country, Curio, and peers) can show the external locked shell (“This brand profile is being prepared for external review…”) after the new OS / `PRIMARY_RELEASE_SLUGS` cohort was introduced — even when they were already founder-approved under older Complete Build / Ready for Active Profile workflows.

## Non-negotiables

- Do **not** silently hide historically approved brands merely because they are missing from the latest release cohort.
- Do **not** mark defective profiles ready.
- Do **not** write `Company Validated`, Source Library status, or Registry approval.
- New brands still require full Tab Factory + image uniqueness + provenance + founder gates.

## Commands

Dry-run:

```bash
npm run brand-explorer-legacy-approved-profile-reconciliation -- --dry-run
```

Apply (founder confirmation required):

```bash
npm run brand-explorer-legacy-approved-profile-reconciliation -- --apply \
  --approve-legacy-approved-profile-reconciliation \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-content-rewrites \
  --confirm-legacy-status-evidence-reviewed
```

## Seed resolution

Founder-named legacy brands resolve via `LEGACY_SEED_BRANDS` (slug + Airtable `recordId` + aliases), not only `PRIMARY_RELEASE_SLUGS`. Examples: Ascend, Comfort Inn & Suites, Country Inn & Suites, Curio, Autograph.

## Classifications

| Classification | Meaning |
| --- | --- |
| `migrate_to_active_profile_ready` | Historical ready signal + visuals present + image uniqueness pass → may write Active Profile / Founder Visual Review fields |
| `needs_new_gate_validation_before_migration` | Historical signal exists but new gates incomplete |
| `content_remediation_required` | Missing presentation content |
| `image_remediation_required` | Image uniqueness / distinct gallery fail |
| `unresolved_manual_review` | No clear historical approval evidence |

## Display behavior

`resolveBrandExplorerDisplayState` recognizes `legacy_approved_pending_migration` for historically approved brands with presentation rows (even outside the latest cohort). The atelier UI treats that state as full-profile eligible so profiles are not silently locked solely for missing `PRIMARY_RELEASE_SLUGS`. Brands that fail image uniqueness or lack presentation remain locked **with** an explicit reconciliation classification — not a silent hide.

## Reports

- `reports/brand-explorer-legacy-approved-profile-reconciliation.json`
- `reports/brand-explorer-legacy-approved-profile-reconciliation.md`
