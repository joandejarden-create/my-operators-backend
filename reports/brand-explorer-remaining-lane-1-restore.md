# Remaining Brands — Lane 1 (FullyReady Restore)

- Generated: 2026-07-22T23:51:25.667Z
- Brands: country-inn-suites, quality-inn, radisson, radisson-blu, radisson-red, suburban-studios, woodspring-suites
- Public restore applied: **false**

## Visibility formalization

- Accidental legacy unlock hold: country-inn-suites, suburban-studios, woodspring-suites
- Posture without founder approve: **founder-preview-only** (no accidental public-full)
- Intentional restore registry: `data/brand-explorer-public-restore-intentional.json`

## Steps

- `public_restore_governance_plan`: ok
- `public_restore_governance_apply`: ok (skipped) — dry_run_hold_founder_preview_only

## Brand actions

- **country-inn-suites**: fullyReady=true action=`hold_end_accidental_legacy_unlock` hold=true
- **quality-inn**: fullyReady=true action=`hold_founder_preview_only` hold=false
- **radisson**: fullyReady=true action=`hold_founder_preview_only` hold=false
- **radisson-blu**: fullyReady=true action=`hold_founder_preview_only` hold=false
- **radisson-red**: fullyReady=true action=`hold_founder_preview_only` hold=false
- **suburban-studios**: fullyReady=true action=`hold_end_accidental_legacy_unlock` hold=true
- **woodspring-suites**: fullyReady=true action=`hold_end_accidental_legacy_unlock` hold=true

## Founder restore command

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

