# v43 — Brand Explorer Active Release Apply

Final gated unlock for Brand Explorer external owner profiles. **Default is dry-run.** Apply only after explicit founder OK.

## First release

```bash
npm run brand-explorer-v43-active-release-apply -- --brands radisson-individuals-by-choice --dry-run
```

## Second batch (after Radisson proof)

```bash
npm run brand-explorer-v43-active-release-apply -- --brands everhome-suites,kimpton --dry-run
```

## Third batch (Design Hotels — after v45 + v42A-R1)

```bash
npm run brand-explorer-v43-active-release-apply -- --brands design-hotels --dry-run
```

## Fourth batch (Indigo / MGallery / SLH — after v42A-R2)

```bash
npm run brand-explorer-v43-active-release-apply -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --dry-run
```

These three graduated from incomplete-control into the primary release cohort after founder visual review projected `approve_for_active_release`.

## What it discovers

Live Airtable meta on `Brand Setup - Brand Basics` for:

| Field | Role |
|-------|------|
| `Founder Visual Review Pass` | Founder visual review gate |
| `Active Profile Approved` | Active profile unlock switch |
| `Ready for Active Profile` | Legacy alias (also set if present) |
| `Active Profile Approved Date` | Release timestamp |
| `Company Validated` / `Company Validation Date` | **Detected and never written** |

If Active Profile / Founder fields are **missing**, dry-run reports them and apply requires `--confirm-create-missing-release-fields` to create checkboxes/date fields, then set values.

## Apply (founder OK)

```bash
npm run brand-explorer-v43-active-release-apply -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --apply \
  --approve-brand-explorer-v43-active-release \
  --confirm-founder-visual-review-passed \
  --confirm-external-quality-lock-passed \
  --confirm-internal-preview-owner-copy-clean \
  --confirm-six-gallery-imageurls \
  --confirm-three-property-example-imageurls \
  --confirm-no-company-validation-claim \
  --confirm-no-content-writes \
  --confirm-no-source-library-changes \
  --confirm-no-registry-changes \
  --confirm-brand-only
```

(`--confirm-create-missing-release-fields` only when schema discovery shows missing release fields.)

## Allowed writes

- Brand Basics: Founder Visual Review Pass, Active Profile Approved, Ready for Active Profile, Active Profile Approved Date

## Forbidden

- Company Validated / Company Validation Date
- Presentation copy / images
- Source Library / Registry
- Any brand still listed in `INCOMPLETE_CONTROL_SLUGS` (currently empty)

## Expected after fourth-batch apply

- `shouldRenderFullProfile = true`
- `brandExplorerDisplayState` = `active_profile_ready` (or `external_owner_ready` if Company Validated already true — v43 does not set it)
- Primary cohort: Everhome, Kimpton, Radisson Individuals, Design Hotels, Hotel Indigo, MGallery, SLH
- Pre-apply may require Case Summary modal fill on `footprint.openings` (`brand-explorer-v43-lifestyle-opening-modal-fill`) and stub Value Proposition scrub if incomplete keyword chips remain

## Modules

- `lib/partner-intelligence/brand-explorer-active-release-apply.js`
- `scripts/brand-explorer-v43-active-release-apply.mjs`
- `scripts/brand-explorer-v43-lifestyle-opening-modal-fill.mjs`
- `scripts/brand-explorer-v43-lifestyle-value-prop-scrub.mjs`
