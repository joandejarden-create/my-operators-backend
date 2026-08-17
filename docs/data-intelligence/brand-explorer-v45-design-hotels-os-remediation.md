# v45 — Design Hotels OS-Guided Remediation + Founder Review Reentry

Moves **Design Hotels** from `draft_applied_with_defects` toward `founder_review_ready` using the Brand Explorer OS `apply_remediation` path.

Does **not** unlock. Does **not** set active-profile approval. Does **not** change Company Validated. Does **not** touch released golden brands or other incompletes.

## Target

- `design-hotels`

## Protected

- `everhome-suites`, `kimpton`, `radisson-individuals-by-choice` (v44 baseline)
- `hotel-indigo`, `mgallery-collection`, `small-luxury-hotels-of-the-world` (not processed)

## Dry-run

```bash
npm run brand-explorer-v45-design-hotels-os-remediation -- --brand design-hotels --dry-run
```

## Apply (only if dry-run clean + founder OK)

```bash
npm run brand-explorer-v45-design-hotels-os-remediation -- --brand design-hotels --apply \
  --approve-brand-explorer-v45-design-hotels-os-remediation \
  --confirm-no-company-validation-claim \
  --confirm-no-active-profile-approval \
  --confirm-no-source-library-changes \
  --confirm-no-registry-changes \
  --confirm-no-image-field-changes \
  --confirm-external-profile-remains-locked \
  --confirm-internal-preview-owner-copy-clean \
  --confirm-released-golden-brands-unchanged \
  --confirm-design-hotels-only
```

## What it does

1. Confirms OS state = `draft_applied_with_defects` / action = `apply_remediation`
2. Ingests live + known defect families
3. Builds Presentation patch plan (residual owner-copy + affiliation/curation scrub)
4. Audits CALA trio: Wake BioHotel, Condesa DF, Carlota
5. Projects internal preview owner-safe after patches
6. Protects v44 release baseline
7. Projects `founder_review_ready` (never `active_profile_ready`)

## Allowed writes (apply only)

- Presentation: Title, Body, Case Summary*, External Display Status (hide only if needed)

## Forbidden

- Company Validated / approval fields
- Source Library / Registry / image fields
- Released golden brand content
- Other incomplete brands

## Affiliation model

Use: independent design-led hotels, member identity, curation, architecture / culture / local identity, affiliation value, Bonvoy caveats only when source-supported.

Avoid: franchise flag, chain prototype, FDD, Item 19, LOI, fee stack, ADR/RevPAR, brand-verified, raw URLs, Sources notes.

## Outputs

- `reports/brand-explorer-v45-design-hotels-os-remediation.{json,md}`
- `reports/brand-explorer-v45-design-hotels-founder-reentry.md`
- `reports/brand-explorer-v45-release-baseline-protection.md`

## Modules

- `lib/partner-intelligence/brand-explorer-v45-design-hotels-os-remediation.js`
- `scripts/brand-explorer-v45-design-hotels-os-remediation.mjs`

OS router next command for Design Hotels points at this dry-run.
