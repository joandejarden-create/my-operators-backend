# v44 — Brand Explorer OS Release Baseline + Next Batch Router

Freezes the Brand Explorer external release cohort. Incomplete-control is currently **empty** after the v43 fourth batch.

## Released golden brands (baseline)

| Slug | Freeze |
|------|--------|
| `everhome-suites` | `active_profile_ready`, full profile, gallery ≥6, property ≥4, tabs ≥10 |
| `kimpton` | `active_profile_ready`, full profile, gallery ≥6, property ≥5, tabs ≥10 |
| `radisson-individuals-by-choice` | `active_profile_ready`, full profile, gallery ≥6, property ≥3, tabs ≥10 |
| `design-hotels` | `active_profile_ready`, full profile, gallery ≥6, property ≥3, tabs ≥10 (graduated via v43 after v45/v42A-R1) |
| `hotel-indigo` | `active_profile_ready`, full profile, gallery ≥6, property ≥3, tabs ≥10 (graduated via v43 after v42A-R2) |
| `mgallery-collection` | `active_profile_ready`, full profile, gallery ≥6, property ≥3, tabs ≥10 (graduated via v43 after v42A-R2) |
| `small-luxury-hotels-of-the-world` | `active_profile_ready`, full profile, gallery ≥6, property ≥3, tabs ≥10 (graduated via v43 after v42A-R2) |

All released: **Company Validated = false (untouched)**, release fields set, golden suite + external quality lock must pass.

## Incomplete routed brands (locked)

None — `INCOMPLETE_CONTROL_SLUGS` is empty after lifestyle graduation.

## Commands

```bash
npm run brand-explorer-v44-release-baseline -- --brands everhome-suites,kimpton,radisson-individuals-by-choice,design-hotels,hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --dry-run

npm run test:brand-explorer-v44-release-baseline
```

## Outputs

- `reports/brand-explorer-v44-release-baseline.json`
- `reports/brand-explorer-v44-release-baseline.md`
- `reports/brand-explorer-v44-next-batch-router.md`

## Regression protection

Fails if:

- any released brand becomes locked / loses full profile
- gallery or property imageUrl counts drop below freeze
- forbidden owner-copy returns on released brands
- external tabs drop below freeze
- Company Validated flips true on this cohort

## Guardrails

- No Airtable writes (v44 itself)
- No Company Validated changes
- No content changes to released brands via v44

## Modules

- `lib/partner-intelligence/brand-explorer-v44-release-baseline.js`
- `scripts/brand-explorer-v44-release-baseline.mjs`
- `scripts/test-brand-explorer-v44-release-baseline.mjs`
