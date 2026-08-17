# Brand Explorer — Public Restore Governance

- Version: `public-restore-governance-v1`
- Generated: 2026-07-23T23:50:45.271Z
- Mode: **APPLY**

## Summary

- Brands: 1
- Fully ready: 1
- Eligible restore: 0
- Held accidental unlock: 0
- Intentional registry: autograph-collection, bw-premier-collection, bw-signature-collection, country-inn-suites, handwritten-collection, preferred-hotels-and-resorts, quality-inn, radisson, radisson-blu, radisson-collection, radisson-red, suburban-studios, tapestry-collection-by-hilton, vignette-collection, woodspring-suites

## Accidental legacy unlock hold

- `country-inn-suites`
- `suburban-studios`
- `woodspring-suites`
- `autograph-collection`
- `handwritten-collection`
- `radisson-collection`
- `tapestry-collection-by-hilton`
- `vignette-collection`

These stay founder-preview-only until listed in `data/brand-explorer-public-restore-intentional.json` via founder-approved `--apply`.

## Brand results

### Tapestry Collection by Hilton

- Slug: `tapestry-collection-by-hilton`
- Fully ready: true
- Action: `already_intentional_public_restore`
- Reason: listed_in_intentional_restore_registry
- Accidental hold: false
- Restore eligible: false

## Apply command (founder approval required)

```bash
npm run brand-explorer-public-restore-governance -- --brands tapestry-collection-by-hilton --apply \
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

