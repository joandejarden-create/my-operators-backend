# v45 Design Hotels — Founder Review Reentry

Generated: 2026-07-21T18:45:24.064Z

## Gate

- Projected founder_review_ready: **true**
- Must NOT be active_profile_ready: **true**
- External remains Profile in Preparation: **true**
- Dry-run clean: **true**

## What founder reviews next

1. Internal preview owner copy (affiliation / curation tone).
2. CALA property examples: Wake BioHotel, Condesa DF, Carlota.
3. Standards / loyalty / economics affiliation fit (no franchise boilerplate).
4. Confirm no unlock / no Company Validated claim.

## Blocked until founder OK + v43

- `apply_active_release`
- `Active Profile Approved`
- External full profile

## Next commands

```
npm run brand-explorer-v45-design-hotels-os-remediation -- --brand design-hotels --dry-run
```

After clean dry-run + explicit founder OK for apply:

```
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

Then founder visual review (not auto):

```
npm run brand-explorer-v42-founder-visual-review -- --brands design-hotels --dry-run
```
