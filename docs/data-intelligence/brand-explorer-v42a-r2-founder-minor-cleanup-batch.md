# v42A-R2 — Founder Minor Cleanup Batch

Narrow Owner Considerations / standards checklist cleanup for brands that v42 routed to **`approve_after_minor_cleanup`**:

- `hotel-indigo`
- `mgallery-collection`
- `small-luxury-hotels-of-the-world`

Protects released golden brands. Does not unlock or approve active profile.

## Command

```bash
npm run brand-explorer-v42a-r2-founder-minor-cleanup-batch -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --dry-run
```

### Apply

```bash
npm run brand-explorer-v42a-r2-founder-minor-cleanup-batch -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --apply \
  --approve-brand-explorer-v42A-R2-founder-minor-cleanup-batch \
  --confirm-no-company-validation-claim \
  --confirm-no-active-profile-approval \
  --confirm-no-source-library-changes \
  --confirm-no-registry-changes \
  --confirm-no-image-field-changes \
  --confirm-no-released-brand-changes \
  --confirm-external-profiles-remain-locked \
  --confirm-brand-only
```

## Scope

Fixes the Owner Considerations caution caused by missing `standards.requirement` Presentation rows (atelier placeholder: “No owner planning checklist…”).

Per brand (≤10 patches):

- `standards.intro`
- six `standards.requirement` diligence rows (brand-model-specific)
- `standards.conversion`
- `standards.questions`

## Forbidden

- Active release / unlock / Company Validated
- Source Library / Registry / Image fields
- Released brand content
- FDD / Item 19 / LOI / fee stack / raw URLs / generic filler
- Franchise / parent-brand language for SLH
- InterContinental confusion for Hotel Indigo

## Projection

After cleanup: founder decision **`approve_for_active_release`**, still externally locked until founder OK + v43.

v42 founder packets use brand copy-signal profiles + brand lenses for these three incomplete brands so Owner Considerations clearance can route to approve (not permanently stuck on `no_profile` / `no_lens`).

## Change impact

**Medium** dry-run · **High** on apply (Presentation creates for standards slots).

Rollback: delete/hide created `standards.*` rows for the three target brands only.
