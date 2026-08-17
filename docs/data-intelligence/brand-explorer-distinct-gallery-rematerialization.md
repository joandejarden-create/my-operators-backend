# Distinct Gallery Rematerialization (MGallery + SLH)

## Goal

Replace padded / near-duplicate Brand Explorer gallery images so live uniqueness reports:

- `galleryDistinctCount >= 6`
- `scenarioDistinctCount >= 3`
- `propertyExampleDistinctCount >= 3`

Targets only:

- `mgallery-collection`
- `small-luxury-hotels-of-the-world`

Hotel Indigo and all legacy/protected brands are refused.

## Commands

Dry-run:

```bash
npm run brand-explorer-distinct-gallery-rematerialization -- --brands mgallery-collection,small-luxury-hotels-of-the-world --dry-run
```

Apply (founder gates required):

```bash
npm run brand-explorer-distinct-gallery-rematerialization -- --brands mgallery-collection,small-luxury-hotels-of-the-world --apply \
  --approve-distinct-gallery-rematerialization \
  --confirm-no-company-validation-changes \
  --confirm-no-release-field-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-protected-brands-unchanged \
  --confirm-gallery-distinct-six \
  --confirm-scenario-distinct-three \
  --confirm-property-distinct-three \
  --confirm-no-slot-padding
```

## Inventory

- **MGallery:** Accor `ahstatic` property photography (`ho_00` / `ho_01` / `ho_02` per CALA hotel — distinct scenes, not size variants).
- **SLH:** Official `slh.com` media + LucidCM property images across Coral Reef Club, Quinta da Comporta, and Hôtel San Régis. International properties labeled **International Reference**.

If inventory cannot supply 6 distinct gallery images, the tool does **not** pad slots and routes the brand to `image_remediation` with a source acquisition plan.

## Forbidden writes

- Company Validated / Company Validation Date
- Release / Active Profile approval fields
- Source Library status
- Registry approval/status
- Protected brand content

## Reports

- `reports/brand-explorer-distinct-gallery-rematerialization.json`
- `reports/brand-explorer-distinct-gallery-rematerialization.md`
- `reports/brand-explorer-distinct-gallery-rematerialization-mgallery.md`
- `reports/brand-explorer-distinct-gallery-rematerialization-slh.md`
