# Brand Explorer Image Role-Match

Captions/roles must match visual or metadata evidence. Distinct gallery images alone are not enough.

## Roles

- Exterior / Arrival
- Guest Room / Suite
- Public Space / Lobby
- F&B / Bar / Restaurant / Local Experience
- Design Detail / Interior Detail
- Property Setting / Destination Context
- Wellness / Pool / Spa
- Meeting / Event Space
- Neighborhood / Local Context

## Evidence order

1. Accor DAM type codes (`ho` exterior, `ro` room, `ba` bar/F&B, `sp` spa/wellness)
2. Filename / asset id cues (SLH LucidCM, Scene7)
3. Source page / alt text when present
4. Assigned caption role

## Commands

```bash
npm run test:brand-explorer-image-role-match

npm run brand-explorer-image-role-match-audit -- --brands mgallery-collection,hotel-indigo,small-luxury-hotels-of-the-world --dry-run

npm run brand-explorer-image-role-match-remediation -- --brands mgallery-collection --dry-run

npm run brand-explorer-image-role-match-remediation -- --brands mgallery-collection --apply \
  --approve-image-role-match-remediation \
  --confirm-no-company-validation-changes \
  --confirm-no-release-field-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-protected-brands-unchanged \
  --confirm-six-distinct-gallery-images \
  --confirm-image-captions-match-visual-content \
  --confirm-no-wrong-role-images
```

## Gate wiring

`image_role_match` is required for:

- tab-factory `auditPass`
- visual asset pack readiness
- OS release-readiness / founder path
- display `visualsReady`

## Forbidden writes

Company Validated, release fields, Source Library status, Registry approval, protected brands.
