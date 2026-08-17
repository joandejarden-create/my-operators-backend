# Brand Explorer v37B — Lifestyle Batch Source Seeding

Generated: 2026-07-21T09:07:05.565Z
Mode: **apply**

## Summary
- Brands: 2
- Source creates proposed: 7
- Source updates proposed: 0
- Sources blocked: 3
- Config registered: hotel-indigo, mgallery-collection

## Guardrails
- sourceLibraryOnly: true
- noPresentationWrites: true
- noRegistryWrites: true
- noImageFieldWrites: true
- noCompanyValidatedChanges: true
- noActiveProfileApproval: true
- readyForActiveApproval: false

## Batch readiness
### Hotel Indigo
- Config: pass
- Sources create/update: 4/0
- Image review: image_remediation_required
- v36B: pass
- Draft apply: **image_remediation_required**
- Next: # Image remediation — review reports/brand-explorer-v37b-hotel-indigo-image-review.md

### MGallery Collection
- Config: pass
- Sources create/update: 3/0
- Image review: review_complete
- v36B: pass
- Draft apply: **apply_draft_allowed**
- Next: npm run brand-explorer-active-profile-apply-draft -- --brand mgallery-collection --dry-run


## Apply command (Source Library only)
```
npm run brand-explorer-v37b-lifestyle-batch-source-seeding -- --brands hotel-indigo,mgallery-collection --apply --approve-brand-explorer-v37B-lifestyle-batch-source-seeding --confirm-source-library-only --confirm-no-company-validation-claim --confirm-no-presentation-row-changes --confirm-no-registry-changes --confirm-no-image-field-changes --confirm-no-active-profile-approval --confirm-hotel-indigo-mgallery-only
```