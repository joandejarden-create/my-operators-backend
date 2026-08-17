# SLH Apply-Draft Plan (v36D)

- Action: **apply_draft**
- Mode: **dry-run** — apply blocked unless future --apply-draft gate

## Draft plan summary

```json
{
  "presentationPatchCount": 12,
  "expectedPresentationPatches": 12,
  "expectedRegistryCreates": 9,
  "contractValidAssumption": true,
  "liveApiBlockedUntilMaterialization": true,
  "postDraftFounderVisualReviewRequired": true,
  "companyValidatedUntouched": true,
  "activeApprovalNotWritten": true
}
```

## Confirmations

- 12 presentation patches must pass Presentation Plan Row Contract before apply
- 9 registry creates (SLH) must be official property images only
- external owner copy passes affiliation sanitizer — no franchise/parent-flag language
- no Company Validated change
- no active-profile approval
- live API remains gallery-blocked until Image materialization completes
- post-draft founder visual review required

## Allowed command (future)
```
npm run brand-explorer-active-profile-apply-draft -- --brand small-luxury-hotels-of-the-world --apply --approve-brand-explorer-active-profile-draft --approve-brand-explorer-active-profile-copy-governance --founder-approved-active-profile-visual-review --confirm-no-company-validation-claim --confirm-no-summary-url-field --confirm-brand-only --confirm-official-source-images-only --confirm-minimum-six-visible-gallery-images --confirm-property-examples-have-hotel-images --confirm-no-logo-lifestyle-property-images --confirm-standard-detail-governance-reviewed
```

> Do not apply in v36D. Post-draft founder visual review required.