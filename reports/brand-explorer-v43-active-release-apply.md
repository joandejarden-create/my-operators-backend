# v43 Brand Explorer Active Release Apply

Generated: 2026-07-22T06:51:18.265Z
dryRun=false applyExecuted=true applyBlocked=false

## Field discovery (Brand Basics)

- Present release fields: Founder Visual Review Pass, Active Profile Approved, Active Profile Approved Date, Ready for Active Profile
- Missing release fields: (none)
- All expected release fields exist on Brand Basics.

### Related live fields (name contains active/founder/validat/ready/…)

- `Parent Company` (singleLineText)
- `Original Parent Company` (singleLineText)
- `Company Profile` (multipleRecordLinks)
- `Company Profile 2` (multipleRecordLinks)
- `Brand Setup - Brand Explorer Presentation` (multipleRecordLinks)
- `Explorer Hero Verification` (singleLineText)
- `Explorer Hero Data Source` (singleLineText)
- `Brand Explorer Favorites` (multipleRecordLinks)
- `Partner Intelligence - Published Explorer Fields` (multipleRecordLinks)
- `Validation Status` (singleSelect)
- `Company Validated` (checkbox)
- `External Display Status` (singleSelect)
- `Company Validation Date` (date)
- `Active Profile Approved Date` (date)
- `Founder Visual Review Pass` (checkbox)
- `Active Profile Approved` (checkbox)
- `Ready for Active Profile` (checkbox)

## Summary

- Brands: 3
- Pre-apply pass: 3/3
- Projected full profile: 3/3
- Records patched: 3
- Incomplete locked: **yes**

## Exact apply command (founder OK required)

```
npm run brand-explorer-v43-active-release-apply -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --apply --approve-brand-explorer-v43-active-release --confirm-founder-visual-review-passed --confirm-external-quality-lock-passed --confirm-internal-preview-owner-copy-clean --confirm-six-gallery-imageurls --confirm-three-property-example-imageurls --confirm-no-company-validation-claim --confirm-no-content-writes --confirm-no-source-library-changes --confirm-no-registry-changes --confirm-brand-only
```

### Hotel Indigo
- OS: founder_review_ready → founder_visual_review
- live display: draft_applied_with_defects full=false
- projected: active_profile_ready full=true
- pre-apply gate: **pass** 
- writes: Founder Visual Review Pass, Active Profile Approved, Ready for Active Profile, Active Profile Approved Date

### MGallery Collection
- OS: founder_review_ready → founder_visual_review
- live display: draft_applied_with_defects full=false
- projected: active_profile_ready full=true
- pre-apply gate: **pass** 
- writes: Founder Visual Review Pass, Active Profile Approved, Ready for Active Profile, Active Profile Approved Date

### Small Luxury Hotels of the World
- OS: founder_review_ready → founder_visual_review
- live display: draft_applied_with_defects full=false
- projected: active_profile_ready full=true
- pre-apply gate: **pass** 
- writes: Founder Visual Review Pass, Active Profile Approved, Ready for Active Profile, Active Profile Approved Date

## Incomplete control


## Guardrails
- No content writes · no Company Validated · no Source Library · no Registry · no incomplete unlock
