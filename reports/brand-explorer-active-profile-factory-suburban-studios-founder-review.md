# Founder Visual Review v34D

- Brand: **Suburban Studios** (`suburban-studios`)
- Generated: 2026-07-14T11:53:51.282Z
- Overall: **FAIL**

## Live Brand Explorer checks
- **FAIL** — 6 gallery images visible with imageUrl (0/6)
- **FAIL** — 3 property examples visible with hotel images (0 cards)
- **PASS** — No logo/lifestyle/generic property images (gallery + property scan)
- **PASS** — No IMAGE placeholders on scenario cards (ok)
- **FAIL** — No FDD / Item 19 / ADR / RevPAR / net contribution language (55 high findings)
- **PASS** — Standard detail governance visible and safe (unknown)
- **PASS** — Company Validated untouched (before=false after=false)
- **FAIL** — Registry traceability for visual slots (9 gaps)
- **PASS** — No stale UI fallback titles (0 risks)

## Failed checks
- `gallery_six_visible`: 6 gallery images visible with imageUrl
- `property_examples_hotel_images`: 3 property examples visible with hotel images
- `copy_safety`: No FDD / Item 19 / ADR / RevPAR / net contribution language
- `registry_traceability`: Registry traceability for visual slots

## Stage 1 — Draft apply
- Allowed (apply mode): **yes**
- Flags required for apply: approveBrandExplorerActiveProfileDraft, founderVisualReview, confirmNoCompanyValidationClaim, confirmNoSummaryUrlField, confirmBrandOnly, confirmOfficialSourceImagesOnly, confirmMinimumSixVisibleGalleryImages, confirmPropertyExamplesHaveHotelImages, confirmNoLogoLifestylePropertyImages, confirmStandardDetailGovernanceReviewed, approveCopyGovernance
```bash
npm run brand-explorer-active-profile-apply-draft -- --brand suburban-studios --apply --approve-brand-explorer-active-profile-draft --approve-brand-explorer-active-profile-copy-governance --founder-approved-active-profile-visual-review --confirm-no-company-validation-claim --confirm-no-summary-url-field --confirm-brand-only --confirm-official-source-images-only --confirm-minimum-six-visible-gallery-images --confirm-property-examples-have-hotel-images --confirm-no-logo-lifestyle-property-images --confirm-standard-detail-governance-reviewed
```

## Stage 2 — Founder visual review (this report)
Blocked. Fix issues or re-run draft apply, then re-run founder visual review.

## Stage 3 — Active approval
- Allowed: **yes**

## Workflow separation
1. **apply-draft** — writes gallery, property examples, scenarios, copy governance, visibility; does **not** approve active profile.
2. **founder-review** — live rendered checks (this report).
3. **apply-approved** — records active-profile approval only after visual review passes.
