# Founder Visual Review v34D

- Brand: **Design Hotels** (`design-hotels`)
- Generated: 2026-07-15T08:52:45.361Z
- Overall: **PASS**

## Live Brand Explorer checks
- **PASS** — 6 gallery images visible with imageUrl (6/6)
- **PASS** — 3 property examples visible with hotel images (3 cards)
- **PASS** — No logo/lifestyle/generic property images (gallery + property scan)
- **PASS** — No IMAGE placeholders on scenario cards (ok)
- **PASS** — No visible source URLs, governance language, or modal placeholders in owner copy (0 blockers)
- **PASS** — No FDD / Item 19 / ADR / RevPAR / net contribution language (0 high findings)
- **PASS** — Standard detail governance visible and safe (unknown)
- **PASS** — Company Validated untouched (before=false after=false)
- **PASS** — Registry traceability for visual slots (0 gaps)
- **PASS** — No stale UI fallback titles (0 risks)

## Stage 1 — Draft apply
- Allowed (apply mode): **yes**
- Flags required for apply: approveBrandExplorerActiveProfileDraft, founderVisualReview, confirmNoCompanyValidationClaim, confirmNoSummaryUrlField, confirmBrandOnly, confirmOfficialSourceImagesOnly, confirmMinimumSixVisibleGalleryImages, confirmPropertyExamplesHaveHotelImages, confirmNoLogoLifestylePropertyImages, confirmStandardDetailGovernanceReviewed, approveCopyGovernance
```bash
npm run brand-explorer-active-profile-apply-draft -- --brand design-hotels --apply --approve-brand-explorer-active-profile-draft --approve-brand-explorer-active-profile-copy-governance --founder-approved-active-profile-visual-review --confirm-no-company-validation-claim --confirm-no-summary-url-field --confirm-brand-only --confirm-official-source-images-only --confirm-minimum-six-visible-gallery-images --confirm-property-examples-have-hotel-images --confirm-no-logo-lifestyle-property-images --confirm-standard-detail-governance-reviewed
```

## Stage 2 — Founder visual review (this report)
All checks passed. Founder may proceed to Stage 3 active approval.

## Stage 3 — Active approval
- Allowed: **yes**
- Flags required for apply: approveBrandExplorerActiveProfile, confirmFounderVisualReviewPassed, confirmNoCompanyValidationClaim, confirmBrandOnly
```bash
npm run brand-explorer-active-profile-apply-approved -- --brand design-hotels --apply --approve-brand-explorer-active-profile --confirm-founder-visual-review-passed --confirm-no-company-validation-claim --confirm-brand-only
```

## Workflow separation
1. **apply-draft** — writes gallery, property examples, scenarios, copy governance, visibility; does **not** approve active profile.
2. **founder-review** — live rendered checks (this report).
3. **apply-approved** — records active-profile approval only after visual review passes.
