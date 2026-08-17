# Founder Visual Review v34D

- Brand: **Design Hotels** (`design-hotels`)
- Generated: 2026-07-14T17:34:12.334Z
- Overall: **FAIL**

## Post-draft apply status
- Draft applied: **yes**
- Company Validated unchanged: **yes**
- Active profile approved: **no** (draft stage only)
- readyForActiveProfile set: **no**

## Live Brand Explorer checks
- **PASS** — 6 gallery images visible with imageUrl (6/6)
- **FAIL** — 3 property examples visible with hotel images (6 cards)
- **PASS** — No logo/lifestyle/generic property images (gallery + property scan)
- **PASS** — No IMAGE placeholders on scenario cards (ok)
- **PASS** — No FDD / Item 19 / ADR / RevPAR / net contribution language (0 high findings)
- **PASS** — Standard detail governance visible and safe (unknown)
- **PASS** — Company Validated untouched (before=false after=false)
- **FAIL** — Registry traceability for visual slots (15 gaps)
- **PASS** — No stale UI fallback titles (0 risks)

## Failed checks
- `property_examples_hotel_images`: 3 property examples visible with hotel images
- `registry_traceability`: Registry traceability for visual slots

## Stage 1 — Draft apply
- Allowed (apply mode): **yes**
```bash
npm run brand-explorer-active-profile-apply-draft -- --brand design-hotels --apply --approve-brand-explorer-active-profile-draft --approve-brand-explorer-active-profile-copy-governance --founder-approved-active-profile-visual-review --confirm-no-company-validation-claim --confirm-no-summary-url-field --confirm-brand-only --confirm-official-source-images-only --confirm-minimum-six-visible-gallery-images --confirm-property-examples-have-hotel-images --confirm-no-logo-lifestyle-property-images --confirm-standard-detail-governance-reviewed
```

## Stage 2 — Founder visual review (this report)
Blocked. Fix issues or re-run draft apply, then re-run founder visual review.

## Stage 3 — Active approval
- Allowed: **no**
- blocker: founder_visual_review_not_passed
- Flags required for apply: approveBrandExplorerActiveProfile, confirmFounderVisualReviewPassed

## Workflow separation
1. **apply-draft** — writes gallery, property examples, scenarios, copy governance, visibility; does **not** approve active profile.
2. **founder-review** — live rendered checks (this report).
3. **apply-approved** — records active-profile approval only after visual review passes.
