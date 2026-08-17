# v47 Brand Explorer Batch Image Materialization

Generated: 2026-07-21T22:42:45.561Z

Apply mode — Presentation Image + registry candidate writes only when gates passed.

## Summary

- OS routing pass: **true**
- Baseline protection: **true**
- Eligibility: {"build_draft_ready":3}
- Materialization blocked brands: **0**
- Presentation writes: **true**
- apply_draft_allowed projected: **true**

## Target routing

| Brand | OS state | Next action | Full profile | Eligibility | Projection |
|---|---|---|---|---|---|
| hotel-indigo | draft_applied_with_defects | image_remediation | false | build_draft_ready | 6/6 g / 3/3 p |
| mgallery-collection | draft_applied_with_defects | image_remediation | false | build_draft_ready | 6/6 g / 3/3 p |
| small-luxury-hotels-of-the-world | draft_applied_with_defects | image_remediation | false | build_draft_ready | 6/6 g / 3/3 p |

## Guardrails

- activeRelease: false
- companyValidatedChanges: false
- releasedBrandChanges: false
- incompleteBrandUnlock: false
- rawUrlsInOwnerFacingCopy: false
- genericImagesAccepted: false
- registryOnlyCountedAsRenderReady: false
