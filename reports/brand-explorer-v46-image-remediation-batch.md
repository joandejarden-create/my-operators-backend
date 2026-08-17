# v46 Brand Explorer Image Remediation Batch

Generated: 2026-07-21T21:39:58.276Z

Read-only. Targets Hotel Indigo, MGallery, SLH. Protects released golden brands. No Presentation writes. No unlock.

## Summary

- OS routing pass: **true**
- Baseline protection: **true**
- Eligibility: {"asset_pack_ready":3}
- Apply draft allowed: **false**

## Target routing

| Brand | OS state | Next action | Full profile | Live gallery | Live openings | Eligibility |
|---|---|---|---|---|---|---|
| hotel-indigo | draft_applied_with_defects | image_remediation | false | 0 | 0 | asset_pack_ready |
| mgallery-collection | draft_applied_with_defects | image_remediation | false | 0 | 0 | asset_pack_ready |
| small-luxury-hotels-of-the-world | draft_applied_with_defects | image_remediation | false | 0 | 0 | asset_pack_ready |

## Per-brand eligibility

### hotel-indigo
- **asset_pack_ready** — Candidate pack meets 6/3/3 with accepted property-specific images; Presentation imageUrl materialization still required before draft apply.
- Accepted gallery/property/scenario: 6/3/3

### mgallery-collection
- **asset_pack_ready** — Candidate pack meets 6/3/3 with accepted property-specific images; Presentation imageUrl materialization still required before draft apply.
- Accepted gallery/property/scenario: 6/3/3

### small-luxury-hotels-of-the-world
- **asset_pack_ready** — Candidate pack meets 6/3/3 with accepted property-specific images; Presentation imageUrl materialization still required before draft apply.
- Accepted gallery/property/scenario: 6/3/3

## Guardrails

- activeRelease: false
- companyValidatedChanges: false
- releasedBrandChanges: false
- incompleteBrandUnlock: false
- genericImagesAccepted: false
- registryOnlyCountedAsRenderReady: false
- presentationWrites: false
