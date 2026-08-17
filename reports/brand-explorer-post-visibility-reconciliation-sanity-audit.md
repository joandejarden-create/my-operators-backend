# Brand Explorer — Post-Visibility Reconciliation Sanity Audit

Generated: 2026-07-22T17:48:32.030Z
Version: post-visibility-reconciliation-sanity-audit-v1

**Audit-only.** No Company Validated / Source Library / Registry / content / image / release-field writes.

## Summary

- Brands evaluated: **17**
- External quality lock cohort size: **7** (still 7/7 by design)
- Restored legacy publicly visible: **4**
- Primary locked: **1**
- Rows with mismatch flags: **5**

External quality lock defaults to PRIMARY_RELEASE_SLUGS only (7 brands). Restored legacy Ascend/Comfort/Curio/Tribute are publicly visible via transitional legacy unlock but are not members of that test cohort yet.

### Live buckets

- restored_legacy_approved_cohort: **4**
- remediation_cohort: **3**
- founder_preview_only_cohort: **3**
- other_visible: **6**
- remediation_cohort_primary: **1**

## Cohort definitions

### Primary active release

`everhome-suites, kimpton, radisson-individuals-by-choice, design-hotels, hotel-indigo, mgallery-collection, small-luxury-hotels-of-the-world`

### Restored legacy approved (transitional public)

`ascend, comfort-inn-suites, curio-collection, tribute-portfolio`

Add dedicated legacy-restored quality lock OR expand EXTERNAL_QUALITY_LOCK_COHORT once image role-match / uniqueness gates are accepted for these brands.

### External quality lock cohort

`everhome-suites, kimpton, radisson-individuals-by-choice, design-hotels, hotel-indigo, mgallery-collection, small-luxury-hotels-of-the-world`

7/7 after legacy restore is expected: restored Ascend/Comfort/Curio/Tribute are outside this cohort by design until explicitly added.

## Radisson Individuals conflict

Single canonical Brand Basics record for Radisson Individuals by Choice (recRyvM8OmLlDj9G7). 12 other Radisson* Brand Basics rows are sibling Choice brands, not duplicate Individuals profiles. No separate “Radisson Individuals” (without by Choice) record exists. image_remediation / internal_preview_blocked on radisson-individuals-by-choice refers to this same record’s live image-uniqueness / OS debt — not a wrong legacy seed.

- Configured canonical record: `recRyvM8OmLlDj9G7`
- Unique Radisson* Brand Basics IDs found: **13**
- Duplicate Individuals risk: **false**
- Sibling Radisson* brands (not Individuals duplicates): **12**

### Formula search (Brand Name contains Radisson)

- `rec1sizeL0DhslQL8` — Radisson Inn & Suites
- `rec2DDyPu38C6zDBC` — Radisson Collection
- `rec3nyARnkn97W9w6` — Country Inn & Suites by Radisson
- `recFLwYLMKLbXZFM6` — Radisson RED
- `recNrxgX2wRIZeKJ2` — Prize by Radisson
- `recPAB0PgJyKE2v09` — Radisson Collection by Choice
- `recRyvM8OmLlDj9G7` — Radisson Individuals by Choice
- `recUX55vdDCcMHkDg` — Radisson Blu
- `recWPEvxBQxVVzSq3` — Radisson Blu by Choice
- `recXYvwtNQGUzFZcn` — Radisson
- `recmKqo7M7mLZgRqQ` — Radisson RED by Choice
- `recyI1O61Or7LXwXl` — Park Inn by Radisson
- `recywbx1YQSTCPqW1` — Radisson by Choice

### Name probe results

- **Radisson Individuals by Choice**: found=true id=`recRyvM8OmLlDj9G7` state=draft_applied_with_defects full=false
- **Radisson Individuals**: found=false id=`—` state=— full=null
- **Radisson by Choice**: found=true id=`recywbx1YQSTCPqW1` state=draft_applied_with_defects full=false
- **Radisson Blu by Choice**: found=true id=`recWPEvxBQxVVzSq3` state=draft_applied_with_defects full=false
- **Radisson Blu**: found=true id=`recUX55vdDCcMHkDg` state=hidden_incomplete full=false
- **Radisson RED**: found=true id=`recFLwYLMKLbXZFM6` state=hidden_incomplete full=false
- **Radisson Collection**: found=true id=`rec2DDyPu38C6zDBC` state=hidden_incomplete full=false

## Internal-preview-copy investigation (Everhome / Kimpton / Radisson)

### everhome-suites

- Live internal hits: **0**
- Projected residual hits: **0** (residual patches=0)
- Public external path hits: **0**
- Public affected while full profile: **false**
- Blocks release baseline: **false**
- Founder banner false positive: **false**
- Note: clean_on_live_internal_preview

### kimpton

- Live internal hits: **0**
- Projected residual hits: **0** (residual patches=0)
- Public external path hits: **0**
- Public affected while full profile: **false**
- Blocks release baseline: **false**
- Founder banner false positive: **false**
- Note: clean_on_live_internal_preview

### radisson-individuals-by-choice

- Live internal hits: **1**
- Projected residual hits: **1** (residual patches=0)
- Public external path hits: **0**
- Public affected while full profile: **false**
- Blocks release baseline: **false**
- Founder banner false positive: **true**
- Note: founder_preview_banner_phrase_internal_review_false_positive_on_locked_brand_public_unaffected

Live hits:
- `internal_review` internal review: internal review


## Full profile matrix

| Brand | Slug | Record ID | OS state | OS action | Public display | shouldRenderFull | Legacy hist | Active approved | Founder pass | PRIMARY | Legacy seed | Restored | EQL cohort | Golden | Gate failures | Expected | Mismatches |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Ascend Hotel Collection | ascend | reclkgOzvAcBheUSo | display:draft_applied_with_defects | not_in_os_default_cohort | draft_applied_with_defects | true | true | true | true | false | true | true | false | false | — | restored_legacy_approved_public_visible_transitional | restored_legacy_publicly_visible_but_not_in_external_quality_lock_cohort_transitional |
| Autograph Collection | autograph-collection | recEJCTDj1zrsjPM6 | display:hidden_incomplete | not_in_os_default_cohort | hidden_incomplete | false | true | false | false | false | true | false | false | false | missing_presentation_rows; missing_scenario_rows; visual_asset_counts; image_uniqueness; image_role_match; active_profile_not_approved; founder_visual_review_not_passed | legacy_seed_locked_other | — |
| Comfort Inn & Suites | comfort-inn-suites | recOzH5iAE1xEjyD0 | display:draft_applied_with_defects | not_in_os_default_cohort | draft_applied_with_defects | true | true | true | true | false | true | true | false | false | — | restored_legacy_approved_public_visible_transitional | restored_legacy_publicly_visible_but_not_in_external_quality_lock_cohort_transitional |
| Country Inn & Suites by Choice | country-inn-suites | recaayt9u7YYg8h7Y | display:legacy_approved_pending_migration | not_in_os_default_cohort | legacy_approved_pending_migration | false | true | false | false | false | true | false | false | false | visual_asset_counts; image_uniqueness; image_role_match; active_profile_not_approved; founder_visual_review_not_passed | legacy_seed_locked_other | — |
| Curio Collection by Hilton | curio-collection | receQkxgjlezsc1xg | display:draft_applied_with_defects | not_in_os_default_cohort | draft_applied_with_defects | true | true | true | true | false | true | true | false | false | — | restored_legacy_approved_public_visible_transitional | restored_legacy_publicly_visible_but_not_in_external_quality_lock_cohort_transitional |
| Design Hotels | design-hotels | rec02zPClpWUTCyXM | asset_ready | image_remediation | active_profile_ready | true | false | true | true | true | false | false | true | true | visual_asset_pack_ready; tab_factory_audit; rendered_field_completeness_audit; rendered_field_completeness_remediation; image_distinctiveness; golden_content_quality | primary_release_visible_with_gate_debt | — |
| Everhome Suites | everhome-suites | recqkkrsevi4r9ibj | draft_applied_with_defects | apply_remediation | active_profile_ready | true | false | true | true | true | false | false | true | true | tab_factory_audit; rendered_field_completeness_audit; rendered_field_completeness_remediation; golden_content_quality | primary_release_visible_with_gate_debt | — |
| Handwritten Collection | handwritten-collection | rec7hTXwMRC81EPqz | display:hidden_incomplete | not_in_os_default_cohort | hidden_incomplete | false | true | false | false | false | true | false | false | false | missing_presentation_rows; missing_scenario_rows; visual_asset_counts; image_uniqueness; image_role_match; active_profile_not_approved; founder_visual_review_not_passed | legacy_seed_locked_other | — |
| Hotel Indigo | hotel-indigo | recegXrqaPiSLGCIe | active_profile_ready | no_action | active_profile_ready | true | false | true | true | true | false | false | true | true | — | primary_release_visible_with_gate_debt | — |
| Kimpton Hotels | kimpton | recCKuXCmGvxHPfb3 | draft_applied_with_defects | apply_remediation | active_profile_ready | true | false | true | true | true | false | false | true | true | tab_factory_audit; rendered_field_completeness_audit; rendered_field_completeness_remediation; golden_content_quality | primary_release_visible_with_gate_debt | — |
| MGallery Collection | mgallery-collection | recrWCD1LMqu864oU | active_profile_ready | no_action | active_profile_ready | true | false | true | true | true | false | false | true | true | — | primary_release_visible_with_gate_debt | — |
| Radisson Individuals by Choice | radisson-individuals-by-choice | recRyvM8OmLlDj9G7 | internal_preview_blocked | image_remediation | draft_applied_with_defects | false | false | true | true | true | false | false | true | true | image_uniqueness; visual_asset_pack_ready; property_examples_three_imageurl; no_visible_source_urls; no_forbidden_copy_presentation; internal_preview_owner_copy_live; tab_factory_audit; rendered_field_completeness_audit; rendered_field_completeness_remediation; image_distinctiveness; image_role_match; golden_content_quality | primary_release_locked_needs_remediation | radisson_individuals_in_primary_release_but_not_active_profile_ready_live_image_or_copy_debt |
| Small Luxury Hotels of the World | small-luxury-hotels-of-the-world | recjjSnY2opb8P4DG | active_profile_ready | no_action | active_profile_ready | true | false | true | true | true | false | false | true | true | — | primary_release_visible_with_gate_debt | — |
| Suburban Studios | suburban-studios | reclcjg5Foa9Vs5TC | display:legacy_approved_pending_migration | not_in_os_default_cohort | legacy_approved_pending_migration | false | true | false | false | false | true | false | false | false | visual_asset_counts; image_uniqueness; image_role_match; active_profile_not_approved; founder_visual_review_not_passed | legacy_seed_locked_other | — |
| Tribute Portfolio | tribute-portfolio | recCvV0PuZOi8c3hC | display:draft_applied_with_defects | not_in_os_default_cohort | draft_applied_with_defects | true | true | true | true | false | true | true | false | false | — | restored_legacy_approved_public_visible_transitional | restored_legacy_publicly_visible_but_not_in_external_quality_lock_cohort_transitional |
| Vignette Collection | vignette-collection | recDwzv86TWnz2gGB | display:hidden_incomplete | not_in_os_default_cohort | hidden_incomplete | false | true | false | false | false | true | false | false | false | missing_presentation_rows; missing_scenario_rows; visual_asset_counts; image_uniqueness; image_role_match; active_profile_not_approved; founder_visual_review_not_passed | legacy_seed_locked_other | — |
| WoodSpring Suites | woodspring-suites | recsOd51NzRPYsMko | display:legacy_approved_pending_migration | not_in_os_default_cohort | legacy_approved_pending_migration | false | true | false | false | false | true | false | false | false | missing_scenario_rows; visual_asset_counts; image_uniqueness; image_role_match; active_profile_not_approved; founder_visual_review_not_passed | legacy_seed_locked_other | — |

## Recommendations

- Treat VISIBILITY_RESTORED_RELEASE_SLUGS as a documented transitional public cohort until a legacy quality lock (or cohort expansion) is approved.
- Do not interpret image_remediation on radisson-individuals-by-choice as a duplicate-record problem unless radissonIndividualsConflict.duplicateRisk shows conflicting Individuals record IDs.
- Resolve internal-preview-owner-copy hits on Everhome/Kimpton/Radisson before treating OS release-readiness as fully green; external quality lock PASS alone is insufficient for founder path.
