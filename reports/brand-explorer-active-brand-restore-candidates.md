# Active Brand Restore Candidates

Brands that are already clean or closest to public-full restore.

| Brand | Slug | Bucket | Fastest safe path | Risk | Founder review? | Gate failures |
| --- | --- | --- | --- | --- | --- | --- |
| Ascend Hotel Collection | `ascend` | public_full_clean | maintain_public_visibility_baseline | Low | false | — |
| Comfort Inn & Suites | `comfort-inn-suites` | public_full_clean | maintain_public_visibility_baseline | Low | false | — |
| Curio Collection by Hilton | `curio-collection` | public_full_clean | maintain_public_visibility_baseline | Low | false | — |
| Design Hotels | `design-hotels` | public_full_clean | maintain_public_visibility_baseline | Low | false | — |
| Everhome Suites | `everhome-suites` | public_full_clean | maintain_public_visibility_baseline | Low | false | — |
| Hotel Indigo | `hotel-indigo` | public_full_clean | maintain_public_visibility_baseline | Low | false | — |
| Kimpton Hotels | `kimpton` | public_full_clean | maintain_public_visibility_baseline | Low | false | — |
| MGallery Collection | `mgallery-collection` | public_full_clean | maintain_public_visibility_baseline | Low | false | — |
| Radisson Individuals by Choice | `radisson-individuals-by-choice` | public_full_clean | maintain_public_visibility_baseline | Low | false | — |
| Small Luxury Hotels of the World | `small-luxury-hotels-of-the-world` | public_full_clean | maintain_public_visibility_baseline | Low | false | — |
| Tribute Portfolio | `tribute-portfolio` | public_full_clean | maintain_public_visibility_baseline | Low | false | — |

## Historically done but not public-full

### Autograph Collection (`autograph-collection`)
- Why historically done: LEGACY_SEED_BRANDS — founder historically finished profile; API legacyHistoricalApproved; LEGACY_SEED_SLUGS
- Why not public-full: founder_visual_review_not_pass; active_profile_not_approved; thin_presentation; gallery_distinct_lt_6; outside_primary_and_visibility_restored_cohorts
- Gate blockers: tab_factory_audit, rendered_field_completeness, image_uniqueness, image_role_match, golden_content_quality
- Blocker type: substantive
- Bucket: true_incomplete
- Fastest safe path: tab_factory_build_from_scratch
- Command: `npm run brand-explorer-active-profile-factory -- --brand autograph-collection`

### Country Inn & Suites by Choice (`country-inn-suites`)
- Why historically done: LEGACY_SEED_BRANDS — founder historically finished profile; API legacyHistoricalApproved; LEGACY_SEED_SLUGS; Presentation depth (210 rows)
- Why not public-full: founder_visual_review_not_pass; active_profile_not_approved; outside_primary_and_visibility_restored_cohorts
- Gate blockers: tab_factory_audit, rendered_field_completeness, image_uniqueness, image_role_match, golden_content_quality
- Blocker type: substantive
- Bucket: content_remediation_needed
- Fastest safe path: targeted_presentation_field_gate_completion
- Command: `npm run brand-explorer-public-profile-stabilization -- --brands country-inn-suites`

### Handwritten Collection (`handwritten-collection`)
- Why historically done: LEGACY_SEED_BRANDS — founder historically finished profile; API legacyHistoricalApproved; LEGACY_SEED_SLUGS
- Why not public-full: founder_visual_review_not_pass; active_profile_not_approved; thin_presentation; gallery_distinct_lt_6; outside_primary_and_visibility_restored_cohorts
- Gate blockers: tab_factory_audit, rendered_field_completeness, image_uniqueness, image_role_match, golden_content_quality
- Blocker type: substantive
- Bucket: true_incomplete
- Fastest safe path: tab_factory_build_from_scratch
- Command: `npm run brand-explorer-active-profile-factory -- --brand handwritten-collection`

### Quality Inn (`quality-inn`)
- Why historically done: ACTIVE_BRAND_BATCH governance cohort; Presentation depth (198 rows)
- Why not public-full: founder_visual_review_not_pass; active_profile_not_approved; gallery_distinct_lt_6; outside_primary_and_visibility_restored_cohorts
- Gate blockers: brand_not_found, tab_factory_audit, rendered_field_completeness, image_uniqueness, image_role_match, golden_content_quality
- Blocker type: substantive
- Bucket: content_remediation_needed
- Fastest safe path: targeted_presentation_field_gate_completion
- Command: `npm run brand-explorer-public-profile-stabilization -- --brands quality-inn`

### Radisson by Choice (`radisson`)
- Why historically done: ACTIVE_BRAND_AUDIT_TARGETS — historically Explorer-active; ACTIVE_BRAND_AUDIT_TARGETS + Choice family active profile; Presentation depth (205 rows)
- Why not public-full: founder_visual_review_not_pass; active_profile_not_approved; outside_primary_and_visibility_restored_cohorts
- Gate blockers: brand_not_found, tab_factory_audit, rendered_field_completeness, golden_content_quality
- Blocker type: substantive
- Bucket: content_remediation_needed
- Fastest safe path: targeted_presentation_field_gate_completion
- Command: `npm run brand-explorer-public-profile-stabilization -- --brands radisson`

### Radisson Blu by Choice (`radisson-blu`)
- Why historically done: ACTIVE_BRAND_AUDIT_TARGETS — historically Explorer-active; ACTIVE_BRAND_AUDIT_TARGETS + Choice family repair writer; Presentation depth (223 rows)
- Why not public-full: founder_visual_review_not_pass; active_profile_not_approved; outside_primary_and_visibility_restored_cohorts
- Gate blockers: brand_not_found, tab_factory_audit, rendered_field_completeness, golden_content_quality
- Blocker type: substantive
- Bucket: content_remediation_needed
- Fastest safe path: targeted_presentation_field_gate_completion
- Command: `npm run brand-explorer-public-profile-stabilization -- --brands radisson-blu`

### Radisson RED by Choice (`radisson-red`)
- Why historically done: ACTIVE_BRAND_BATCH + presentation fixtures (206 rows live); Presentation depth (206 rows)
- Why not public-full: founder_visual_review_not_pass; active_profile_not_approved; outside_primary_and_visibility_restored_cohorts
- Gate blockers: brand_not_found, tab_factory_audit, rendered_field_completeness, golden_content_quality
- Blocker type: substantive
- Bucket: content_remediation_needed
- Fastest safe path: targeted_presentation_field_gate_completion
- Command: `npm run brand-explorer-public-profile-stabilization -- --brands radisson-red`

### Suburban Studios (`suburban-studios`)
- Why historically done: LEGACY_SEED_BRANDS — founder historically finished profile; API legacyHistoricalApproved; LEGACY_SEED_SLUGS; Presentation depth (196 rows)
- Why not public-full: founder_visual_review_not_pass; active_profile_not_approved; gallery_distinct_lt_6; outside_primary_and_visibility_restored_cohorts
- Gate blockers: tab_factory_audit, rendered_field_completeness, image_uniqueness, image_role_match, golden_content_quality
- Blocker type: substantive
- Bucket: content_remediation_needed
- Fastest safe path: targeted_presentation_field_gate_completion
- Command: `npm run brand-explorer-public-profile-stabilization -- --brands suburban-studios`

### Vignette Collection (`vignette-collection`)
- Why historically done: LEGACY_SEED_BRANDS — founder historically finished profile; API legacyHistoricalApproved; LEGACY_SEED_SLUGS
- Why not public-full: founder_visual_review_not_pass; active_profile_not_approved; thin_presentation; gallery_distinct_lt_6; outside_primary_and_visibility_restored_cohorts
- Gate blockers: tab_factory_audit, rendered_field_completeness, image_uniqueness, image_role_match, golden_content_quality
- Blocker type: substantive
- Bucket: true_incomplete
- Fastest safe path: tab_factory_build_from_scratch
- Command: `npm run brand-explorer-active-profile-factory -- --brand vignette-collection`

### WoodSpring Suites (`woodspring-suites`)
- Why historically done: LEGACY_SEED_BRANDS — founder historically finished profile; API legacyHistoricalApproved; LEGACY_SEED_SLUGS; Presentation depth (209 rows)
- Why not public-full: founder_visual_review_not_pass; active_profile_not_approved; gallery_distinct_lt_6; outside_primary_and_visibility_restored_cohorts
- Gate blockers: tab_factory_audit, rendered_field_completeness, image_uniqueness, image_role_match, golden_content_quality
- Blocker type: substantive
- Bucket: content_remediation_needed
- Fastest safe path: targeted_presentation_field_gate_completion
- Command: `npm run brand-explorer-public-profile-stabilization -- --brands woodspring-suites`
