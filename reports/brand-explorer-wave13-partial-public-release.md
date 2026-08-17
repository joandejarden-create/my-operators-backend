# Brand Explorer Wave 13 — Partial Public Release

Version: `wave13-partial-public-release-v1` · Generated: 2026-07-27T18:18:28.789Z
Validation appended: 2026-07-27T19:07:34.453Z
Mode: **APPLY** · writePerformed: **true**

Active universe: **45** (39 → 45)
Ready (writes): `wave13_six_brand_partial_release_complete_so_held`
Acceptance complete: **false** (PVQL public-full-only + 24-tab freeze blocked by Wave 13 section-pattern debt)

## Scope

- Released (6): `mama-shelter`, `mercure`, `ibis`, `novotel`, `pullman`, `fairmont-hotels-and-resorts`
- Held: `so-hotels-and-resorts` — Under Review, no release fields, not in intentional restore registry
- Excluded / untouched: The House of Originals · Morgans Originals · Radisson Collection

## Stage 9–10 writes (done)

- Brand Status → Active for six only
- Release fields on six Basics only: Active Profile Approved, Ready for Active Profile, Active Profile Approved Date, Founder Visual Review Pass
- Intentional restore registry 29 → 35 (six added; SO/ not added)
- No Company Validated / Source Library / Registry approval / content / image writes

## Post-release validation

| Gate | Result |
| --- | --- |
| Active universe SoT | **45** (reconcilesTo39=false) |
| PVQL overallPass | **true** (legacy flagged allowed) |
| PVQL --public-full-only | **FAIL** — 39/45 lockPass; six Wave 13 fail `tab_factory_audit` |
| shouldRenderFullProfile | **45/45 true** (six are public-full) |
| Tab-factory (six) | failFindings=0 but auditPass=false (section_pattern_parity) |
| 24-tab quality audit | **do_not_freeze_remediation_required** — 39 approve / 6 remediation_required |
| Recent momentum evidence (permanent 3) | **PASS** |
| Mandatory release gates | **PASS** |
| brand-explorer-os release-readiness --skip-regression | completed (primary cohort clean) |

### Wave 13 PVQL / tab-factory failure mode

- `geographic_footprint`: only 1 filled regional card (need ≥3)
- `recent_momentum`: dated/URL/structured date lines below min; thin bodies
- Completeness field fails: **0** (rendered_field_completeness PASS)
- Images uniqueness + role-match: **PASS**

### Protected original 39

- Remain `public_full_clean` / `approve_for_baseline_freeze`
- Not modified by Stages 9–10

## Baseline freeze

- **mayFreeze45BaselineNow: false**
- Wait for SO/ cleanup + promotion **and** Wave 13 section-pattern cleanup on the six before revising protected baseline 39 → 45/46

## Acceptance checklist

- 1_six_promoted_active: **true**
- 2_six_release_fields: **true**
- 3_so_under_review: **true**
- 4_so_no_release_fields: **true**
- 5_house_excluded: **true**
- 6_morgans_untouched: **true**
- 7_radisson_collection_untouched: **true**
- 8_active_universe_45: **true**
- 9_pvql_pass_active_universe: **false**
- 10_quality_audit_pass_active_universe: **false**
- 11_evidence_quality_pass: **true**
- 12_mandatory_release_gates_pass: **true**
- 13_company_validated_untouched: **true**
- 14_source_library_untouched: **true**
- 15_registry_untouched: **true**
- 16_no_content_image_rewrites: **true**
- 17_ready_statement: **wave13_six_brand_partial_release_complete_so_held**

## Blocker / next task

`wave13_section_pattern_parity_blocks_pvql_and_quality_freeze`: Six released brands render public-full (shouldRenderFullProfile=true) but tab-factory auditPass=false due to geographic_footprint (<3 filled regions) and recent_momentum card structure (dates/URLs/bodies). Fix requires Presentation content writes — out of scope for Stages 9–10.

Next: Separate Wave 13 post-release section-pattern cleanup (geo regions + momentum cards) for the six brands; keep SO/ held until its minor cleanup task.
