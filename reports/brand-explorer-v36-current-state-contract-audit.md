# Brand Explorer v36A Current-State Contract Audit

Generated: 2026-07-15T13:08:09.536Z
Audit version: v36A
Mode: **read-only** — no Airtable writes

## Executive summary

The codebase already implements most v36 concepts under different names (v34D factory, v34B asset pack, external-owner governance, complete-build batch queue). The primary gap is a **unified, code-derived Full Tab Content Contract** aligned with renderers — not greenfield infrastructure.

## v36 component classification

### Brand Knowledge Pack
- **Status:** partially_exists
- **Recommendation:** safe_to_implement_now
- **Gaps:** No unified versioned JSON schema; No single export consumed by all writers; Facts not integrated into pack

### Claim Ledger
- **Status:** partially_exists
- **Recommendation:** should_be_deferred
- **Gaps:** No claim-to-slot mapping object; No approval state per claim row in factory pipeline; Facts not required for active-profile draft apply today
- **Conflicts:** Design Hotels has 0 approved facts but 90+ presentation rows

### Visual Asset Pack
- **Status:** already_exists
- **Recommendation:** safe_to_implement_now
- **Gaps:** Registry-only assets can pass traceability QA but fail render until Image materialized

### Brand Model Spec
- **Status:** partially_exists
- **Recommendation:** safe_to_implement_now
- **Gaps:** No single spec drives both renderer fallbacks and writer validation; Affiliation vs franchise rules scattered
- **Conflicts:** Commercial/economics static fallbacks ignore brand model spec

### Full Tab Content Contract
- **Status:** partially_exists
- **Recommendation:** needs_migration
- **Gaps:** Docs/code drift: materials.caseStudy, economics.checklist unwired; standards.* rendered but not in slots doc; overview.proof.1–6 rendered but not documented
- **Conflicts:** Renderer expects fields contract does not document

### External Owner Copy Rules
- **Status:** already_exists
- **Recommendation:** safe_to_implement_now
- **Gaps:** footprint.momentum URL exception recently added — not in all writers; No numeric score
- **Conflicts:** Momentum/openings URLs allowed in body but other slots strip URLs

### Presentation Plan Row Contract
- **Status:** partially_exists
- **Recommendation:** safe_to_implement_now
- **Gaps:** No JSON schema validation before apply; Case summary + imageUrl rules not enforced uniformly

### External Owner Readiness Score
- **Status:** partially_exists
- **Recommendation:** safe_to_implement_now
- **Gaps:** Not numeric; not in final-qa overallNumeric; Modal placeholder detection incomplete in Final QA

### Batch Queue Status
- **Status:** already_exists
- **Recommendation:** safe_to_implement_now
- **Gaps:** No persistent queue table; Per-brand halt reasons not unified UI

### Exception Review Report
- **Status:** partially_exists
- **Recommendation:** safe_to_implement_now
- **Gaps:** No single exception report schema; Founder screenshot issues captured ad hoc in writer reports

## Reference brand QA snapshot

### Design Hotels (`design-hotels`)
- Halted: true (critical_visual_defects)
- readyForActiveProfile: false
- Required section score: 38%
- Top blockers:
  - Openings / Examples / Properties: Insufficient complete openings rows.
  - Recent Momentum: No minimum set of dated/source-backed momentum rows.
  - Standard Detail / Where Available: No approved external-display-safe standards owner table package.
  - Loyalty Program: Required loyalty mechanics/proof coverage incomplete.
  - Geographic Footprint: Regional copy still template-thin.

### Small Luxury Hotels of the World (`small-luxury-hotels-of-the-world`)
- Source: factory report (v35D)
- Readiness: blocked_by_factory_rules
- readyForActiveProfile: false
- Founder visual review: FAIL
- Failed visual checks: gallery_six_visible, property_examples_hotel_images, scenario_no_placeholders, ui_fallback_risk
- Top blockers:
  - factory: need_6_visible_gallery_imageUrl_got_0
  - factory: no_visible_scenario_cards_in_api
  - factory: atelier_hardcoded_scenario_fallback_risk:overview.scenario.1
  - factory: atelier_hardcoded_scenario_fallback_risk:overview.scenario.2
  - factory: atelier_hardcoded_scenario_fallback_risk:overview.scenario.3

### Tribute Portfolio (`tribute-portfolio`)
- Halted: false (—)
- readyForActiveProfile: false
- Required section score: 100%

### WoodSpring Suites (`woodspring-suites`)
- Halted: false (—)
- readyForActiveProfile: false
- Required section score: 100%
- Top blockers:
  - Where This Brand Creates the Most Value: high
  - Where This Brand Creates the Most Value: high
  - Where This Brand Creates the Most Value: high
  - Brand Materials: high

### Everhome Suites (`everhome-suites`)
- Halted: false (—)
- readyForActiveProfile: true
- Required section score: 100%

## Implementation risks

- **source_urls_in_external_copy** (high): footprint.openings and footprint.momentum allow trailing URLs; other slots strip URLs via sanitizeAffiliationExternalCopy. Writers without slot-aware policy reintroduce governance hits.
- **source_traceability_fields_missing** (medium): No Presentation columns for sourceIds/sourceFootnote; trace lives in apply reports only
- **registry_link_missing_on_presentation** (high): Brand Asset Registry link may be missing on Presentation table; factory infers traceability via slot+URL matching
- **gallery_requires_presentation_image** (critical): UI reads block.imageUrl from Presentation Image attachment only; registry-only assets do not render
- **registry_passes_qa_not_render** (high): Factory registry traceability rule can pass while gallery_six_visible fails
- **hardcoded_ui_fallbacks** (medium): Commercial, loyalty, economics, proof grids use static fallbacks masking empty slots
- **empty_modal_fields** (high): footprint.openings modals need Case Summary columns or 5+ paragraph Body; Final QA weak on modal placeholders
- **economics_fee_template_language** (medium): Affiliation brands still inherit FDD-oriented economics templates and fee bucket defaults
- **company_validated_wording** (critical): All factory apply paths block Company Validated changes; copy must never imply brand-verified
- **draft_vs_active_apply_separation** (high): apply-approved does not write readyForActiveProfile; founders must not conflate draft materialization with active approval
- **materials_casestudy_unwired** (medium): materials.caseStudy parsed in modal JS but not rendered — data can exist with zero UI
- **openings_body_shape** (high): 4-paragraph footprint.openings bodies lose scenario/meta/teaser in parser

## Recommended v36 implementation plan

### Implement immediately
- Visual Asset Pack contract (formalize v34B output + render-readiness flag)
- External Owner Copy Rules (centralize slot URL policy + affiliation sanitizers)
- Presentation Plan Row Contract (validate before apply)
- Batch Queue Status (surface complete-build-orchestrator aggregate)
- Exception Review Report (merge founder queue + visual defects + blockers)
- Brand Model Spec (extract from ACTIVE_PROFILE_BRAND_CONFIGS + lifestyle config)

### Next implementation prompt

> Implement v36B Brand Knowledge Pack + Presentation Plan Row Contract as read-only lib modules that load ACTIVE_PROFILE_BRAND_CONFIGS, approved sources, and API blocks; emit validation report per brand without Airtable writes. Add render-readiness and fallback-active flags to Visual Asset Pack output. Wire into factory preflight stage before build-draft.

## Validation tests

- `test:brand-explorer-active-profile-staged-apply`: **pass** (exit 0)
- `test:partner-intelligence-publish-readiness`: **pass** (exit 0)
- `test:partner-intelligence-profile-governance-publish`: **pass** (exit 0)
