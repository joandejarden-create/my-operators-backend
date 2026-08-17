# v37C-R1 Display Gating + Visual Candidate Integration

Generated: 2026-07-21T09:40:21.021Z
Mode: **dry-run**

## Batch readiness
## hotel-indigo
- source_ready: true
- visual_candidate_pack_ready: false
- asset_pack_ready: false
- build_draft_ready: false
- external_display_safe: false
- apply_draft_allowed: false
- should_hide_external_profile: true
- recommended_next_action: image remediation required (CALA → U.S. → global property-specific replacements)
- apply_draft_blocked_reason:
  - visual_candidate_pack_incomplete
  - asset_pack_incomplete
  - build_draft_incomplete
  - render_contract_fail
  - external_display_not_safe
  - hotel_indigo_property_specific_images_below_minimum

## mgallery-collection
- source_ready: true
- visual_candidate_pack_ready: true
- asset_pack_ready: true
- build_draft_ready: true
- external_display_safe: false
- apply_draft_allowed: false
- should_hide_external_profile: true
- recommended_next_action: bridge candidates into presentation/registry materialization pipeline before apply-draft
- apply_draft_blocked_reason:
  - render_contract_fail
  - external_display_not_safe

## Renderer suppression rules (owner-facing)
- Hide helper/fallback text when scenario rows are missing.
- Hide standards placeholder paragraphs for source-only/incomplete profiles.
- Do not treat Source Library-only updates as profile-ready.
- Keep QA/readiness diagnostics in reports, not external UI.