# v39 Brand Explorer Active Profile Release Audit

Generated: 2026-07-21T11:20:56.525Z

**Live API / render contract is source of truth.** Report-only readiness is not release readiness.

## Summary
- Brands audited: 3
- Primary safe_to_unlock_after_active_approval: 0
- Primary release_remediation_required: 0
- Primary mapping_fix_required: 3
- Primary not_owner_ready: 0
- Incomplete control pass: **yes**
- Active release apply executed: **no**

## Per brand
### everhome-suites (primary_release)
- displayState: `draft_applied_with_defects`
- shouldRenderFullProfile: false
- release outcome: **false_blocker_due_to_mapping**
- allowed next action: mapping_fix_required
- failed gates: founder_visual_review_passed, active_profile_approval_set, should_render_full_profile, display_state_release_ready, render_contract_pass
- report readyForActiveProfile: true
- mismatches: report_ready_but_api_locked, active_approval_missing, legacy_ready_signal_not_connected, registry_ready_but_render_locked

### kimpton (primary_release)
- displayState: `draft_applied_with_defects`
- shouldRenderFullProfile: false
- release outcome: **false_blocker_due_to_mapping**
- allowed next action: mapping_fix_required
- failed gates: founder_visual_review_passed, active_profile_approval_set, should_render_full_profile, display_state_release_ready, render_contract_pass
- report readyForActiveProfile: true
- mismatches: report_ready_but_api_locked, active_approval_missing, legacy_ready_signal_not_connected, registry_ready_but_render_locked

### radisson-individuals-by-choice (primary_release)
- displayState: `draft_applied_with_defects`
- shouldRenderFullProfile: false
- release outcome: **false_blocker_due_to_mapping**
- allowed next action: mapping_fix_required
- failed gates: founder_visual_review_passed, active_profile_approval_set, should_render_full_profile, display_state_release_ready, render_contract_pass
- report readyForActiveProfile: true
- mismatches: report_ready_but_api_locked, active_approval_missing, legacy_ready_signal_not_connected, registry_ready_but_render_locked

## Designed release apply (NOT executed)
```
npm run brand-explorer-active-release-apply -- --brands everhome-suites,kimpton,radisson-individuals-by-choice --apply --approve-brand-explorer-active-release --confirm-founder-visual-review-passed --confirm-external-quality-lock-passed --confirm-six-gallery-imageurls --confirm-three-property-example-imageurls --confirm-no-company-validation-claim --confirm-no-forbidden-owner-copy --confirm-brand-only
```