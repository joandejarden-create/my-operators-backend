# v39 Release Plan — kimpton

- Cohort: primary_release
- Current display state: `draft_applied_with_defects`
- shouldRenderFullProfile: **false**
- Release outcome: **false_blocker_due_to_mapping**
- Reason: Report ready signal not connected to live Active Profile Approved — mapping/approval write only
- Allowed next action: **mapping_fix_required**
- Founder confirmation required: yes
- Generic release apply can handle: yes
- Brand-specific patch needed: no
- Ready for active approval: **no** (audit only)

## Failed gates
- `founder_visual_review_passed`
- `active_profile_approval_set`
- `should_render_full_profile`
- `display_state_release_ready`
- `render_contract_pass`

## Remediation items
- **founder_visual_review_passed**: Run founder visual review and record Pass (Airtable write: yes; code: no; type: true_issue)
- **active_profile_approval_set**: Gated active-release apply after founder + DOM gates pass (Airtable write: yes; code: no; type: true_issue)
- **should_render_full_profile**: Satisfy active_profile_ready / external_owner_ready gates (Airtable write: no; code: no; type: true_issue)
- **display_state_release_ready**: Clear failed gates so display state advances (Airtable write: no; code: no; type: true_issue)
- **render_contract_pass**: Ensure Presentation imageUrl matches registry/render readiness (Airtable write: yes; code: no; type: true_issue)

## Required writes (if later gated apply)
- Founder Visual Review Pass
- Ready for Active Profile
- Active Profile Approved

## Exact allowed next command
```
Code/mapping fix: connect complete-build readyForActiveProfile to live Active Profile Approved fields — then re-audit
```

## Blocked commands
- brand-explorer-active-release-apply (until gates pass)
- any Company Validated write
- Source Library writes from this pipeline

## Designed release apply (NOT executed)
```
npm run brand-explorer-active-release-apply -- --brands kimpton --apply --approve-brand-explorer-active-release --confirm-founder-visual-review-passed --confirm-external-quality-lock-passed --confirm-six-gallery-imageurls --confirm-three-property-example-imageurls --confirm-no-company-validation-claim --confirm-no-forbidden-owner-copy --confirm-brand-only
```

Guardrails: no Company Validated changes; no Source Library writes; no blind unlock.