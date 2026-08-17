# v39 Brand Explorer Active Profile Release Audit

Read-only audit that reconciles report readiness vs live Brand Library API / Presentation imageUrl gates.

```bash
npm run brand-explorer-v39-active-release-audit -- --brands everhome-suites,kimpton,radisson-individuals-by-choice,hotel-indigo,mgallery-collection,design-hotels,small-luxury-hotels-of-the-world --dry-run
```

## Source of truth
Live `shouldRenderFullProfile` + Presentation `imageUrl` + external DOM quality lock.
complete-build `readyForActiveProfile` alone is never sufficient.

## Release outcomes
- `safe_to_unlock_after_active_approval`
- `release_remediation_required`
- `false_blocker_due_to_mapping`
- `not_owner_ready`

## Active release apply
Designed but not executed in v39. Requires explicit gated command with founder + DOM + imageUrl confirms.

Guardrails: no Company Validated changes; no incomplete brand unlock; no blind active approval.