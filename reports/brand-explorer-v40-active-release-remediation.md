# v40 Brand Explorer Active Release Remediation

Generated: 2026-07-21T10:58:19.583Z

Apply executed: Presentation owner-copy patches only. No unlock / no active approval / no Company Validated.

## Summary
- Brands: 3
- Total blockers: 61
- Total patches: 52
- Owner copy projected clean: 3/3
- Records patched: 45
- Apply errors: 0
- Incomplete control pass: **yes**
- Unlock in v40: **no**
- Active approval touched: **no**

## everhome-suites
- displayState: `draft_applied_with_defects`
- patches: 14
- owner copy after: projected clean
- still blocked: founder=true approval=true
- apply: records=11 errors=0

## kimpton
- displayState: `draft_applied_with_defects`
- patches: 22
- owner copy after: projected clean
- still blocked: founder=true approval=true
- apply: records=19 errors=0

## radisson-individuals-by-choice
- displayState: `draft_applied_with_defects`
- patches: 16
- owner copy after: projected clean
- still blocked: founder=true approval=true
- apply: records=15 errors=0

## Apply command used
```
npm run brand-explorer-v40-active-release-remediation -- --brands everhome-suites,kimpton,radisson-individuals-by-choice --apply --approve-brand-explorer-v40-active-release-remediation --confirm-no-company-validation-claim --confirm-no-active-profile-approval --confirm-no-source-library-changes --confirm-no-registry-changes --confirm-external-owner-copy-clean --confirm-brand-only
```