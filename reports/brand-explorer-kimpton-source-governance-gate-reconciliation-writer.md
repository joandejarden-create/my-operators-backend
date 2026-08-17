# Brand Explorer Kimpton Source Governance Gate Reconciliation v30D

- Generated: 2026-07-10T12:46:41.203Z
- Brand: **Kimpton Hotels** (`kimpton`)
- v30D exists: **yes**
- Root cause: **stale_gate_used_profile_publish_eligibility_not_explorer_governance**
- Issue class: **audit_logic_inconsistency**
- Stale gate logic: **yes**
- Real governance blocker: **no**
- Airtable modified: **no**
- Company Validated untouched: **yes**

## Fact governance breakdown
- Total Explorer facts: **48**
- Approved / public: **7**
- Approved / internal only: **0**
- Rejected / internal only: **41**
- Rejected / public: **0**
- Pending review (public): **0**
- Hold / founder review (public): **0**
- Source confirmation needed: **0**

## Governance gates
- Explorer governedPlatformReady: **yes**
- Legacy gate (profile publish): **no**
- Profile publish blockers: would_downgrade_existing_validation
- Live validation status: **Company Published**

## Complete Build after fix
- governedPlatformReady: **yes**
- readyForActiveProfile: **yes**
- readinessBand: **ready**

## Code repairs
- assessBrandExplorerGovernanceReadiness() — explorer-scoped gate (pending public, approved public source-backed)
- computeGovernedPlatformReady() in complete-build orchestrator uses explorer gate, not profile publish eligibility
- Rejected/internal facts excluded from governedPlatformReady blockers

## Apply command
`npm run brand-explorer-kimpton-source-governance-gate-reconciliation-writer -- --brand kimpton --apply --approve-brand-explorer-v30D-kimpton-source-governance-gate-reconciliation --confirm-no-company-validation-claim`