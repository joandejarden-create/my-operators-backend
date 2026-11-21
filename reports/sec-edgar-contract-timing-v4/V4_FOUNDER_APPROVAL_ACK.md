# V4 Founder Approval Acknowledgement

**Date:** 2026-11-20  
**PR #495:** OPEN / MERGEABLE — **must merge before V4 implementation**  
**Schema Meta apply:** NOT executed  
**HPC creates:** NOT executed  

## Approved

- Global research + Americas/Europe HPC priority (independent scopes)
- Tables: HI Hotel Contracts, HI Contract Milestones on `appCCUsuGsE1ifoLk`
- Additive Inventory/Findings geography + link fields
- Milestone **Publication State** (default CUSTOMER_BLOCKED)
- `VERIFIED_DISCOVERY_TO_HPC_CREATION` (controlled; not bulk auto-create)
- 25–50 US dry-run pilot manifest before any HPC create apply

## Prepared (local design artifacts)

- `docs/ai-build-system/HOTEL_CONTRACT_TIMING_V4_GEOGRAPHIC_ARCHITECTURE.md`
- `docs/ai-build-system/HOTEL_CONTRACT_TIMING_V4_HPC_CREATION_WORKFLOW.md`
- `reports/sec-edgar-contract-timing-v4/SCHEMA_DIFF_APPROVED_PENDING_APPLY.json`
- `reports/sec-edgar-contract-timing-v4/HPC_CREATION_PILOT_MANIFEST_TEMPLATE.json`

## Blocked until IC

1. Merge PR #495 + post-merge sync  
2. Open sequential V4 PR from synchronized main  
3. Dry-run ensure-script for approved schema  
4. Explicit Meta apply authorization  
5. Build US 25–50 creation manifest → founder approve → apply + read-back  

Dirty concurrent HE worktree preserved. No extension of #495.
