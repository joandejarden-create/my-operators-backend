# V4 Factory — Final Design Acceptance Record

**Date:** 2026-11-20  
**Founder status:** ACCEPTED with additional binding requirements  
**PR #495:** Still OPEN — prerequisite for V4 implementation PR  
**Live validation claim:** **NONE** (scaffold/dry-run ≠ live persistence or promotion)

## Accepted

- Two-mode architecture (RESEARCH / CANONICAL_PROMOTION)  
- Shared Node engine + thin PowerShell orchestrator  
- Checkpointing, cost ledger, change-detection vocabulary  
- Global research + Americas/Europe HPC priority via config  
- Durable runtime independent of Git worktrees (`HIF_DURABLE_ROOT`)  
- Promotion authorization bound to: manifest hash, allowedActions, canonical base, record identities, max batch, runId, expiry  
- Separate Research vs Promotion test suites  
- Future recurring execution = same engine (no second pipeline)  

## Explicit non-claims

- Schema Meta apply not executed  
- No HPC hotels created  
- No complete live Research Mode persistence run yet (milestone after tables exist)  
- Dry-run / scaffold success ≠ live validation  

## First V4 acceptance milestones (post-#495)

1. Contracts/Milestones schema created + verified  
2. Historical findings migrated + read-back  
3. One complete Research Mode run  
4. Real 25–50 US HPC creation dry-run manifest  
5. Promotion auth gates tested  
6. Creates remain separately founder-approved  
7. Consistent status/cost/checkpoint/change reports  

## Status

**`AWAITING_INTEGRATION_CONTROLLER`**
