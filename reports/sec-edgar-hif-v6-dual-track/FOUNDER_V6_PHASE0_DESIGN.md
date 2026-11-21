# HIF V6 — Phase 0 Status + Dual-Track Design (pre-merge)

**As of:** 2026-11-21  
**PR #503:** OPEN · MERGEABLE · tip `acf04f4c…` · **not on `origin/main`**  
**Merge SHA:** *(none — not merged)*  
**V6 research waves:** **NOT AUTHORIZED**

## Phase 0 verdict

| Check | Result |
|---|---|
| PR #503 current status | OPEN |
| Latest approved implementation HEAD | `acf04f4c1e2ffb2804160743e9c12bea2efcf22a` |
| Integration Controller main sync of V5 | **PENDING** |
| Required CI on tip | Re-running / in progress after industrial-scale push |
| V5 merged | **NO** |
| Post-merge tests | N/A until merge |
| Dirty HE / ADP / GDI / BE worktrees | **PRESERVED** (no reset/clean/stash-discard) |

**Final status: `AWAITING_INTEGRATION_CONTROLLER`**

Per founder directive: while V5 is unmerged, only source inventories and implementation design are allowed. No competing implementation PR. No Track A/B research execution.

## What landed (design only)

1. **Shared research contract** — `lib/hotel-intelligence/sec-edgar/hif-v6-shared-research-contract.js`  
   Dual tracks, shared identities, commercial signal classes, court evidence classes, ownership roles, routing/governance, execution gate.
2. **Track A inventory** — 14 source families (SEC/CMBS/EX-10 critical; recorded MA memoranda; court/insolvency; portfolios).
3. **Track B inventory** — 25 portals/registries; initial wave candidates: CVM, EDINET, Companies House, SGX, HKEX, SEDAR+.
4. **Gate test** — `npm run test:hif-v6-dual-track-contracts` (waves denied until merge).

## Parallel-track plan (post-merge)

| Item | Decision |
|---|---|
| Workspace | Registered HE only: `C:/Dev/dealality/ws-hotel-explorer` |
| Separate unregistered worktrees | **PROHIBITED** |
| New registered worktrees | Integration Controller only (registry edit) |
| Branches after merge | `feat/hif-v6-track-a-*` · `feat/hif-v6-track-b-*` |
| Shared core | One HIF / HE identity + dual prospect engines — no duplication |
| Merges | Integration Controller · sequential |

## Commercial products (unchanged)

- `BRAND_CONVERSION_TARGETS`
- `MANAGEMENT_CONTRACT_TARGETS`
- LEASE incidental only — no third list

## Routing / spend

- Deterministic → free official / Exa Search → independent verification  
- Exa Agent: `MANUAL_ESCALATION_ONLY`  
- Jev: `SHADOW_ONLY`  
- No silent budget exceedance; use founder budget request template for paid expansion

## Next after IC merge of #503

1. Post-merge HE sync to `ws/hotel-explorer` + FF `origin/main`
2. Freeze V6 baseline from merged tip
3. Open Track A and Track B feature branches (or IC-registered homes if provisioned)
4. Execute authorized waves with checkpoints, cost ledgers, hotel-level results
5. Separate PRs · sequential IC merge

Artifacts: `EXECUTION_GATE.json` · `TRACK_A_SOURCE_INVENTORY.json` · `TRACK_B_SOURCE_INVENTORY.json` · `PHASE0_V5_INTEGRATION_STATUS.json`
