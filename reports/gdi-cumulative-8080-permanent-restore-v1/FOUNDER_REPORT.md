# GDI Cumulative Environment Permanent Restore — Founder Report

**Date:** 2026-11-27  
**Status:** `GDI_CUMULATIVE_ENVIRONMENT_PERMANENTLY_RESTORED — AWAITING_FOUNDER_REVIEW`  
**Contract:** `DEALALITY_ADDITIVE_NON_REGRESSION_V1` + `DEALALITY_CANONICAL_PORT_8080_OWNERSHIP_V1`  
**Lifecycle:** FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT (no CURRENT publish)

## Exact root cause

Repeated founder regressions (missing Executive Summary, three tabs, Wyndham 0/0) were **not** browser cache and **not** loss of the Wyndham package on the GDI home.

1. **Port 8080 had no real workspace ownership.**  
   `DEALALITY_CANONICAL_CHECKOUT=1` and/or `DEALALITY_ALLOW_WORKTREE_SERVER=1` alone could bind `:8080` from **any** checkout basename.
2. **Foreign worktrees repeatedly stole `:8080`.**  
   Live incident during this remediation: after a stop/restart race, PID on `:8080` was `C:\Dev\dealality\_verify_pr627_main` on HE branch `feat/he50-release-reconciliation-v1` (basename `_verify_pr627_main`). That tree serves pre-ES HTML (3 tabs) and lacks the current Wyndham STAGED tip → founder sees ES missing + 0/0.
3. **Prior documented thief:** `wt-he-unified-theme-v1` (BUILD_DECISIONS).
4. **Launcher false success:** previous launcher wrote a PID and reported OK even when the child logged `FATAL: port 8080 is already in use`, leaving the founder on the foreign process.
5. **STAGED inventory is checkout-local unless pinned.**  
   Integration’s bundled Wyndham STAGED tip was still `…_3283ee92` (CLEAN11-era). The valid tip `…_84288a6e` (26 IDs) lives under the registered GDI home bundled store. Serving from the wrong tree resolves the wrong/missing pointer.

## Responsible process / workspace

| Field | Value |
|---|---|
| Unauthorized process observed | `node server.js` PID 9512 |
| Unauthorized cwd | `C:\Dev\dealality\_verify_pr627_main` |
| Unauthorized branch | `feat/he50-release-reconciliation-v1` |
| Mechanism | Env-bypass `:8080` + race after stop |
| Canonical restore cwd | `C:\Dev\dealality\ws-gdi` |
| Canonical branch | `feat/gdi-cumulative-es-list-staging-20261127` (PR #626) |

## Permanent runtime ownership enforcement

- Module: `lib/dealality-dev-os/canonical-port-8080-ownership-v1.js`
- Wired into `server.js` boot (fail-closed).
- **Env flags alone cannot authorize `:8080`.**
- Authorized exact paths only:
  - Registered GDI home `C:/Dev/dealality/ws-gdi`
  - Integration home with explicit `DEALALITY_8080_OWNER=integration`
- Feature ports: GDI **8095**, HE **8091**, ADP **8092**, BE **8093**
- Launcher `npm run dealality:gdi-local-8080`:
  - Reclaims any `:8080` listener
  - Pins `GDI_PUBLICATION_PACKAGE_ROOT` to GDI durable bundled store
  - **Success only after listen + `/api/dev/runtime-identity` proves `ws-gdi` + ownership + Wyndham STAGED**

## Durable STAGED package verification

| Item | Value |
|---|---|
| Durable root | `C:\Dev\dealality\ws-gdi\lib\group-demand-intelligence\bundled-publication-packages` |
| Tip STAGED | `gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_84288a6e` |
| Prior commercial cert | `…_c456c74e` (superseded by competitive recovery tip) |
| CLEAN11 preserved | `…_3283ee92` (not promoted) |
| Opportunity IDs | **26/26** locked IDs reconciled (0 missing / 0 extra) |
| CURRENT | **null** (customer isolation preserved) |

## Outstanding PR consolidation assessment

| PR | State | Assessment |
|---|---|---|
| **#626** | OPEN | **Canonical consolidation vehicle.** Contains ES + List + Admin dashboards + competitive scope + this ownership enforcement. Merge via Integration Controller. |
| **#618** | OPEN | **Superseded** by #626 (ES Phase 1 absorbed / refined). Close after #626 merge; do not merge in parallel. |
| **#624** | OPEN | **Superseded** by #626 (sortable List View absorbed). Close after #626 merge. |
| **#620** | OPEN | **Not superseded.** Checkpoint B paid LDI/competitive continuation. Keep open; do **not** merge for this restore. Remaining $13 auth is separate. |

Overlaps: #618/#624 files collide with #626 cumulative branch — #626 is the integration path.  
Missing on `origin/main`: Executive Summary, four-tab registry, List View cumulative, Admin GDI dashboards, ownership enforcement, tip STAGED `84288a6e`.

**GDI chat does not merge.** Integration Controller must merge #626 after CI, then post-merge sync `ws/gdi`.

## Canonical integrated commit (local cumulative)

- Workspace: `C:\Dev\dealality\ws-gdi`
- Branch: `feat/gdi-cumulative-es-list-staging-20261127`
- Pre-ownership HEAD: `98e0272cb` (plus this ownership commit once pushed)
- Served identity after restore: basename `ws-gdi`, ownership `AUTHORIZED_GDI_HOME`, STAGED `…_84288a6e`, count **26**

## Repeatability evidence

1. **Canonical restart** — launcher reclaimed PID, re-verified identity; ES present; Wyndham 26 retained.  
2. **Competing feature worktree** — `ws-integrated-staging` on **8099** with allow-worktree: started; `:8080` remained `ws-gdi` / 26 / `84288a6e`.  
3. **Foreign `:8080` with env bypass** — denied (`PORT_8080_UNAUTHORIZED_WORKSPACE`; `DEALALITY_CANONICAL_CHECKOUT=1` ignored).  
4. **Validator** — `npm run dealality:validate-gdi-cumulative-8080` → `READY_FOR_FOUNDER_REVIEW`.

## Exact repeatable commands

```powershell
cd C:\Dev\dealality\ws-gdi
npm run dealality:git-preflight -- --op status-check --stream gdi
npm run test:canonical-port-8080-ownership-v1
npm run dealality:gdi-local-8080
npm run dealality:validate-gdi-cumulative-8080
# Feature work (never 8080):
#   cd <feature-tree>; $env:PORT=8095; $env:DEALALITY_ALLOW_WORKTREE_SERVER=1; node server.js
```

## Screenshots

- `reports/gdi-cumulative-8080-permanent-restore-v1/after-executive-summary.png` — four tabs + ES + Qualified Opportunities **26** + STAGED badge.
- Browser a11y: Executive Summary selected; Research Receipt / Productivity / Priority tiles markers present; Opportunities All **(26)**; Competitive **(3)**.

## Acceptance criteria (Integration Controller)

Reject completion claims unless:

1. `:8080` runtime-identity shows authorized GDI (or Integration+owner flag) workspace.  
2. Served HTML includes `executive-summary-tab.js` and four-tab registry.  
3. Wyndham STAGED count = 26 on tip ≥ `84288a6e` (not CLEAN11, not empty).  
4. CURRENT remains unpublished for Wyndham.  
5. `npm run dealality:validate-gdi-cumulative-8080` PASS.  
6. Ownership unit gate PASS (`test:canonical-port-8080-ownership-v1`).  
7. No foreign worktree can bind `:8080` via env flags alone.

No paid research. No CURRENT. No production deploy. Dirty unrelated worktrees preserved.
