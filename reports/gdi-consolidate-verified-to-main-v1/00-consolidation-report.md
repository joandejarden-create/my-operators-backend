# GDI consolidate verified work → main

**Status:** `READY_TO_MERGE_MAIN` (awaiting founder merge approval)  
**Integration branch:** `release/gdi-consolidate-verified-to-main-v1`  
**Proposed integration SHA:** `b84c68e0b0b72c5868515fdfc27cce724f659992` (6 commits ahead of main)

## SHAs

| Reference | SHA | Notes |
| --- | --- | --- |
| `origin/main` | `0df6e883116d3941b1937a4f645574130ab3a468` | Merge PR #146 |
| Railway current SUCCESS deploy | deployment `009f8692-58e6-4316-8758-da294127a380` | CLI upload; **no GitHub commitHash in metadata** |
| Prior Railway DTO-fix deploy | `4f9cd188-cca2-47fa-82cf-8603d218c46e` (REMOVED) | Evidence: correlated to `7603fe138` |
| Production-critical code missing from main | `7603fe138`, `2841abbd2` | EXTERNAL_SHARE approved-detail survival + timestamp exclusions |
| Portfolio parity | `7de492352` | Universal hotel/portfolio gate + admin |
| Operational workflow | `e4e173bfd` | PowerShell + Airtable sync (dry-run certified) |
| Gate polish on integration | `aca75ce286b63aeb01a5f935aa42701a27ee7063` | Exclude `publicationState` / UNCLASSIFIED sentinels from parity |
| Consolidation report | `b84c68e0b0b72c5868515fdfc27cce724f659992` | Inventory + READY_TO_MERGE_MAIN record |

## Classification (A–E)

### A — Already on main
- PR **#146** merged (`0df6e8831`) — universal presentation contract / freeze / shared rendering core
- PR **#137** merged earlier — one-click publish packaging

### B — Completed & tested, **not** on main (INCLUDED in this PR)
1. `7603fe138` — preserve `approvedCustomerFacingDetail` on EXTERNAL_SHARE detail DTO (**production hotfix stranded off main**)
2. `2841abbd2` — runtime timestamp leaf exclusions for cert stability
3. `7de492352` — portfolio full-content parity + admin indicators + CI
4. `e4e173bfd` — PowerShell workflow + Airtable ownership/sync (local dry-run certified)
5. Integration tip — parity metadata exclusion so one-click pre-pointer gate stays green

### C — Unfinished / do not merge as-is
- PR **#136** (`feat/gdi-spice-published-inventory-registry-v1`) — **CONFLICTING**, superseded by #137/#146 + later hotfixes
- Open AVA PRs (#147, #145, #142) — unfinished/parallel product tracks

### D — Superseded
- Older presentation/registry experiment branches under `fix/gdi-*`, `gdi/*`, `release/gdi-consolidate-2026-10-15`
- PR #136 content relative to current durable registry + one-click path

### E — Unrelated to GDI (leave open / out of this PR)
- Brand Explorer, homepage, Many Futures, ADP deploy historical PRs
- AVA V4 release PRs

## Deliberately excluded
- Entire historical GDI research experiment branches
- Volume backups / work inventory dumps under `reports/.../backup-volume-*` and `work-gdi-published-inventory` (local untracked)
- Token registries / share JWT material
- AVA consolidation
- Force-merge of conflicting PR #136

## Features included
- Universal presentation contract + complete approved objects
- EXTERNAL_SHARE DTO preservation (prod fix)
- Full-content parity verifier + portfolio audit
- Shared tile/drawer rendering path (from #146 + follow-ups)
- One-click publish with pre-pointer full-content gate
- Admin Full Content Parity column + verify API
- PowerShell `scripts/gdi-workflow.ps1` + Airtable sync with sales-field ownership
- Existing share auth unchanged (no token rotation in this PR)

## Test results (integration branch)

| Suite | Result |
| --- | --- |
| `test-gdi-external-share-approved-detail-survival-v1` | PASS |
| `test-gdi-full-content-parity-v1` | PASS |
| `test-gdi-portfolio-full-content-parity-v1` | PASS (8/8) |
| `test-gdi-universal-operational-workflow-v1` | PASS (12/12) |
| `test-gdi-one-click-publish-auth-v1` | PASS (after metadata exclusion fix) |
| `test-gdi-locked-published-presentation-parity-v1` | PASS |
| PowerShell `Verify` dry-run | PASS |
| ADP fingerprint inventory script | SKIP/ENV (missing local inventory fixture; not caused by this diff) |

## Railway vs main gap

**Critical:** Production received CLI deploys of the DTO/parity hotfix line (`7603fe138` / `2841abbd2`), but **`origin/main` still stops at PR #146 merge** and does **not** contain those commits. This integration PR closes that stranded-production-fix gap.

Current Railway SUCCESS (`009f8692`) is a CLI deploy without commit metadata; timeline places it after the DTO-fix deploy. Do **not** redeploy or republish from this consolidation PR without separate founder authorization.

## PRs to close after this merge
- **#148** — subsumed (portfolio parity commits included)
- **#149** — subsumed (workflow commits included)
- **#136** — superseded / conflicting — close without merge

## Merge instruction
1. Review clean diff `origin/main...release/gdi-consolidate-verified-to-main-v1` (exactly the 5 commits).
2. Founder merge to main when approved.
3. Close #136, #148, #149.
4. Future GDI work branches from updated main.
5. Railway redeploy only under separate authorization (this PR does not auto-deploy).
