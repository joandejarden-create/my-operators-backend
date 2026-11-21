# Hotel Intelligence Factory V4 — Two-Mode Operating Architecture

**Status:** FOUNDER FINAL DESIGN ACCEPTANCE (2026-11-20)  
**Gate:** PR #495 merge + `main` sync before V4 implementation PR  
**Do not extend PR #495**  
**Not in V4:** unattended HPC creation, production scheduler, customer publication  

## Design acceptance principle

A successful **dry-run** or completed **factory scaffold** is **never** evidence that live research persistence or canonical promotion has been validated. Live validation requires authorized table apply, Research Mode end-to-end persistence with read-back, and separately authorized Promotion manifests.

## Permanent requirement

All Hotel Contract Timing Intelligence and Discovery-to-HPC work must be **factory-ready**: reusable, resumable, auditable, cost-controlled, and orchestrable from PowerShell without rewriting research logic.

PowerShell = thin orchestration. Node.js modules = shared execution engine for interactive, scheduled, and future admin-UI invocation (**one pipeline** — no duplicate provider call paths).

## MODE 1 — RESEARCH

**Purpose:** Continuously discover, verify, deduplicate, and persist **global** hotel intelligence.

| Permitted | Forbidden |
|---|---|
| Worldwide research (SEC-API.io, Exa, free EDGAR, approved international sources) | Create HPC hotels |
| Extract identities, orgs, contracts, milestones | Modify canonical HPC attributes |
| Verify / classify evidence | Promote to customer-facing evidence |
| Persist to approved research tables (`CUSTOMER_BLOCKED`) | Publish customer intelligence |
| Resume incomplete runs | Exceed approved budgets |
| Track NEW / UPDATED / UNCHANGED / SUPERSEDED / CONFLICTED | |
| Generate HPC match candidates + creation manifests (dry-run) | |

`researchCoverage: GLOBAL` always. Americas/Europe HPC enrichment priority is **configuration only** (`geographicPriority` / `hpcEnrichmentRegions`).

## MODE 2 — CANONICAL PROMOTION

**Purpose:** Convert verified research into canonical HPC-linked intelligence.

### Authorization binding (fail closed on any mismatch)

Promotion apply must bind **all** of:

1. Exact **manifest SHA-256** (file + `HIF_FOUNDER_MANIFEST_SHA256` env)  
2. **allowedActions** ⊆ factory allowlist  
3. **canonicalBaseId** = `appCCUsuGsE1ifoLk`  
4. **recordIdentities** — every candidate id in the apply batch  
5. **maxBatchSize** — batch length ≤ authorized max  
6. **runId** — must match the live factory run  
7. **expiresAt** — authorization not expired  

Plus: `HIF_APPLY_PROMOTION=1`. CREATE rows require per-row `founderApproval.approvedForCreate=true`. Module: `promotion-authorization-v1.js`.

| Permitted (when authorized) | Required protections |
|---|---|
| Link discoveries → existing HPC | Exact approved creation/update manifests |
| Create verified missing HPC hotels in approved cohorts | Collision + alias checks |
| Non-destructive evidence-backed HPC enrichment | Identity + evidence gates |
| Link approved contracts, milestones, orgs | Pre-apply validation + read-back |
| Queue new hotels for Hotel Explorer enrichment | Idempotent execution; auto-stop on unexpected writes |

Publication authorization is **independent** of both modes (default blocked).

## Durable runtime (worktree-independent)

Checkpoints, caches, cost ledgers, and durable run results live under:

`HIF_DURABLE_ROOT` (recommended) → default `C:\Dev\dealality\hif-durable`

```
{hif-durable}/
  runs/{runId}/checkpoint.json, cost-ledger.json, logs/
  cache/
  ledgers/cumulative-cost.json
  reports/{runId}/
  authorization/
```

Repo `reports/hotel-intelligence-factory/{runId}/` is an optional **mirror for PRs**, not the resume source of truth. Temporary Git worktrees must not own factory state.

## Pipeline stages (shared engine)

```
CONFIG → DISCOVER → EXTRACT → VERIFY → RECONCILE → PERSIST_RESEARCH
                                              ↓
                         (MODE 2 only, authorized)
                         MANIFEST → PROMOTE → READBACK → QUEUE_HE
```

Change vocabulary: `NEW` · `UPDATED` · `UNCHANGED` · `SUPERSEDED` · `CONFLICTED`  
Historical versions preserved.

## Module map

| Concern | Module |
|---|---|
| Engine | `hotel-intelligence-factory/engine-v1.js` |
| Modes | `modes-v1.js` |
| Promotion auth binding | `promotion-authorization-v1.js` |
| Durable paths | `durable-paths-v1.js` |
| Checkpoint / resume | `checkpoint-v1.js` |
| Cost ledger | `cost-ledger-v1.js` |
| Change detection | `change-detection-v1.js` |
| Config | `config-v1.js` |
| Stages | `stages-v1.js` (includes `ENRICH_CONTRACT_TERMS`; opt-in `DISCOVER_EXPAND`) |
| Contract terms / renewals | `sec-edgar/contract-terms-*-v1.js` |
| SEC discovery expansion | `sec-edgar/sec-discovery-expansion-design-v1.js`, `sec-discovery-coverage-ledger-v3.js` |
| CLI / PS | `scripts/hotel-intelligence-factory-run.mjs`, `.ps1` |
| Terms & renewals pilot | `scripts/sec-edgar-contract-terms-renewals-pilot.mjs` |
| Discovery universe audit | `scripts/sec-edgar-discovery-universe-audit.mjs` |

## Test suites (separate)

| Suite | Script |
|---|---|
| Research Mode | `scripts/test-hotel-intelligence-factory-research-mode-v4.mjs` |
| Canonical Promotion Mode | `scripts/test-hotel-intelligence-factory-promotion-mode-v4.mjs` |
| Contract terms / renewals | `scripts/test-sec-edgar-contract-terms-renewals-v4.mjs` |

Must cover: negative permissions, rerun idempotency, resume after interruption, authorization mismatch/expiry, and (in live V4) Airtable read-back failure fail-closed.

## First V4 acceptance milestones (after #495)

1. Approved Contracts/Milestones schema created + verified (Meta apply)  
2. Existing contract research migrated with dedupe + read-back  
3. **One complete Research Mode run** demonstrated (live persist — not scaffold)  
4. 25–50 US hotels in a real HPC creation **dry-run** manifest  
5. Canonical Promotion authorization gates tested  
6. Newly created hotels remain on separate founder approval  
7. Factory status, costs, checkpoints, change reports consistent  

## Recurring execution (future)

Same Node engine + PowerShell contract; provider fingerprints in checkpoint prevent duplicate paid retrieval. **No second research pipeline.**

## Related docs

- `HOTEL_CONTRACT_TIMING_V4_GEOGRAPHIC_ARCHITECTURE.md`  
- `HOTEL_CONTRACT_TIMING_V4_HPC_CREATION_WORKFLOW.md`  
- `HOTEL_INTELLIGENCE_FACTORY_POWERSHELL_CONTRACT_V4.md`  
