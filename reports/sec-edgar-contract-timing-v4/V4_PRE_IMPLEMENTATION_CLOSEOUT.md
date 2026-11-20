# V4 Pre-Implementation Closeout

**Date:** 2026-11-20  
**Founder architecture approval:** Confirmed  
**PR #495:** OPEN / MERGEABLE — **integration gate unchanged**  
**Schema Meta apply:** Pending  
**HPC creates:** None  

## Preserved artifacts

| Artifact | Path |
|---|---|
| Geographic + schema architecture | `docs/ai-build-system/HOTEL_CONTRACT_TIMING_V4_GEOGRAPHIC_ARCHITECTURE.md` |
| HPC creation workflow | `docs/ai-build-system/HOTEL_CONTRACT_TIMING_V4_HPC_CREATION_WORKFLOW.md` |
| Schema diff (approved, not applied) | `reports/sec-edgar-contract-timing-v4/SCHEMA_DIFF_APPROVED_PENDING_APPLY.json` |
| Pilot manifest template | `reports/sec-edgar-contract-timing-v4/HPC_CREATION_PILOT_MANIFEST_TEMPLATE.json` |
| US selection module | `lib/hotel-intelligence/sec-edgar/contract-timing-v4-us-pilot-selection-v1.js` |
| Selection dry-run script | `scripts/sec-edgar-contract-timing-v4-us-pilot-select.mjs` |
| Selection criteria + dry-run report | `reports/sec-edgar-contract-timing-v4/US_PILOT_SELECTION_CRITERIA.md` |

## Standing prohibitions (until authorized V4 PR)

1. Do not Meta-apply HI Hotel Contracts / HI Contract Milestones  
2. Do not mutate HPC schema or create hotels  
3. Do not extend PR #495 with V4 work  
4. Preserve dirty Hotel Explorer worktree  

## US pilot selection (refined)

Deterministic scoring prioritizes identity, address/geo, contract milestones, aliases, owner/manager evidence, and HE enrichment potential.

Hard rejects include non-hotel asset classes, generic names, and **street-address-as-name** false positives.

Dispositions: `LINK_EXISTING` · `CREATE_READY` · `RESEARCH_REQUIRED` · `REJECT_AMBIGUOUS`  

`CREATE_READY` requires live HPC verified-absent collision **and** strong evidence. **No padding** to hit 25–50.

Latest dry-run (481 findings → 305 US hotels; `--hpc-check` on top collision queue): see `US_PILOT_SELECTION_DRY_RUN.json`. Shortlist capped at 50 non-rejected. `CREATE_READY` rows still require **per-row founder manifest approval** before any create; V4 PR will deepen alias/duplicate reconciliation (current live check returned 0 `LINK_EXISTING` in the first pass — treat as provisional).

## Factory-ready architecture (founder final design acceptance)

Two-mode permanent design prepared (not scheduled / not unattended HPC).  
**Dry-run/scaffold ≠ live validation.**

| Doc / code | Path |
|---|---|
| Factory architecture | `docs/ai-build-system/HOTEL_INTELLIGENCE_FACTORY_V4.md` |
| PowerShell command contract | `docs/ai-build-system/HOTEL_INTELLIGENCE_FACTORY_POWERSHELL_CONTRACT_V4.md` |
| Design acceptance record | `reports/sec-edgar-contract-timing-v4/V4_FACTORY_DESIGN_ACCEPTANCE.md` |
| Engine + modes/stages/checkpoint/cost/changes | `lib/hotel-intelligence/hotel-intelligence-factory/` |
| Promotion auth binding | `…/promotion-authorization-v1.js` |
| Durable paths | `…/durable-paths-v1.js` |
| Research Mode tests | `scripts/test-hotel-intelligence-factory-research-mode-v4.mjs` |
| Promotion Mode tests | `scripts/test-hotel-intelligence-factory-promotion-mode-v4.mjs` |
| Node CLI / PS orchestrator | `scripts/hotel-intelligence-factory-run.mjs` / `.ps1` |
| Default config | `configs/hotel-intelligence-factory/default.v4.json` |

**MODE 1 RESEARCH** — global discover/verify/persist research tables; no HPC creates.  
**MODE 2 CANONICAL PROMOTION** — authorization bound to manifest hash + actions + base + identities + batch + runId + expiry.

## First V4 delivery after #495 merge + sync

New sequential PR from synchronized `main`:

1. Final live schema preflight + approved exact diff  
2. Controlled create of Contracts + Milestones tables (separate apply gate)  
3. Migration dry-run for historical findings → contracts/milestones  
4. Fully researched 25–50 US HPC creation manifest  
5. Verified HPC duplicate/alias reconciliation  
6. Explicit founder approval gate per proposed CREATE  
7. Hotel Explorer enrichment queue integration  
8. Wire full DISCOVER→PERSIST / PROMOTE stage bodies into factory engine  
9. CI, read-back, CUSTOMER_BLOCKED safeguards  

No unattended HPC creation, production scheduler, or customer publication in V4.

## Status

**`AWAITING_INTEGRATION_CONTROLLER`**
