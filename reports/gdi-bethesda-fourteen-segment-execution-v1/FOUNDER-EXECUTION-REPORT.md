# Bethesda Marriott — Existing GDI 14-Segment Full E2E Execution

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Baseline main before this work:** `ed87dd599`  
**Date:** 2026-11-12  
**Paid research authorized:** **NO**  
**Customer publication:** **NO**  
**USD spent this run:** **$0.00**

---

## A. Existing system (found and used — not rebuilt)

### Canonical execution map

```
scripts/gdi-workflow.ps1  (PowerShell orchestrator)
  → scripts/gdi-universal-operational-workflow-v1.mjs

scripts/gdi-full-e2e-fourteen-segment-orchestrator-v1.mjs   ← USED
  → full-e2e-fourteen-segment-traversal-v1.js               (14 cells)
  → full-e2e-fourteen-segment-discovery-execution-v1.js     (executors)
  → segment-discovery-executor-v1.js                        (queries + provider)
  → original-full-e2e-research-chain-v1.js                  ← USED (--run-original-full-e2e-chain)
       → discovery → ORS deep research → contacts → commercial spine → ten-section presentation

scripts/gdi-research-to-page-workflow-v1.mjs --mode full-e2e  ← USED (dry-run on existing store)
  → segment research plan → reuse → eligibility → cards → completion evaluator
```

### Fourteen canonical workflow segments (executed)

1. `associations_conferences_events`  
2. `corporate_accounts_scaling`  
3. `corporate_meetings_incentives`  
4. `government_public_sector`  
5. `sports_entertainment`  
6. `education_scientific`  
7. `projects_infrastructure`  
8. `social_special_events`  
9. `local_catering_day_meetings`  
10. `corporate_celebrations_private_events`  
11. `travel_trade_group_intermediaries`  
12. `relocation_extended_stay`  
13. `preferred_lodging_partnerships`  
14. `professional_event_planners`  

Source: `listFourteenWorkflowSegmentIds()` in `universal-contact-research-contract-v1.js`.

### Commands actually run

```bash
# 1) Accounting / connectivity
node scripts/gdi-full-e2e-fourteen-segment-orchestrator-v1.mjs \
  --hotels recLuxvwwxID7U2B8 --dry-run

# 2) All 14 discovery executors (zero-paid; no Exa)
node scripts/gdi-full-e2e-fourteen-segment-orchestrator-v1.mjs \
  --hotels recLuxvwwxID7U2B8 --execute-discovery --dry-run \
  --out reports/gdi-bethesda-fourteen-segment-execution-v1

# 3) Original Full E2E chain (fixture/zero-cost connectivity)
node scripts/gdi-full-e2e-fourteen-segment-orchestrator-v1.mjs \
  --hotels recLuxvwwxID7U2B8 --run-original-full-e2e-chain --dry-run \
  --out reports/gdi-bethesda-fourteen-segment-execution-v1/original-chain

# 4) Existing inventory Full E2E dry-run (59 store rows)
node scripts/gdi-research-to-page-workflow-v1.mjs \
  --hotel recLuxvwwxID7U2B8 --mode full-e2e --dry-run
```

### Broken connections found and fixed

| Defect | Fix |
|--------|-----|
| `RESEARCH_SEGMENT` circular TDZ blocked orchestrator import | Leaf module `workflow/research-segment-ids-v1.js`; research-constants + event-to-demand import the leaf |
| `bethesda-maturation-checkpoint-v1` TDZ on `RESEARCH_TERMINAL` | Lazy `terminalOutcomes()` |
| Discovery ledger roots only pointed at off-repo Railway paths | Added repo-local `data/group-demand-intelligence/discovery-ledgers` first |
| Fourteen-segment orchestrator missing from `package.json` | Added `gdi:full-e2e-fourteen-segment*` scripts |

---

## B. Discovery results (honest)

| Metric | Result |
|--------|--------|
| Segments attempted | **14 / 14** |
| Executor invoked | **14 / 14** |
| Queries built | Yes (e.g. associations cell built **10** live query specs for DMV) |
| Fresh provider retrievals | **0** |
| Historical ledger replay hits | **0** (no Bethesda `four-hotel-14-workflow-full-e2e-v1.json` in ledger roots) |
| Live paid Exa calls | **0** (not authorized) |
| Cell status | **14 × EXECUTION_BLOCKED** — `PAID_PROVIDER_REQUIRES_FOUNDER_AUTHORIZATION` |
| Raw extracted commercial candidates (live) | **0** |
| Fixture connectivity candidates (chain only) | 40 extracted → **0 retained** after filters (not publication inventory) |
| Genuinely NEW unique Bethesda opportunities | **0** |

**Do not interpret blocked paid discovery as “zero demand found.”**  
Every segment produced real search queries; provider execution was gated.

Durable per-segment state written under:  
`data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/full-e2e-fourteen-segment-state/`

---

## C. Qualification / content (this zero-paid run)

| Metric | Result |
|--------|--------|
| New Bethesda-relevant opportunities deep-researched | **0** |
| New verified contacts | **0** |
| New Sales Ready / Account Development from discovery | **0** |
| Existing store Full E2E dry-run | Evaluated **59** canonical rows; overall status **FAILED** on segment coverage / maturation completeness (expected without paid multi-segment discovery) |
| Existing commercial list | Still **67** (post-#198 acceptance) — unchanged |
| Underlying IDs | **88** preserved |

---

## D. Paid research authorization request (ONE package)

Existing chain budget manifest (Bethesda-only, 14 segments):

| Item | Value |
|------|-------|
| Discovery | 14 cells × 2 medium Exa runs ≈ **$2.80** |
| Deep research | ~25 medium runs ≈ **$2.50** |
| Contact | Public/ORS only — **$0** Surfe/PDL |
| Contingency | **$0.79** |
| **Hard ceiling** | **`$6.09`** |

**Authorization phrase required to proceed (when approved):**  
`--authorize-paid-discovery --authorize-paid-research --budget-ceiling-usd 6.09`  
plus a new founder authorization id (prior Aimbridge ledger is **not** reusable).

Excludes: production Airtable writes, customer publication, Surfe, share registry changes.

Suggested command after approval:

```bash
node scripts/gdi-full-e2e-fourteen-segment-orchestrator-v1.mjs \
  --hotels recLuxvwwxID7U2B8 \
  --run-original-full-e2e-chain \
  --authorize-paid-discovery \
  --authorize-paid-research \
  --budget-ceiling-usd 6.09 \
  --out reports/gdi-bethesda-fourteen-segment-execution-v1/paid-live
```

---

## E. Safety

- Paid spend this session: **$0**  
- No customer publication  
- No maturity auto-promotion  
- No overwrite of canonical 59 / historical 29 commercial bodies from discovery  
- 88 IDs intact  
- Fixture chain explicitly **not** claimed as Full E2E discovery complete  

---

## F. Artifacts

- `reports/gdi-bethesda-fourteen-segment-execution-v1/orchestrator-report.json`  
- `reports/gdi-bethesda-fourteen-segment-execution-v1/discovery-execution-traces.json`  
- `reports/gdi-bethesda-fourteen-segment-execution-v1/original-chain/original-full-e2e-chain/chain-execution-trace.json`  
- `reports/gdi-bethesda-fourteen-segment-execution-v1/original-chain/original-full-e2e-chain/budget-manifest.json`  

---

## Bottom line for the founder

The **existing** PowerShell/Node fourteen-segment Full E2E system was located, unblocked, and **actually executed** for all 14 Bethesda segments.

Without a new paid Exa authorization, discovery correctly **stops** at `PAID_PROVIDER_REQUIRES_FOUNDER_AUTHORIZATION` after building queries — it does **not** invent “empty market” results.

**Next decisive step:** approve the **$6.09** hard-capped Bethesda 14-segment live chain above so Stage A live retrieval + ORS deep research + contact/qualification can complete on real candidates.
