# GDI 3.0 — Slice 1 Amendment: Research Prioritization, Depth & Durable Intelligence Prep

**Status:** Founder review gate (offline evaluation + telemetry only)  
**Date:** 2026-11-19  
**PR:** #440 (Slice 1 implementation branch)  
**Explicit non-goals:** No new production selection weights · no Slice 2 persistence apply · no paid calls · no adaptive AI-agent resume

---

## A. Candidate prioritization — audit findings

### Current path (canonical Full E2E)

1. Fourteen-segment discovery matrix (taxonomy order).  
2. `collectRetainedCandidatesFromDiscovery` (cell order; fair scheduler **off** by default).  
3. `screenAllCandidatesPreDeep` (filters duplicates / out-of-market / already-paid; **no spend**).  
4. `selectCandidatesForDeepResearch` — fair floor (`fairMinPerSegment` default **1**) then commercial score fill.  
5. Process selected opportunities in selection order (budget can still block later rows).

### Critical structural issue

With defaults **`maxCandidates=8`** and **`fairMinPerSegment=1`** across **14** planned segments:

- The fair pass walks taxonomy order and consumes **all 8 slots**.  
- The commercial score fill **never runs**.  
- Segments 9–14 (`local_catering_day_meetings` … `professional_event_planners`) are systematically deferred even if higher-scoring.  
- Provider result order only breaks score ties; it does **not** drive paid selection under the commercial register — but taxonomy fair-floor order effectively does.

Existing scores **are** computed (`combinedScore` from dimension weights + legacy investment) but are subordinated to the fair-floor + cap interaction.

Hotel relevance / market signals enter via pre-deep screen + score dimensions **before** paid deep, when screening is enabled. Shared intelligence reuse ledger can add inventory org keys before discovery (Airtable reuse off by default).

### Offline comparison (frozen inventories)

Run: `node scripts/run-gdi-research-priority-offline-comparison-v1.mjs`  
Artifacts: `reports/gdi-research-priority-offline-comparison-v1/`

Policies compared (evaluation only):

| Policy | Meaning |
| --- | --- |
| FIRST_IN_LIST | First N inventory rows |
| CURRENT_FAIR_FLOOR_THEN_SCORE | Production register defaults |
| PROPOSED_PURE_SCORE_EVAL | Offline proposed research-priority rubric |

**Proposed evaluation rubric (NOT production):** demand credibility · hotel/market relevance · future timing/sourcing · commercial accessibility · potential economic relevance · novelty / expected value of additional research.

Rule: do **not** penalize emerging demand solely for missing named buyer or confirmed room nights.

### Founder recommendation (selection policy)

**Do not enable proposed weights in production yet.**

Safe options after review (choose explicitly):

1. Raise default deep cap to `plannedSegmentCount` (14) when budget allows, **or**  
2. Reserve commercial-fill slots after a bounded fair floor (e.g. fair floor uses `min(fairMin×segments, floor(cap×0.5))`), **or**  
3. Keep current policy but treat `fairFloorCanConsumeEntireCap` as a hard honesty flag (already surfaced in telemetry).

---

## B. Progressive research depth contract

Module: `lib/group-demand-intelligence/research/gdi-progressive-research-depth-contract-v1.js`

| Stage | Purpose |
| --- | --- |
| STAGE 0 | Discovery + inexpensive screening |
| STAGE 1 | Targeted demand evidence |
| STAGE 2 | Buyer / commercial / competitive qualification |
| STAGE 3 | Selective deeper investigation for high-value unresolved questions |

Every continuation packet requires: unresolved question · evidence already collected · expected commercial benefit · proposed next action · estimated cost / remaining budget · stopping condition.

Stop classes: `ACTIONABLE` · `UNSUITABLE` · `TOO_EARLY_MONITOR` · `ALREADY_SUFFICIENTLY_KNOWN` · `PERMITTED_RESEARCH_EXHAUSTED` · `BUDGET_OR_AUTH_BLOCKED`.

**Slice 1 production change:** attach stop classification + continuation packet shape to processed rows (telemetry). **Does not** replace ORS supervisor math or force additional Exa loops.

Existing ORS stop behavior remains blocker-/iteration-driven; Stage 3 expected-value continuation is **recommended**, not enabled as a new spend loop.

---

## C. Slice 2 persistence dependencies (inspect only in Slice 1)

Bridge: `gdi-full-e2e-research-persistence-bridge-v1.js`  
Store: `gdi-hotel-research-inventory-store-v1.js`

### Must carry forward from Full E2E → durable inventory

1. Stable organization identity (`organizationKey` / shared demand key)  
2. Stable market-opportunity identity (`programKey` / opportunity id)  
3. Segment + demand-mechanism classification  
4. Evidence refs with provenance + dates  
5. Verified named contacts (role + authority basis when known)  
6. Previous hotel/venue history when evidenced  
7. Hotel-specific qualification + pursuit recommendation  
8. Research run history / material change markers  
9. Continuation state + next refresh triggers  
10. Confidential hotel-specific feedback isolation (never cross-hotel)

### Current gap

`processed[]` rows are thin; bridge falls back to the processed row and can drop evidence/drawer/full contacts. Full objects exist in `opportunity-research-state/*.json` and in-chain opportunity objects.

**Slice 2:** attach full opportunity (or ORS reload) into bridge extract; dry-run first; no production writes without founder auth.

Airtable owns mirrored research-inventory rows under explicit write-set gates; filesystem ORS/state remains the research working set. Market Opportunity Graph modules exist for cross-hotel reuse but are not default intake.

---

## D. Research economics (captured per run)

Module: `gdi-research-economics-ledger-v1.js` → `research-economics.json`

Includes: discovered · LIVE_NEW · previously known · considered · selected · skipped(+reasons) · spend · contacts gained · actionable · stopped/monitored · reused evidence · zero-yield honesty · cost per incremental actionable (when spend > 0).

Cheaper ≠ better without coverage + quality.

---

## E. Slice plan (unchanged sequencing)

| Slice | Scope |
| --- | --- |
| **1** | Provenance · coverage · selection transparency · progressive depth contract · durable budget · synthetic protection · offline priority comparison (**this amendment**) |
| **2** | Durable intelligence & Airtable persistence bridge (dry-run first) |
| **3** | Commercial Intelligence Brief v2 drawers |

---

## F. Acceptance for amended Slice 1

- [x] Trace selection path + fair-floor/cap starvation documented  
- [x] Offline first-in-list vs current vs proposed comparison harness  
- [x] Progressive depth contract defined; telemetry attached  
- [x] Economics ledger written per chain run  
- [x] Budget durability + yield honesty from prior Slice 1 work  
- [x] No new production selection weights enabled  
- [x] No paid calls / no Airtable production writes  
- [ ] Founder review of prioritization + depth recommendations **before** Slice 2 and before any production selection policy change

**STOP** — await founder decision on selection-policy option (1/2/3) and Slice 2 authorization.
