# HIF — Historical Foundation, Market Critical Mass & Airtable Persistence

**As of:** 2026-11-21  
**Publication:** CUSTOMER_BLOCKED  
**Research spend:** $0  
**Baseline main:** `a91d34b77` (includes PR #537 CB-1)  
**Branch:** `feat/hif-v6-track-a-foundation-airtable-persistence`  
**E2E classification:** **PERSISTENCE_VALIDATED** (Lakeville inventory+finding create + idempotent NOOP + read-back) — not FULL_E2E / not CUSTOMER_PUBLISHED

---

## A. Current inventory (live Airtable)

| Store | Count |
|-------|------:|
| Hotel Property Census (canonical hotels) | **19,462** |
| HI Hotel Discovery Inventory | **541** (+1 Lakeville) |
| HI Discovery Findings | **446** (+1 Lakeville BCT) |
| HI Hotel Contracts | **269** |
| HI Contract Milestones | **259** |
| HI Organizations | **7** |
| Qualified first-100 BCT/MCT (CB-1) | **1** |
| Commercially accessible (CB-1) | **1** |
| Outreach-ready | **0** |

---

## B. Country × Timing × Freshness × Accessibility

See CB-1 matrices + this foundation:

| Country | Coverage | Near-term BCT/MCT | Access | Gap label |
|---------|----------|-------------------|--------|-----------|
| US | Active (HPC heavy + CB-1) | 1 BCT (Lakeville) | Accessible probable | Private franchise windows still thin |
| CA | UNDER_RESEARCHED | 0 | — | Macro conversions ≠ hotel decisions |
| MX | UNDER_RESEARCHED | 0 | — | ES sources needed |
| BR | Partial (Aena adjacent) | 0 principal | — | Existing-hotel MCT thin |
| GB | Partial (Dorset award-pending) | 0 open | Closed tender | Award watch |
| Other | Worldwide eligible | incidental | — | e.g. Casa Mett SG adjacent |

---

## C. Market critical mass

| Market | Hotel universe estimate | HIF HPC hotels | Contract intel | Near-term BCT/MCT | Closest to commercial viability? |
|--------|-------------------------|----------------|----------------|-------------------|----------------------------------|
| US | UNKNOWN (HPC 19k global, US-dominant) | Large share of 19,462 | 269 contracts / 259 milestones (mostly US SEC) | 1 verified open BCT | **Closest** — continue private CB waves |
| CA | UNKNOWN | Present in HPC | Thin HI contracts | 0 | Not yet |
| MX | UNKNOWN | Present | Thin | 0 | Not yet |
| BR | UNKNOWN | Present | Thin + adjacent Aena | 0 | Not yet |
| GB | UNKNOWN | Present | Dorset pending | 0 open | Early |

Universe denominators outside HPC remain **UNKNOWN** (no fabricated STR/market totals).

**Recommendation:** Continue US private owner-led BCT/MCT foundation before claiming multi-country commercial launch readiness.

---

## D. Historical research posture

- SEC name-free + EX-10 persistence already landed contracts/findings (V4/V5).  
- CB-1 current-backward produced Lakeville (private, no RFP).  
- Historical events preserved separately from current opportunities.  
- Supersession: completed deals (Pommier, Durban, Cala) retained as RESEARCH_ONLY / completed — not opportunities.

---

## E. Airtable persistence ledger (this cycle)

| Action | Detail |
|--------|--------|
| Bases/tables | `appCCUsuGsE1ifoLk` → Discovery Inventory + Discovery Findings |
| Mapping | `docs/ai-build-system/HIF_FOUNDATION_AIRTABLE_SOURCE_DESTINATION_MAPPING.md` |
| Created | Inventory `recPXlLWkd63mLK5h` · Finding `recjL2vx3ExNlReF1` |
| Updated | 0 |
| No-ops (idempotent retry) | 2 |
| Conflicts | 0 |
| Research-only | 0 this batch |
| Read-back | OK · Publication State `CUSTOMER_BLOCKED` · Finding Type `BRAND_AFFILIATION` · Field `COMMERCIAL_OPPORTUNITY_BCT` |
| Schema changes | **0** |
| HPC creates | **0** |
| Errors/retries | 0 |

Dry-run: `LAKEVILLE_PERSIST_DRY_RUN.json` · Apply: `LAKEVILLE_PERSIST_APPLY.json`

---

## F. Commercial pipeline

- First-100: **1/100** (Lakeville Inn BCT, private pathway)  
- Relevant vs accessible: both 1 (contact not validated → not outreach-ready)  
- Public vs private: Lakeville private; Dorset/Chicago public closed  

---

## G. Weekly automation readiness

| Prerequisite | Status |
|--------------|--------|
| Stable hotel IDs (HPC + Discovery Property ID) | Ready |
| Stable contract / milestone / finding IDs | Ready |
| Idempotent upsert + read-back | Demonstrated |
| Change-type vocabulary designed | Documented below |
| Unattended scheduler | **NOT ACTIVATED** — needs founder review of cost/schedule/publication |
| Full historical backfill complete | No — continue foundation batches |

Proposed weekly deltas (future): NEW / UPDATED / TIMING_CHANGED / BRAND_CHANGED / OPERATOR_CHANGED / AGREEMENT_EXTENDED / COMMERCIAL_SIGNAL_ADDED / QUALIFICATION_CHANGED / ACCESSIBILITY_CHANGED / SUPERSEDED / CLOSED

---

## H. Integration

- Modules: `hif-foundation-persistence-v1.js`, audit + Lakeville persist scripts, mapping doc  
- Tests: `test:hif-foundation-persistence-v1` (+ existing V7/CB-1 gates)  
- PR: open from this branch → **AWAITING_INTEGRATION_CONTROLLER**  
- Dirty HE/ADP/GDI/BE/Track worktrees preserved · no post-merge apply  
- Cost: **$0**
