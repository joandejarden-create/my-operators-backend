# Founder forensic report — Original GDI contact search & alternative-person intelligence

**Contract:** `gdi-contact-search-alternative-person-archaeology-v1`  
**Mode:** $0 forensic audit only — **STOP before implementation**  
**Compared tips:** `origin/main` `a7171e4a3` · PR #585 `5c4d41308`  
**No Exa / Findymail / Surfe / Airtable / STAGED / CURRENT / publish in this audit**

---

## Executive verdict

Substantial contact-intelligence architecture **still exists in the repo** as libraries. It is **not fully executing** inside canonical Full E2E / CohortExecute / the Design Hotels live buyer-WHO recovery.

PR #585 correctly restored:
- min-1 named-person finalization
- separate WHO / HOW completeness gates
- public-first HOW after WHO

PR #585 did **not** restore the original **alternative-person / role-hierarchy search loop**. That is the primary missing orchestration link behind the 113 → 3 yield.

**Last “designed as a whole system” surface (libraries intact):** Scaling / adaptive buyer stack dated ~2026-11-05 (`b97ccb27b` lineage), ontology from 2026-10-13 (`f565f21cf`), commercial selection 2026-11-20 (`b5566d115`).  
**Canonical Full E2E chain birth:** 2026-11-11 (`eaca98603`) — wired deep research + contact investigation, **without** the buyer strategy ladder.  
**That disconnect remains on main and on #585.**

---

## 1. What was the original contact search architecture?

Layered, not a single script:

| Layer | Purpose | Canonical modules |
|---|---|---|
| Role ontology / priority | Ideal role vs influencer vs backup by event family | `contact-candidate/ontology.js` (`ROLE_PRIORITY_BY_FAMILY`, `ROLE_RELEVANCE`) |
| Buyer strategy ladder | Ordered public search strategies when ideal buyer missing | `scaling-buyer-research-strategies-v1.js` (`BUYER_RESEARCH_STRATEGY_SEQUENCE`, `selectNextBuyerResearchStrategy`) |
| Adaptive recovery planner | Offline next-action + extended pathways | `buyer-recovery-planner-v1.js` (`EXTENDED_BUYER_RECOVERY_PATHWAYS`) |
| Scaling investigation loop | Executes strategy ladder iteratively | `research/scaling-intelligent-investigation-loop-v1.js` |
| Buyer-WHO fallback | Post-deep forced WHO queries + alt source URLs | `research/autonomous-deep-research-recovery-v1.js` (`ensureBuyerWhoResearchExecuted`, `buildBuyerDiscoveryQueries`) |
| Contact eligibility / ranking | Direct buyer vs entry point vs unsupported | `contact-eligibility-standard-v1.js`, `presentation/gdi-commercial-contact-selection-v1.js` |
| Primary / secondary selection | Two-contact standard | `secondary-named-contact-v1.js`, `scaling-two-contact-standard-v1.js`, `research/scaling-contact-intelligence-v1.js` |
| Completeness WHO/HOW split | Named person vs channels | `contact-completeness-v1.js` (`splitWhoHow`), Contact Intelligence V1/V2.1 |
| Public HOW | Email/phone/LinkedIn public-first | `presentation/gdi-public-contact-discovery-workflow-v1.js` (+ hierarchy / verification) |
| Identity reuse | Cross-opportunity person reuse | `workflow/gdi-opportunity-contact-lineage-reuse-v1.js` |
| Optional paid HOW | Surfe/Findymail after public exhausted | `workflow/buyer-contact-enrichment-v1.js` — **not** the WHO engine |

Intended sequence (historical design intent):

```
DEMAND → DEEP RESEARCH → WHO (ideal role)
  → if missing: next strategy in ladder / adjacent roles / intermediaries
  → rank candidates (buyer authority > commercial relevance > channels)
  → select primary + secondary
  → HOW public channels
  → optional paid enrichment
  → commercial qualification → STAGED
```

---

## 2. What alternative-person logic did we previously build?

Explicit distinctions already encoded:

| Class | Meaning | Where |
|---|---|---|
| **PRIMARY BUYER** | Direct hotel / meetings / housing / sourcing decision influence | `COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER`, `ROLE_RELEVANCE.PRIMARY_DECISION_MAKER`, eligibility `DIRECT_BUYER` |
| **VIABLE ALTERNATIVE** | Credible commercial entry / influencer who can route to the decision | `RELEVANT_COMMERCIAL_ENTRY_POINT`, `ALTERNATIVE_ENTRY_POINT` in `classifyPeopleIntoContacts`, ontology `STRONG_INFLUENCER` / `OPERATIONAL_CONTACT` / `BACKUP_CONTACT` |
| **SECONDARY CONTACT** | Independently verified second named person | `selectSecondaryNamedContact`, scaling two-contact standard, named-coverage `SECOND_VERIFIED` |
| **ORGANIZATION ROUTE** | Homepage / desk / mailbox — **not** a named-person substitute | Org fallback after exhaustion; #585 finalization gate refuses org-only |

**Strategy ladder (do not stop after first miss)** — `BUYER_RESEARCH_STRATEGY_SEQUENCE`:

1. Official leadership / team pages  
2. Meetings / incentives / sales ops  
3. Workplace / facilities / CRE  
4. People / HR / L&D  
5. Regional ops / expansion  
6. Procurement / EA / admin  
7. Press-named then verify affiliation  
8. Public professional profiles  
9. Org route ask-for **paired with** a verified named person  

Extended planner pathways (associations / projects / local language / chapter leadership): `EXTENDED_BUYER_RECOVERY_PATHWAYS` in `buyer-recovery-planner-v1.js`.

Relevance / confidence: commercial contact scoring + classification (`gdi-commercial-contact-scoring-v1.js`, `gdi-commercial-contact-classification-v1.js`); eligibility classes; strategy gain scores by gap (`STRATEGY_GAIN_BY_GAP` in buyer-recovery-planner).

---

## 3. Where is that code today?

**Present on disk** (main and #585). Classification:

| Capability | Status | Notes |
|---|---|---|
| Role ontology / priority ladders | **ACTIVE (library)** | Not driven by Full E2E orchestrator |
| Buyer strategy ladder | **DISCONNECTED from Full E2E** | Used by Scaling investigation loop + offline planner + some adaptive/association runners |
| Buyer-recovery planner | **PARTIALLY PRESERVED** | Explicitly “offline recommendations only” — does not execute searches |
| `ensureBuyerWhoResearchExecuted` | **ACTIVE (shallow)** | Wired in Full E2E deep path; ≤5 fixed queries; **no ladder loop** |
| Contact investigation pass | **ACTIVE (partial)** | Ranks/harvests **existing** evidence; official HTTP follow-up; does not generate role-hierarchy searches |
| Commercial contact selection | **ACTIVE (library)** | Not called from `original-full-e2e-research-chain-v1.js` |
| Secondary named selection | **ACTIVE (library)** | Contact pass documents attempt; live recovery rarely produced secondaries |
| Public HOW workflow | **WIRED in #585** / **DISCONNECTED on main** | #585 adds `HOW_CONTACT_CHANNEL_DISCOVERY` after WHO |
| Named-contact min-1 gate | **ACTIVE (#585)** | Not on main until merge |
| Identity reuse | **ACTIVE (library)** | Live recovery imported lineage helpers; limited yield impact |
| Surfe/Findymail | **OPTIONAL_PAID_OFF** | Correctly not WHO |

**Callers of `selectNextBuyerResearchStrategy` / `planBuyerRecovery` today:**  
`scaling-intelligent-investigation-loop-v1.js`, `buyer-recovery-planner-v1.js`, `adaptive-*`, `association-events-hotel-runner-v1.js`, `scaling-companies-full-e2e-publication-v1.js` — **not** `original-full-e2e-research-chain-v1.js`, **not** `Invoke-GdiPortfolioProcessing.ps1`.

---

## 4. Is it executing in current Full E2E?

| Surface | WHO shallow | Strategy ladder / alternatives | HOW public-first |
|---|---|---|---|
| `origin/main` Full E2E chain | Yes (buyer-WHO + contact pass) | **No** | **No** (until #585) |
| PR #585 Full E2E chain | Yes + named min-1 | **No** | **Yes** (new stage) |
| `Invoke-GdiPortfolioProcessing.ps1` FullE2E / CohortExecute | Delegates to Node Full E2E | **No ladder** | Only if Node chain has HOW (#585) |
| Design Hotels live recovery script | `ensureBuyerWhoResearchExecuted` only | **No** | **No** (WHO-only recovery) |
| Scaling companies intelligent loop | Yes | **Yes** | Via scaling path |

**Trace caveat:** Presence of stage `BUYER_WHO_RESEARCH` ≠ alternative-person hierarchy. Live traces show a single forced WHO query batch, then stop.

---

## 5. What functionality was lost or bypassed?

1. **Iterative alternative-role search** after ideal buyer miss  
2. **Strategy exhaustion criteria** before declaring unresolved  
3. **Commercial ranking** (`selectBestCommercialContacts`) in the canonical chain  
4. **Ontology-driven role priority** as the search generator (not just a classifier of what Exa already returned)  
5. **HOW after WHO** on main (fixed in #585 wiring only)  
6. **Viable alternative retention** when junk extractions dominate (false “DM established” then all rejected)  
7. **Second-person research** as a real second pass (often status-only annotation)

Not removed from git — **orchestration bypass**.

---

## 6. Which commits introduced the regressions?

| Commit | Date | Effect |
|---|---|---|
| `eaca98603` | 2026-11-11 | Restored `original-full-e2e-research-chain-v1.js` without strategy ladder / commercial selection / HOW |
| `b9f7ea999` (#386) | ~2026-11 | Reconciled PowerShell contact research → contact investigation pass (still no ladder) |
| Live recovery `gdi-design-hotels-buyer-who-live-recovery-v1.mjs` (PR #585 work) | 2026-11-24 | Paid WHO via `ensureBuyerWho` only — intentional narrow recovery, **not** full ladder |
| `5c4d41308` (#585) | 2026-11-24 | Restores HOW + gates; **still does not** reconnect ladder |

Libraries from `b97ccb27b` / `f565f21cf` / `b5566d115` remain; Full E2E never fully adopted them.

---

## 7. Why did 113 contact searches produce only three additional names?

Evidence from `reports/gdi-buyer-who-live-recovery-v1/LIVE_RECOVERY_REPORT.json` (110 detailed rows):

| Observation | Count / fact |
|---|---|
| Rows with **zero** stakeholders after WHO | **95 / 110** |
| Rows with some stakeholders | **15** (often chrome/nav junk) |
| `buyer_intelligence_gap_forced_who_and_discovery_queries` | **108** — WHO ran, gap remained |
| `buyer_who_established_role_relevant_dm` | **2** — both still `UNRESOLVED_BUYER` after quality gates |
| Newly verified accepted buyers | **3** (Lori Raleigh, Karla Mawcinitt, Joni Musabayana) |
| Strategy ladder iterations | **0** |
| HOW channel research in that run | **0** |

### Failure modes (orchestration ≠ provider alone)

1. **Shallow WHO:** `buildBuyerDiscoveryQueries` + `researcher.execute` capped to **≤5** queries (`slice(0, 5)`). No advance through `BUYER_RESEARCH_STRATEGY_SEQUENCE`.  
2. **No alternative-role regeneration** after rejection — one pass then `UNRESOLVED_BUYER`.  
3. **Extraction junk:** e.g. Journey Mexico — stakeholders “Family Travel”, “Historic Center”, “Orizaba Street”; `hasRoleRelevantDecisionMaker` returned true (false positive); all 9 rejected as junk; `primaryContact` null.  
4. **Viable people overlooked:** EventPipe row contained **Craig Davis** / **Amy Yoder** amid junk (“Hotel Booking Software”); primary stayed null — ranking/selection not applied; no second strategy.  
5. **Strict quality gates** (plausible name, employer affiliation, source URL) correctly blocked bad names; without ladder continuation, correct rejects = terminal unresolved.  
6. **Provider limits secondary:** Exa returned pages; many orgs simply yielded no clean person text. That is real — but **cannot explain** skipped ladder or ignored viable names.  
7. **Serialization:** Accepted buyers preserved via restage; unresolved never got a second chance in-chain.

**Differentiation:** ~86% zero-stakeholder is partly provider/evidence; the rest (and especially “DM established → still unresolved” + discarded real names) is **orchestration / ranking / false-positive WHO completion**.

---

## 8. Can existing code restore the missing capabilities?

**Yes — reconnect, do not rewrite.**

Minimum reuse set:

1. `selectNextBuyerResearchStrategy` + attempt ledger on opportunity  
2. `planBuyerRecovery` / extended pathways for segment-aware next steps  
3. `ROLE_PRIORITY_BY_FAMILY` / `preferredRolesForOpportunity` to generate role-targeted queries  
4. `selectBestCommercialContacts` + `selectSecondaryNamedContact` after each harvest  
5. Existing `ensureBuyerWhoResearchExecuted` as **one** ladder step, not the whole WHO engine  
6. Existing HOW (`runRestoredHowContactChannelDiscovery`) after WHO_COMPLETE  
7. Keep #585 named-person gate + separate HOW/Sales Ready gates  

Buyer-recovery-planner is currently offline-only — needs a thin **executor** that applies recommended strategy IDs via existing researcher (mocked in tests).

---

## 9. Smallest safe implementation plan (for after founder GO)

**Do not implement until approved.**

1. Add Full E2E stage `BUYER_ALTERNATIVE_PERSON_RESEARCH` after shallow WHO when `WHO_INCOMPLETE` or quality-rejected.  
2. Loop `selectNextBuyerResearchStrategy` until named person accepted **or** ladder exhausted (document exhaustion).  
3. After each harvest: `selectBestCommercialContacts` → apply primary; `selectSecondaryNamedContact` for second.  
4. Reject unrelated executives via existing commercial classification (already prefers meetings/housing over pure C-suite).  
5. Keep HOW separate; never require email for WHO.  
6. Mock-certify the 10 scenarios in Phase 6 below.  
7. No paid Exa until wiring certified $0.  
8. No STAGED mutation until cert PASS + founder GO.

---

## 10. Tests that permanently protect this

Proposed permanent suite (mocked providers, $0):

1. Ideal DM found → primary set; ladder not over-run  
2. Ideal unavailable → viable alternative selected  
3. Multiple alternatives → best ranked (buyer authority > relevance > channels)  
4. Second relevant person identified  
5. Unrelated senior executive rejected  
6. Person verified without email → WHO_COMPLETE, HOW may be incomplete  
7. Previously identified person reused (lineage / cross-hotel cache)  
8. WHO succeeds, HOW incomplete  
9. Org homepage alone → not finalized  
10. Ladder exhausted → documented unresolved; **must show >1 strategy attempted**

Assert stage order: WHO → alternative loop → HOW → qualification.  
Assert Full E2E / Cohort / resume / replay all record alternative attempts when WHO incomplete.

---

## Historical-versus-current capability matrix

| Capability | Historical design | `origin/main` | PR #585 | Classification |
|---|---|---|---|---|
| Buyer identification (shallow WHO) | Yes | Yes | Yes | ACTIVE |
| Decision-maker role preference | Ontology + scoring | Library only | Library only | DISCONNECTED |
| Alternative-person / strategy ladder | Scaling + planner | Not in Full E2E | Not in Full E2E | DISCONNECTED |
| Role hierarchy escalation | `ROLE_PRIORITY_BY_FAMILY` + ladder | Library | Library | DISCONNECTED |
| Contact candidate ranking | Commercial selection | Library | Library | DISCONNECTED |
| Primary/secondary selection | Scaling two-contact / secondary-named | Partial (contact pass) | Partial + coverage metadata | PARTIALLY PRESERVED |
| Public person research | Buyer-WHO + native WHO | Shallow WHO | Shallow WHO | PARTIALLY PRESERVED |
| Employer verification | Eligibility + affiliation gates | Active in live recovery | Active | ACTIVE |
| LinkedIn / email / phone HOW | Public discovery workflow | Bypassed in Full E2E | Wired after WHO | #585 ACTIVE / main DISCONNECTED |
| Identity reuse | Lineage + CI V2.1 | Library | Library + recovery import | PARTIALLY PRESERVED |
| Search exhaustion criteria | Ladder exhausted + planner | False-exhaust guards exist; ladder unused | Same | DISCONNECTED |
| Contact fallback handling | Org route after named attempt | Org-only can linger | Org-only cannot finalize | PARTIALLY PRESERVED (#585 better) |
| Findymail/Surfe | Optional after HOW | Off by default | Off / refused in HOW | ACTIVE (correctly off) |

---

## Phase 5 — E2E integration check

- **Invoke-GdiPortfolioProcessing.ps1:** Thin launcher → Node Full E2E / Cohort. No alternative-person logic in PowerShell.  
- **FullE2E / CohortExecute:** Inherit chain gaps above.  
- **BUYER_WHO_RESEARCH:** Present; demonstrates query batch, **not** ladder.  
- **Deep-research orchestration:** ORS + optional Exa seed; WHO fallback attached; alternatives = URL seeds only.  
- **Finalization / STAGED:** #585 enforces ≥1 named; does not prove alternative search occurred.

---

## Phase 6 — Certification scenarios (planned; not run as new suite in this audit)

Scenarios 1–10 listed in §10. Existing related tests:  
`test:gdi-named-contact-coverage-standard-v1`, `test:gdi-who-how-contact-intelligence-restore-v1`, `test:gdi-public-source-contact-discovery-permanent-v1`, Scaling investigation tests (if present).  
**Gap:** no permanent test that **fails** when Full E2E stops after first unsuccessful WHO query without ladder attempts.

---

## Answers to the ten founder questions (short)

1. **Original architecture:** Ontology + strategy ladder + ranking + two-contact + WHO/HOW split + optional paid HOW.  
2. **Alternative-person logic:** Ordered strategies + entry-point classes + secondary selection + extended pathways.  
3. **Code today:** Intact as libraries; Scaling/adaptive paths use ladder; Full E2E does not.  
4. **Executing in Full E2E?** Shallow WHO yes; alternative hierarchy **no**; HOW yes on #585 only.  
5. **Lost/bypassed:** Ladder loop, ontology-driven search generation, commercial ranking in-chain, main HOW.  
6. **Regression commits:** Especially `eaca98603` Full E2E restore without ladder; live recovery narrow WHO path.  
7. **113→3:** Shallow single-pass WHO + junk extraction + false DM flags + quality rejects without continuation; some real names discarded.  
8. **Restore from existing code?** Yes — reconnect ladder + ranking into Full E2E.  
9. **Smallest plan:** Alternative-person stage looping existing `selectNextBuyerResearchStrategy` + selection helpers; mock cert; then paid.  
10. **Protecting tests:** Ten scenarios in §10, especially “must attempt multiple strategies before unresolved.”

---

## Governance confirmation

- $0 forensic audit  
- No additional Exa / Findymail / Surfe  
- No Airtable writes  
- No STAGED / CURRENT changes in this phase  
- No production publication  
- Historical packages / worktrees preserved  

## STOP

**Awaiting founder review before any implementation.**  
Recommended GO scope if approved: reconnect existing alternative-person ladder into canonical Full E2E + permanent mock certification — not a new contact engine.
