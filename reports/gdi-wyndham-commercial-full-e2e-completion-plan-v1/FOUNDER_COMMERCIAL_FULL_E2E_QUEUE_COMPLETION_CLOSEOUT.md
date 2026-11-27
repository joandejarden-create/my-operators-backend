# FOUNDER CLOSEOUT — Commercial Full E2E Queue Completion (P0)

**At:** 2026-11-27  
**Auth:** Diagnose + implement minimal $0 workflow corrections · prepare commercial plan  
**NOT authorized:** paid research · Airtable apply · STAGED · CURRENT · Findymail · Surfe  
**Freezes:** Bethesda ON · Wyndham CLEAN 11 ON  
**Branch:** `feat/gdi-commercial-full-e2e-queue-completion-v1`  
**Prior cert (do not restart):** `reports/gdi-wyndham-sd-bayside-paid-live-cert-25usd-v1/`

---

## 1. Actual deep-selection algorithm (code)

### Flow (traced)

1. **Discovery** → collect all retained candidates (`original-full-e2e-research-chain-v1.js` ~1691–1721).
2. **Pass A — pre-deep screen ALL candidates ($0)** — `screenAllCandidatesPreDeep` / `screenCandidatePreDeep` in `gdi-pre-deep-candidate-screening-v1.js`.
3. **Pass B — fair floor + score fill under `maxCandidates`** — `selectCandidatesForDeepResearch` in `gdi-deep-research-selection-register-v1.js` (~315–486).
4. **Deep loop** on selected only; CAD / competitor mapping / FQO run **after** deep, not as pre-deep reject gates.

### Scoring (`scoreSelectionDimensions`, selection-register ~60–189)

`combinedScore = weighted*100*0.65 + legacyInvestmentScore*0.35 + lodgingBuyerBoost + capabilityFit.boostPoints`

Weights: credible 0.25 · hotel fit 0.2 · novelty 0.15 · timing 0.15 · gaps 0.1 · commercial 0.1 · cost efficiency 0.05.

Order: global score → fair pass (≤`fairMinPerSegment` default **1**/segment, lodging/buyer preferred) → commercial fill by score until cap.

### Eligibility (pre-deep)

| Eligible | Ineligible |
|---|---|
| `ELIGIBLE_FOR_DEEP_RESEARCH` | `DUPLICATE_NO_NEW_EVIDENCE` |
| `ELIGIBLE_FOR_TARGETED_RESEARCH` | `GLOBAL_INTELLIGENCE_ONLY` |
| Enrichment when `allowDeepResearch` | `REJECT_UNSUPPORTED` / `SCREEN_ONLY` |

**Not auto-reject:** missing named buyer/channel; competitor host alone (`competitorHostIsPermanentDisqualification: false` in pursuit screen). Competitor-committed still `ADVANCE_TO_DEEP` unless catchment-unfit without overflow.

### Cap / fairness

| Layer | Behavior |
|---|---|
| Canonical entry / CLI (historical) | `maxCandidates ?? 8` — **CERTIFICATION_SAMPLE** |
| Chain alone if `maxCandidates=null` | Uses full eligible pool length |
| Deferred | Register-only; recompete next run; **no durable queue resume** (until this PR) |

**Screening ≠ FQO.** FQO is independent six-rule after research.

---

## 2. Reconciliation of all 161 discoveries

Lossless unique partition (Wyndham cert `gdi_e2e_14seg_mwyoeueh_729210`):

| Population | Count | Overlap? |
|---|---:|---|
| Discovered unique | **161** | — |
| Final `DUPLICATE_NO_NEW_EVIDENCE` / `DUPLICATE_REUSE` | **32** | Disjoint from eligible |
| Eligible after cross-segment dedupe | **129** | 161 − 32 |
| Deep selected (cert sample) | **8** | ⊂ 129 |
| Deferred (eligible, not selected) | **121** | 129 − 8 |
| **Founder “32 gap”** | **32** | = duplicate/reuse population — **not** missing |

`screenedCount` 172 = 161 unique + **11** cross-segment remap decision rows (same ID written twice: eligible → then duplicate).

Artifacts:

- `01-LOSSLESS_CANDIDATE_REGISTER.json` — one disposition per candidate  
- `02-COMMERCIAL_RESEARCH_COMPLETION_QUEUE.json`  
- `03-COMPLETION_PLAN.json`

---

## 3. Selection / rejection reasons (cert run)

- **Selected 8:** fair-segment floor (1 each across first 8 segments with eligible inventory) under `maxCandidates=8`.
- **Deferred 121:** `DEFERRED_LOWER_RANK` / fairness-or-budget — **omitted because fixed N reached**, not because commercially invalid.
- **Duplicate 32:** mostly `EXISTING_HOTEL_INVENTORY_ORG` (CLEAN 11 / published inventory) + cross-segment program suppressions.

---

## 4. Hotel vs competitive dispositions

New governed classifier: `gdi-competitive-business-classification-v1.js`

| Class | Meaning |
|---|---|
| DIRECT_PURSUIT | Credible business hotel can pursue |
| COMPETITIVE_DISPLACEMENT | Evidence-backed future contestability |
| OVERFLOW_OR_ANCILLARY | Spill / exhibitor / ancillary vs committed program |
| FUTURE_RECURRING_OPPORTUNITY | Future edition / cycle |
| HISTORICAL_OR_COMMITTED | Competitor intel without present pathway |
| RESEARCH_REQUIRED | Unresolved |

Maps onto existing CAD / competitor-commitment fields. Venue-only ≠ confirmed hotel contract. **Never auto-FQO.**

Permanent screening dispositions: `gdi-commercial-deep-research-screening-dispositions-v1.js`  
(`DEEP_RESEARCH_REQUIRED` … `INSUFFICIENT_INITIAL_EVIDENCE`). Contact gaps do **not** block deep.

---

## 5. Full research-completion queue

Modes: `gdi-commercial-deep-research-operating-modes-v1.js`

| Mode | Cap | Complete claim |
|---|---|---|
| **CERTIFICATION_SAMPLE** | default 8 | Never “comprehensive” while queue remains |
| **COMMERCIAL_FULL_E2E** | all eligible; budget-gated batches | COMPLETE only when every eligible has disposition; else `PARTIAL_RESEARCH_BUDGET_LIMIT` / `PARTIAL_QUEUE_REMAINING` |

CLI: `--operating-mode COMMERCIAL_FULL_E2E` or `--commercial-full-e2e`  
(default remains certification sample).

Queue builder: `gdi-commercial-research-completion-queue-v1.js`  
Completeness report distinguishes assessed / live / deep / competitive / FQO / continuation / auth pending; flags mock leisure + PARTIAL predictive.

---

## 6. Duplicate-billing recovery fix + tests

**Root cause:** buyer-WHO ledger path `null` → `live_deep_research_ledger_path_refused` hung chain; recovery `maybeSeedExaDeepResearch` re-POSTed Exa Agent (15 ops / 8 IDs) because:

1. In-memory stamps missing on rebuilt candidates  
2. Ledger settle used `kind: EXA_AGENT_DEEP_RESEARCH` without `stage: DEEP_RESEARCH`  
3. `completedDeepResearchOpportunityIds` only matched `stage===DEEP_RESEARCH`  
4. No durable Exa Agent job store

**Fixes shipped:**

| Fix | Location |
|---|---|
| Recognize `kind=EXA_AGENT_DEEP_RESEARCH` as settled | `gdi-authorized-paid-budget-ledger-v1.js` |
| Normalize `stage=DEEP_RESEARCH` on settle | same |
| `maybeSeed` skips ledger-settled + job-store resume | `gdi-original-full-e2e-execution-restore-v1.js` |
| Durable Exa Agent deep job store | `gdi-exa-agent-deep-job-store-v1.js` |
| Wire `buyerWhoLedgerPath` + allowlist cert/data prefixes | entry + `deep-research-gdi-live-certification-v1.js` |
| Pass ledger / auth / completed IDs into deep pass | chain |

**Tests:** `npm run test:gdi-commercial-full-e2e-queue-completion-v1` → **13/13 PASS**  
Also: `test:gdi-deep-research-selection-register-v1` PASS · `test:gdi-durable-budget-ledger-v1` PASS

---

## 7. Historical dossier reuse

- CLEAN 11 frozen / inventory org → `DUPLICATE_REUSE` (32)  
- Ledger-settled deep IDs → `DEEP_RESEARCH_ALREADY_COMPLETE_REUSE` / seed skip  
- Job store completed `providerRunId` → no resubmit  
- Prior Agent dossiers remain in opportunity-research-state (untouched)

---

## 8. Estimated full commercial research spend (Wyndham)

From cert artifacts + queue (no new paid calls):

| Metric | Value |
|---|---:|
| Unique discovered | 161 |
| Already deep (this cert) | 8 |
| Remaining eligible deep targets | **121** |
| Quote unit (Exa Agent medium) | $0.10 |
| **Expected Agent spend to clear queue** | **~$12.10** |
| Suggested batch size | 20 |
| Estimated batches | **7** |
| Prior cert spend (sunk) | $2.90 |

**Not claimed:** FQO count. Cert conversion was 1/8 deeps; do **not** treat 25–35 as a cap or guarantee.

Leisure still mock_live — full commercial completion also needs authorized live leisure if applicable modules must be paid-live.

---

## 9. Implementation PR + merge status

PR opens from this branch after prechecks.  
**GDI does not merge** — Integration Controller owns merge.

---

## 10. Next founder authorization required

```
FOUNDER_GO AUTHORIZE_WYNDHAM_COMMERCIAL_FULL_E2E_QUEUE
hotel=recnPB3Q02NO8Ma1o
operatingMode=COMMERCIAL_FULL_E2E
budgetCeilingUsd=<N>   # suggest 15–20 for first 2–3 batches (~40–60 deeps) or ~13 for full 121
authorizePaidResearch=1
authorizeExaAgentLive=1
reuseExistingAgentDossiers=1
batchSize=20
NO_AIRTABLE_APPLY until separate write GO
BETHESDA_FREEZE=ON
WYNDHAM_CLEAN11_FREEZE=ON
```

Optional separate GO for **paid leisure** live modules (not mock).

---

## SUCCESS CRITERIA (this PR)

- Every discovered candidate accounted for (161 lossless register)  
- Certification sample ≠ production policy (explicit modes)  
- Eligible queue resumes in budgeted batches; budget exhaustion = PARTIAL  
- Competitor-committed remains researchable  
- Duplicate Agent billing path fixed + tested  
- No paid campaign executed under this directive
