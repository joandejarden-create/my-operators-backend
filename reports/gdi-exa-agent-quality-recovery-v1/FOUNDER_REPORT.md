# FOUNDER REPORT — Exa Agent Quality Recovery ($0)

**Contract:** `gdi-exa-agent-quality-recovery-v1`  
**Date:** 2026-11-24  
**Paid calls:** **$0** (no Exa / Findymail / Surfe / Airtable / STAGED pointer / CURRENT / publish)  
**Base tip:** `0956eeab8aa6d0ebab790b3535afca42e269d889`  
**Wave reference:** `reports/gdi-exa-agent-30-opportunity-live-wave-v1/FOUNDER_REPORT.md`  
**Authoritative ledger:** `reports/gdi-exa-agent-quality-recovery-v1/RECONCILED_SCORECARD.json`

---

## 1. Authoritative reconciled scorecard

| Metric | Wave (historical) | After quality recovery |
|---|---|---|
| Screened | 30 | 30 |
| Paid-eligible | 29 | 29 |
| Agent-researched | 28 | 28 |
| Exa spend (wave) | $2.80 | $0 additional |
| Primary among Agent | **20/28 raw (71.4%)** | **13/28 (46.4%)** |
| Quality-adjusted primary (wave) | 17/28 (60.7%) | — superseded by corrected 13/28 |
| Preliminary “21/28” / local STAGED | 21 STAGED packages | Revalidated below |
| DIRECT_BUYER | 3/28 (10.7%) | **3/28 (10.7%)** (same count; different evidence bar) |
| VIABLE_ALTERNATIVE (corrected) | (inflated / mixed) | **10/28** |
| Distinct secondary | — | **15** |
| WHO_COMPLETE (commercial primary) | inflated via weak named floor | **13/28** |
| Sales Ready | 0 | **0** |
| STAGED cert (21 local packages) | 21 prepared | **13 PASS / 6 HOLD / 2 REJECT** |
| Non-staged / no commercial WHO among 29 | — | Additional REJECT/no-recovery rows outside the 21 packages |

### Scorecard reconciliation notes

- **Preliminary 21/28** ≈ local STAGED candidate packages written when `finalizationEligible` was true under the pre-recovery named floor (includes weak / junk / RELEVANT_ORG cases).
- **Final raw 20/28** = Agent-accepted primary name present after live wave verification.
- **Quality-adjusted 17/28** = wave founder correction excluding obvious junk / weak cases without full structural gates.
- **Corrected 13/28** = authoritative post-recovery commercial WHO primaries (DIRECT_BUYER or VERIFIED_VIABLE_ALTERNATIVE) under hardened name + buyer classification + employer gates.
- Historical wave reports were **not rewritten**. Corrected ledger is additive under `reports/gdi-exa-agent-quality-recovery-v1/`.

Per-opportunity rows (ID, hotel, demand, Agent cost, before/after class, WHO, Sales Ready, STAGED disposition) are in `RECONCILED_SCORECARD.json`.

---

## 2. Root causes of false positives

1. **Non-person text harvested as names** — UI/platform chrome (`Android Once`) and nav/membership controls (`Member Select`) passed earlier extraction/ranking because gates relied too heavily on banned-phrase lists without structural given/surname / chrome / ALL-CAPS checks.
2. **Title prestige ≠ lodging authority** — Director / Manager / President / CEO / Executive Director were promoted to DIRECT_BUYER without evidenced accommodation, housing, meetings buying, or supplier-lodging responsibility.
3. **RELEVANT_ORG coerced to VIABLE_ALT** — Agent verification mapped organizational contacts into viable alternatives, satisfying WHO/finalization without a commercial pathway.
4. **Missing person-level provenance in wave packages** — many primaries stored `sourceUrl: null`; homepage-only or absent URLs still finalized under the old path.
5. **Named-coverage floor conflated with commercial WHO** — secondary-alone or weak affiliated names could look “finalization eligible” without a commercial primary.

---

## 3. Before / after primary recovery

| Scope | Before | After |
|---|---|---|
| Agent researched | 28 | 28 |
| Primary recovered | 20 (raw) / 17 (quality-adj) | **13 commercial WHO** |
| Rate | 71.4% / 60.7% | **46.4%** |
| Target ≥70% commercial WHO | Fail (quality-adj) | **Fail** |

Positive retention examples (patterns, not hardcoded production rules):

- **Cecilia Gochi Salazar** — accommodation / official travel agency evidence → **DIRECT_BUYER** (PASS).
- **Ricardo Plancarte** — Local Organizing Committee chair → **VIABLE_ALTERNATIVE** (PASS; not overstated as direct buyer).

Rejected junk (held out of WHO):

- **Android Once**, **Member Select** → **REJECT** (`JUNK_OR_IMPLAUSIBLE_NAME`).

---

## 4. Before / after direct-buyer classifications

| | Before (wave) | After |
|---|---|---|
| DIRECT_BUYER count | 3 | **3** |
| Rate | 10.7% | 10.7% |
| Target ≥20% | Fail | **Fail** |

Corrected DIRECT_BUYER set (evidenced lodging/meetings buying only):

1. Cecilia Gochi Salazar — accommodation / official travel agency  
2. Rafael Fernández — Reservations Manager  
3. Jason Gooding — Race Director / Events Manager (events + lodging pathway text)

Former “direct” LOC leadership without lodging verbs (e.g. Ricardo) correctly lands as **VIABLE_ALTERNATIVE**.

Seniority-only leaders (e.g. Director General / Executive Director without lodging evidence) → **VIABLE_ALTERNATIVE** (`SENIORITY_WITHOUT_LODGING_EVIDENCE_NOT_DIRECT_BUYER`), not DIRECT_BUYER.

Bare “Manager” / weak team-page affiliations → **RELEVANT_ORGANIZATIONAL_CONTACT** (HOLD; not WHO-finalizing).

---

## 5. Secondary-contact coverage

- Distinct secondary after reassess: **15**
- Secondaries remain labeled `SECONDARY_CONTACT`
- Secondary alone **cannot** finalize WHO (`finalizeEligible` requires primary)
- Duplicate / junk secondaries dropped when name fails structural gates

---

## 6. HOW and Sales Ready assessment

- Public HOW channels preserved where present; org inboxes remain org routes (prior hardening).
- HOW research status independent of WHO.
- **Sales Ready remains 0/28** — not weakened to improve reported results.
- WHO_COMPLETE (corrected) = commercial primary only; Sales Ready still requires verified direct channel ownership independently.

---

## 7. STAGED candidate PASS / HOLD / REJECT matrix

Local packages under `reports/gdi-exa-agent-30-opportunity-live-wave-v1/staged-candidates/` were **revalidated only** (no pointer flip, no CURRENT, no publish).  
Cert artifacts: `reports/gdi-exa-agent-quality-recovery-v1/staged-revalidation/`.

### PASS (13) — eligible for finalized STAGED under corrected commercial WHO

Includes Cecilia (DIRECT), Ricardo (VIABLE/LOC), Rafael (DIRECT), Jason (DIRECT), and evidenced VIABLE leadership/pathway contacts (Jessica Culp, Dr. Shola K. Roberts, Berta Redondo Benito, Renee Goodwin, Kseniya Kizilova, Kirl Grant-Hoschtialek, Lisa Indar, Laura Lozano Ortega, Liza Ortegón).

### HOLD (6) — additional evidence required (not treated as junk reject)

| Opportunity | Person | Reason |
|---|---|---|
| disc_ccffaf9621e9 | Keyla Velazquez | RELEVANT_ORG / no commercial pathway |
| disc_8b3b9f9c18e6 | Naomi Akoy-Bouguenon | Employer verification |
| disc_6b57fc507245 | Lucas Sales | RELEVANT_ORG |
| disc_cda30433df36 | Meryem Bennouna | RELEVANT_ORG |
| disc_32d284433de0 | Mafalda Cassiano Neves | RELEVANT_ORG |
| disc_cc82b5efa9f2 | Thomas Dubois | RELEVANT_ORG |

### REJECT among the 21 STAGED packages (2)

- `Android Once` — junk / non-person  
- `Member Select` — junk / nav chrome  

Remaining Agent-researched opportunities outside these 21 either never staged or remain non-qualifying under corrected WHO (see full ledger).

**Pointer status:** STAGED pointers **unchanged**. CURRENT **unchanged**. Historical packages preserved.

---

## 8. Code changes and regression coverage

### Modules strengthened (canonical process — no new engine)

- `gdi-original-full-e2e-execution-restore-v1.js` — structural `isPlausibleBuyerPersonName`; `personQualifiesForWhoFinalization` (commercial class / evidenced commercial role; RELEVANT_ORG blocked)
- `gdi-exa-agent-contact-escalation-v1.js` — DIRECT_BUYER requires lodging/meetings evidence; seniority → VIABLE; RELEVANT_ORG not coerced to VIABLE; homepage-only without role evidence not commercial
- `gdi-named-contact-coverage-standard-v1.js` — junk/RELEVANT_ORG cannot be primary; secondary alone cannot finalize; named floor preserved for historical packages

### Permanent fixtures + CI

- `fixtures/gdi-exa-agent-quality-recovery-v1/wave-person-name-regression.json`
- `fixtures/gdi-exa-agent-quality-recovery-v1/commercial-buyer-classification-regression.json`
- `scripts/test-gdi-exa-agent-quality-recovery-v1.mjs` (11 checks)
- `scripts/gdi-exa-agent-quality-recovery-v1.mjs` ($0 reconcile/reassess)
- Wired: `package.json` + `.github/workflows/gdi-regression-recovery-baseline.yml`

### CI assertions covered

- Non-person text rejected; verified people retained  
- Titles alone ≠ buyer authority  
- Weak org affiliation ≠ direct buyer  
- Evidenced viable alternatives remain eligible  
- Agent candidates never bypass independent verification  
- WHO / HOW / Sales Ready independent  
- Agent auth + budget controls preserved; Agent OFF without authorization  

---

## 9. PR and CI status

- Branch: `feat/gdi-exa-agent-quality-recovery-v1`
- Commit: `9769bc8c4`
- PR: **https://github.com/joandejarden-create/my-operators-backend/pull/589** (`OPEN`)
- Local suites: quality-recovery, escalation, named-coverage, who-how, agent-recovery-hardening, ALT ladder, call-graph — **PASS**
- `dealality:ci-precheck` — **PASS**
- GitHub CI: pending on PR
- Registered `ws-gdi` remains on unrelated dirty branch — preserved; shipped from tip-aligned worktree at `0956eea`
- **Integration Controller owns merge** — GDI chat stops at open PR

---

## 10. Recommendation on routine Full E2E Agent activation

| Option | Assessment |
|---|---|
| **A. Continue Agent as separately authorized research capability** | Compatible with current dual-auth + allowlist + ceilings. Keeps $0 default. |
| **B. Activate as standard conditional fallback inside authorized Full E2E budgets** | Premature — commercial WHO 46%, DIRECT 11%, Sales Ready 0; false-positive classes only just hardened. |
| **C. Hold broader Agent scaling until additional improvements** | **Recommended.** |

### Recommendation: **C** (with A as the operating mode while held)

Scale Agent on the quality of these distinctions, not raw named-person recovery:

1. Real person (structural name gates)  
2. Affiliated with demand org (employer verification)  
3. Commercially relevant alternative (evidenced pathway)  
4. Actual lodging/meetings decision-maker (DIRECT_BUYER evidence)

**Do not activate routine Full E2E Agent policy in this task.** Agent remains OFF without explicit authorization.

---

## Governance confirmation

- $0 only · no Exa · no Findymail/Surfe · no Airtable writes  
- No STAGED pointer flips · no CURRENT · no customer publication · no production deploy  
- Historical wave evidence preserved  
- STOP for founder review before integration  

**Final status:** `AWAITING_INTEGRATION_CONTROLLER`
