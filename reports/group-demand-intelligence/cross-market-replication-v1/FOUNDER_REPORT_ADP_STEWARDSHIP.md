# W Rome ADP Stewardship V1 — Founder Report

**Date:** 2026-09-25  
**Branch:** `deploy/gdi-pe-v1-7-customer-closure`  
**Property:** `adp_w_rome` · Census `rece0or38cxo3Fymb`

---

## P. FINAL VERDICT

**W ROME ADP PASSES — MINOR AUTOMATION GAPS REMAIN**

---

## A. ADP STATE BEFORE

| | |
|--|--|
| PROPERTY | `adp_w_rome` |
| CENSUS | **PASS** |
| PEER CANDIDATES | **5** |
| CERTIFIED PEERS | **0** |
| SCENARIOS | **20** |
| GATE | **ADP_STEWARDSHIP_BLOCKED** |

---

## B. PEER REVIEW

| Candidate | Decision | Peer Type | Evidence | Reason |
|-----------|----------|-----------|----------|--------|
| Rome Marriott Hotel (rommc) | **EXCLUDE** | — | Identity not verified as open centro; conflation risk w/ ROMAU Park | Unverified / weak geo |
| The St. Regis Rome | **APPROVE_CORE_PEER** | CORE | marriott.com romxr · Via V.E. Orlando 3 · ~161 keys | Luxury Bonvoy centro substitution |
| Hotel Eden (Dorchester) | **APPROVE_SECONDARY_PEER** | SECONDARY | dorchestercollection.com | Via Veneto lifestyle independent |
| Hotel de Russie (Rocco Forte) | **APPROVE_SECONDARY_PEER** | SECONDARY | roccofortehotels.com · Via del Babuino 9 · 00187 | Same postal lifestyle luxury |
| JW Marriott Rome | **EXCLUDE** | — | No JW Rome (Venice only in IT) | Does not exist |

**Post-review stewardship fills (after 2 excludes, to restore ADEQUATE):**

| Candidate | Decision | Evidence |
|-----------|----------|----------|
| The Westin Excelsior, Rome | APPROVE_CORE_PEER | romwi · Via Veneto 125 · 00187 |
| The Rome EDITION | APPROVE_CORE_PEER | romeb / editionhotels.com · 00187 lifestyle |

Jev did **not** approve/exclude peers.

---

## C. CERTIFIED PEER PACK

| | |
|--|--|
| CORE | **3** |
| SECONDARY | **2** |
| EXCLUDED | **3** (incl. Park Hotel note) |
| NEEDS_MORE_EVIDENCE | **0** |
| ADEQUACY | **PASS** (ADEQUATE / n=5) |

---

## D. SCENARIOS

| | |
|--|--|
| START | 20 |
| KEEP | 20 |
| REMOVE | 0 |
| MERGE | 0 |
| FINAL | 20 |
| CERTIFIED | **YES** (0 US/Bethesda/Renaissance bleed) |

---

## E. STEWARDSHIP GATE

| | |
|--|--|
| BEFORE | ADP_STEWARDSHIP_BLOCKED |
| AFTER | **ADP_STEWARDSHIP_PASS** |

---

## F. ADP RUN

| | |
|--|--|
| RUN ID / PERIOD | `adp_period_adp_w_rome_20260925105322_1192cf` |
| PROVIDERS | openai, gemini, perplexity, claude |
| PROMPTS/SCENARIOS | 20 |
| CALLS | 80 |
| SUCCESS | **80** |
| FAIL | **0** |
| TIMEOUT | 0 |
| RUNTIME | ~12 min |
| COST | **~$2.60** |

---

## G. FINDINGS

| | |
|--|--|
| Demand capture | **60.0%** (12/20 scenarios) |
| TOTAL material surfaces | brief + lostDemand + realityGap + whiteSpace + actions |
| STRONG EVIDENCE | Evidence index present; citation/owned-source fields populated |
| HIGH CONF unsupported | **0** detected in bleed/copy scan |
| ROOT CAUSE COMPLETE | Partial — structured root-cause fields thinner than Bethesda mature pack |
| ACTION COMPLETE | **1** action surface present |
| COMPLETION CRITERIA | Partial / automation gap vs gold baseline packs |

---

## H. PEER / SUBSTITUTION

| | |
|--|--|
| CONSIDERATION | W Rome appears across providers (35+ name/brand hits in published report) |
| EXCLUSION | Family/wellness/adventure intents weaker (0% capture on those 3 scenarios) |
| SUBSTITUTION | EDITION / St Regis / Eden / de Russie / Westin Excelsior referenced |
| PEER PACK USEFUL | **YES** |

---

## I. ADP JEV

| | |
|--|--|
| RUN | YES (shadow sample) |
| APPLY | **NO** |
| SAME | 3 (sample) |
| HIGH-CONF WRONG | 0 |

---

## J. PERSISTENCE

| | |
|--|--|
| FILESYSTEM | **PASS** (`data/ai-demand-positioning/published/adp_w_rome/`) |
| LIVE OVERLAY | NOT_RUN (`ADP_AIRTABLE_BASE_ID` unset — history/overlay policy allows FS primary) |
| WRONG-BASE | **0** |
| CROSS-HOTEL BLEED | **0** |

---

## K. CUSTOMER EXPERIENCE

| Surface | Status |
|---------|--------|
| Dashboard payload | PASS (published report) |
| Detail / evidence | PASS |
| Actions | PASS (present) |
| Report | PASS |
| Share | NOT_RUN this cycle |
| Bleed scan | **PASS** (0 Bethesda/NIH/DMV/Renaissance/Times Square/Manhattan) |

---

## L. QUALITY VS REFERENCES

| Dimension | Bethesda | Renaissance | W Rome |
|-----------|----------|-------------|--------|
| Evidence | Mature | Mature republish | Fresh 80/80 providers |
| Source quality | Strong | Strong | Official + provider mix |
| Root cause | Strong | Strong | Partial (gap) |
| Actions | Strong | Strong | Present / thinner |
| Completion criteria | Strong | Strong | Partial (gap) |
| Peer/substitution | Certified | Certified | Certified ADEQUATE |
| Persistence | FS+overlay | FS republish | FS snapshot |
| UI completeness | Mature | Mature | Payload ready; share not re-run |

Do not rank hotels.

---

## M. MANUAL INTERVENTIONS

| TOTAL | **6** |
|-------|------:|
| LEGITIMATE STEWARDSHIP | **5** |
| OPERATIONAL | **1** |
| AUTOMATION DEBT | **0** this cycle |

| Step | Category |
|------|----------|
| Review original 5 peers | LEGITIMATE_STEWARDSHIP |
| Exclude rommc + JW | LEGITIMATE_STEWARDSHIP |
| Approve St Regis / Eden / de Russie | LEGITIMATE_STEWARDSHIP |
| Fill Westin + EDITION | LEGITIMATE_STEWARDSHIP |
| Certify scenarios KEEP 20 | LEGITIMATE_STEWARDSHIP |
| Authorize live baseline | OPERATIONAL |

---

## N. GENERALIZATION

| | |
|--|--|
| W ROME PROD HARDCODES | **0** (instance peer/CORE data only) |
| ROME / ITALY PROD HARDCODES | **0** |
| GENERIC ADP FIXES | **0** this cycle (prior generic scenario fallback reused) |

---

## O. DECISION

1. Peers adequately evaluated? **YES**  
2. Approved: St Regis (CORE), Eden (SEC), de Russie (SEC); fills Westin + EDITION (CORE). Excluded rommc + JW.  
3. Adequacy? **PASS**  
4. Scenarios? **PASS**  
5. Gate cleared? **YES**  
6. Real ADP baseline? **YES** (80/80, not Renaissance reuse)  
7. Evidence quality acceptable? **YES**  
8. Root/actions/completion complete? **PARTIAL** (minor gap)  
9. Peer pack improved substitution? **YES**  
10. Jev measurable value? **Limited** (shadow only)  
11. Isolation clean? **YES**  
12. Manual interventions? **6**  
13. Mostly legitimate stewardship? **YES** (5/6)  
14. Fully replicated ADP+GDI? **YES with minor ADP narrative-field gaps**  
15. Hotel #4 ready? **GDI+ADP path ready**; peer stewardship still required per hotel  

---

## PERSISTENCE / GENERALIZATION

| | |
|--|--|
| CODE | peer-set + CORE governance + `w-rome-baseline-period-001-v1.js` + runner + tests |
| DATA/PACK | `w-rome-property-profile.json`, published `adp_w_rome/*` |
| W/ROME/ITALY PROD LOGIC | **NO** |
| JEV PROD CHANGED | **NO** |
| PUSH | see commit |

---

## STOP
