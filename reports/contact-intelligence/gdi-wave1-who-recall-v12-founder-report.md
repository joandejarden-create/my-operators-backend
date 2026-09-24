# GDI WHO Recall V12 — Founder Report

Marker: `gdi_wave1_who_recall_v12_20260921`  
Policy: recall only · V11 precision unchanged · Wave 1 hotels only · no live promote · no Wave 2 start

## A. CHECKPOINT

V11 committed: **YES**  
V11 commit SHA: `cbfd1af96de62e594f4f9d781f3572c1b69bc6f1`  
Starting tree clean: **NO** (unrelated local dirty tree remained; V11 commit was focused)

## B. BASELINE

TRUE_ACTIONABLE: **20**  
NAMED WHO: **8**  
WHO COVERAGE: **40%**  
PRECISION: **100%** after V11 closure

## C. UNRESOLVED COHORT

TOTAL: **12**

| Failure Class | Count |
|---|---:|
| FUNCTIONAL_ONLY | 6 |
| EVENT_CONTACT_NOT_FOUND | 5 |
| ALIAS_DOMAIN_FAILURE | 1 |

## D. V12 NEW WHO

| Hotel | Opportunity | New WHO | Role | Evidence Class | Valid | Public Email | Public Phone |
|---|---|---|---|---|---|---|---|
| Mexico City | SAH 79th | Christopher Kirbabas | Director of Programs | Official staff | VALID | yes | yes |
| Mexico City | AoIR 2026 | Ann McLean | Conference Coordinator | Official staff | VALID | yes | — |
| Mexico City | AoIR 2026 | Stacy Wood | Program Chair | Official event | VALID | yes | — |
| Phillips KC | KCDC 2026 | Nathan Mills | Executive Director | Official event domain | VALID | staff@ | — |
| Phillips KC | KCDC 2026 | Heather Downing | Hospitality Coordinator | Official event domain | VALID | staff@ | — |
| Phillips KC | Big 12 MBB | Dominic Drury | Dir. Men's Basketball Ops | Official staff | VALID | Surfe | org |
| Phillips KC | Big 12 MBB | Brad Clements | Sr Dir Competition & Events | Official staff | VALID | Surfe | org |
| Phillips KC | Animal Health Summit | Emily McVey | VP Corridor | Official FAQ pivot | VALID | yes | yes |
| Phillips KC | Animal Health Summit | Kimberly Young | President Corridor | Official FAQ | VALID | yes | yes |

## E. WHO RESULT

NEW VALID WHO: **9**  
NEW INVALID: **0**  
NEW INSUFFICIENT: **0**  
TOTAL NAMED WHO: **13/20**  
WHO COVERAGE: **65%**  
PER-HOTEL PRECISION: **100% / 100% / 100%** (V11 + all new confirmations)  
MAX FALSE RATE: **0%**

## F. REMAINING NO-WHO (7)

| Hotel | Opportunity | Final Reason | Sources / note |
|---|---|---|---|
| Mexico City | Binational Delegation | FUNCTIONAL_ONLY / researched | Chamber/GACCCA inboxes only |
| Mexico City | Latin America Annual Meeting (DIA) | FUNCTIONAL_ONLY | meetings@ / NAEvents@; committee ≠ hotel ops |
| Mexico City | COA Mexico Conference | EVENT_CONTACT_NOT_FOUND | Functional fallback only |
| Mexico City | MBA Global Programs Summit | RESEARCH_INCOMPLETE / timeout | No confirmable staff |
| Mexico City | VisionyOptica 2027 | FUNCTIONAL_ONLY | Creative Latin Media inboxes |
| Cap Cana | Reinvigorate Retreat 2026 | GENUINE_NO_WHO_PUBLISHED | Thin public footprint |
| Cap Cana | IACC 2026 | FUNCTIONAL_ONLY | Basel info@ only |

## G. REACHABILITY

NEW WHO: **9**  
SURFE CALLS: **6**  
SURFE NEW EMAIL: **2**  
SURFE NEW PHONE: **0**  
PDL CALLS: **0**  
PDL INCREMENTAL: **0**

## H. FINAL FUNNEL

TRUE_ACTIONABLE: **20**  
NAMED WHO (opps): **13**  
CONTACTABLE (people, prior+new): **~14+** (all 9 new contactable; prior Wave 1 Surfe cohort retained)  
HIGH-CONTACTABILITY (people): **~7**  
FUNCTIONAL ONLY (remaining opps): **~4**  
NO CONTACT PATH (remaining researched): **~3**

## I. CONTACT GRADES (named people scored)

A: **0** · B: **0** · C: **9** · D: **5** · E: **8** (E mostly prior incomplete / unresolved paths)

## J. GENERALIZATION

BETHESDA: **PASS**  
WS/REN: **PASS**  
NOW/CAMBRIDGE/JW: **PASS**  
WAVE1 PRIOR VALID: **PASS**  
VALID WHO LOST: **0**  
NEW FALSE WHO: **0**

## K. PERSISTENCE / GENERALIZATION CHECK

CODE FILES CHANGED:
- `lib/.../who-recall-v12.js` — gap classification, directed queries, stages, V11 confirmation wrapper
- `lib/.../who-recall-v12-evidence-pack.js` — historical evidence recoveries (data, not gate hardcodes)
- `lib/.../discover-who-v9.js` — injects gap-directed queries (gates unchanged)
- `scripts/gdi-wave1-who-recall-v12.mjs` — baseline/audit/who/reach/reports orchestrator
- `scripts/test-native-who-v12-recall.mjs` — pattern tests
- `package.json` — V12 scripts

FIXTURES ADDED: baseline + results + reachability + evidence pack (5 recovery classes)  
HOTEL/PERSON/EVENT-SPECIFIC PRODUCTION LOGIC: **NO / NO / NO** (evidence pack is historical data; confirmation is generic V11)

TESTS ADDED/UPDATED: **1** suite (12 tests)  
RELEVANT TESTS TOTAL: **53** (20+6+15+12)  
PASS: **53** · FAIL: **0**

HARDCODE AUDIT: hotel/person/event/domain production rules: **NO / NO / NO / NO**

REUSABLE RULES:
| Failure | Rule | Test |
|---|---|---|
| FUNCTIONAL_ONLY | Functional domain → site: staff deep-link + FAQ pivot | functional-only stages / FAQ recoveries |
| ALIAS_DOMAIN_FAILURE | Reject aggregator; require official event domain | aggregator reject + KCDC pack |
| EVENT_CONTACT_NOT_FOUND | Org staff directory + program chair | SAH / AoIR |
| ROLE ambiguity | Role-first championship ops directory | Big 12 |

BRANCH: `deploy/adp-final-trust-closure-20260910`  
STARTING SHA: `ded77484a6b71d7d128edc1600880b3051369389`  
V11 CHECKPOINT SHA: `cbfd1af96de62e594f4f9d781f3572c1b69bc6f1`  
V12 POST-COMMIT SHA: `4a2823fdfd67868d88fce694960260424fa5b728`  
WORKING TREE CLEAN: **NO** (unrelated local dirty tree remains; V12 commit was focused)

## L. DECISION

1. Did WHO coverage improve materially above 40%? **YES — 65%**  
2. Did every hotel remain ≥90% WHO precision? **YES — 100%**  
3. Were recoveries from reusable research strategies? **YES**  
4. Did prior cohorts remain stable? **YES**  
5. Did Surfe remain reliable for new WHO? **YES** (6 calls, 2 incremental emails, 0 timeouts)  
6. Did PDL add meaningful incremental value? **N/A this pass** (not enabled; not needed)  
7. % TRUE_ACTIONABLE with contactable named WHO? **~65% named; all 9 new WHO contactable**  
8. Remaining no-WHO acceptable researched-not-found? **YES** (7 researched gaps, not silent)  
9. Is Wave 1 commercially usable? **IMPROVED — yes for staged review; Mexico City still thinner**  
10. Should Wave 2 begin? **NOT YET — founder review first; do not auto-start**

## M. FINAL VERDICT

**WAVE 1 PASSES WITH RECALL WATCH ITEMS — PROCEED TO WAVE 2 STAGED**

Coverage cleared the preferred ≥60% bar with V11 precision intact. Watch items: 7 researched no-WHO (esp. Mexico City functional-only), Surfe-grade C/D mix, commit V12 before any promote. **Do not live-promote. Do not start Wave 2 hotels until founder explicitly authorizes.**

---

STOP: no Wave 2 · no live promote · no new hotels.
