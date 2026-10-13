# GDI Checkpoint + Contact Intelligence V1.2 — Founder Report

**Date:** 2026-09-24  
**Branch:** `deploy/gdi-pe-v1-7-customer-closure`

---

## A. GIT CHECKPOINT (Phase 1)

BRANCH: `deploy/gdi-pe-v1-7-customer-closure`

COMMITS:
- `4cf8714` — feat(gdi): recover official contact source paths with Jev shadow routing
- `69fd4bd` — fix(gdi): normalize detail drawer loading to platform wave toast

PUSH: **PASS** → `origin/deploy/gdi-pe-v1-7-customer-closure` (`15151a7..69fd4bd`)

REMOTE: `https://github.com/joandejarden-create/my-operators-backend.git`

REGRESSIONS: **PASS** (GDI / New Opps / DG / PE / Coverage / Weekly / Yield / CQ / Contact V1+V1.1 / Jev / Share / CSV / UI)

KNOWN PRE-EXISTING LEFT DIRTY:
- `api/market-alerts*.js`, `lib/market-alerts-*.js`, `public/market-alerts.*`
- `rail-explore`, `shorecast-github-repo`
- hotel-ownership / artifacts / unrelated untracked trees

SECRETS COMMITTED: **NO**

---

## B. CONTACT GAP BEFORE (Phase 2 truth)

Weekly run claimed **NO_CONTACT: 10**, but **7/10 were DISQUALIFIED** (must not deep-research).

Live actionable before V1.2 canary:

| Tier | Count |
|------|------:|
| NAMED_DIRECT | 4 |
| NAMED_PARTIAL | 9 |
| FUNCTIONAL | 9 |
| ORGANIZATION_PATH | 11 |
| GENERIC_ONLY | 0 |
| NO_CONTACT | **1** |

BY REASON (weekly-era 10 audit):

| Reason | N |
|--------|--:|
| LOW_VALUE_NO_DEEP_RESEARCH (disqualified) | 7 |
| ACCOUNT_LEVEL_EVIDENCE_REQUIRED (Marriott HQ watch) | 1 |
| PROGRAM_OWNER_NOT_OBSERVABLE (UMD — already ORG_PATH) | 1 |
| EVENT_OWNER_NOT_OBSERVABLE (Woman's Club — already FUNCTIONAL) | 1 |

---

## C. OFFICIAL SOURCE RECOVERY

OFFICIAL DOMAINS RESOLVED: mostly already known (network queries **0** this pass)  
OFFICIAL SOURCE PATHS FOUND / reused: **12** weak actionable attempted  
STAFF / EVENT PAGES FETCHED: **32** additional fetches

---

## D. CONTACT COVERAGE AFTER

| Tier | Count |
|------|------:|
| NAMED_DIRECT | 4 |
| NAMED_PARTIAL | **11** |
| FUNCTIONAL | **10** |
| ORGANIZATION_PATH | **8** |
| GENERIC_ONLY | 0 |
| NO_CONTACT | **1** |

---

## E. UPGRADES

NO_CONTACT → NAMED: **0**  
NO_CONTACT → FUNCTIONAL: **0**  
NO_CONTACT → ORG_PATH: **0**  
STILL NO_CONTACT: **1** (Marriott HQ account watch — soft-stopped)  
ORG_PATH → FUNCTIONAL/NAMED: **3**
- AAD 2028 → FUNCTIONAL (`registration@aad.org`)
- BEBPA USB 2027 → NAMED_PARTIAL (Karen Bertani)
- Alexandria Soccer Kickoff → NAMED_PARTIAL (Ben Hawkins)

---

## F. SOURCE REUSE

CONTACT CLUES FROM ALREADY-FETCHED / KNOWN OFFICIAL URLS: primary path for most ORG_PATH  
ADDITIONAL CONTACT FETCHES: **32**  
DOMAIN QUERIES: **0** (domains already on record)

---

## G. JEV SHADOW

CALLS: **55**  
TECHNICAL FALLBACKS: **0**  
POLICY FALLBACKS: **10**  
HIGH-CONF WRONG: **0**

---

## H. JEV OUTCOME

CONTACT_SOURCE_PATH: mostly **same** with GDI  
CONTACT_FOLLOWUP_TYPE: mostly **same**  
FUNCTIONAL_PATH_SUFFICIENT: agreement high on WEAK  
STOP_CONTACT_RESEARCH: occasional Jev `STOP_LOW_VALUE` vs GDI `CONTINUE` — **not proven better on yield**; keep shadow

---

## I. SURFE

AUTO CALLS: **0**  
PII PERSISTED: **0** (strip now also clears `contactDataOrigin=SURFE`)  
ON-DEMAND: **PASS**

---

## J. AIRTABLE

PUBLIC CONTACT UPDATES: **12** (research outcomes + upgraded paths)  
PUBLIC EMAILS: **7**  
PUBLIC PHONES: **1**  
UNRESOLVED REASONS PERSISTED: **9**  
RAW PROVIDER/JEV PAYLOADS: **0**

---

## K. EFFICIENCY

TOTAL QUERIES: **0**  
TOTAL FETCHES: **32**  
CONTACTS IMPROVED: **3**  
FETCHES PER IMPROVEMENT: **~10.7**

---

## L. DECISION

1. Remaining “10 blanks” were mostly **disqualified noise**, not actionable public gaps.  
2. **3** recoverable via better official follow-up on ORG_PATH cases.  
3. **1** genuine public-data ceiling (Marriott HQ category watch) + several ORG_PATH where named owner is not public.  
4. Source reuse still useful when official URLs exist.  
5. Named pursuit after strong FUNCTIONAL: **low value** — keep FUNCTIONAL.  
6. Jev routing: **mostly agrees**; no clear efficiency win yet.  
7. Jev stop/continue: mixed; not apply-ready.  
8. No Jev contact decision ready for controlled apply.  
9. Provider reveal still needed for NAMED_PARTIAL HOW + Marriott account path.  
10. Contact coverage is **sufficient for normal weekly GDI** (skip DQ; soft-stop account watches).

---

## M. FINAL VERDICT

**CONTACT COVERAGE IMPROVED — JEV REMAINS SHADOW**

---

## PERSISTENCE / GENERALIZATION

CODE FILES CHANGED (Phase 2):
- `lib/group-demand-intelligence/contact-gap-classify-v1-2.js`
- `lib/group-demand-intelligence/contact-intelligence-v1-2.js`
- `lib/group-demand-intelligence/weekly-contact-resolution-v1-2.js`
- `lib/group-demand-intelligence/contact-tiers-v1-2.js`
- `scripts/gdi-contact-intelligence-v1-2-gap-canary.mjs`
- `scripts/test-gdi-contact-intelligence-v1-2.mjs`
- `package.json`

FIXTURES: none required  
TESTS: `test:gdi-contact-intelligence-v1-2` (+ V1/V1.1/weekly regressions)  
HOTEL-SPECIFIC LOGIC: **NO**  
HARD-CODED PEOPLE: **NO**  
HARD-CODED DOMAINS: **NO**
