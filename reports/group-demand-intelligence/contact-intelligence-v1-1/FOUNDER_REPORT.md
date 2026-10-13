# GDI Contact Intelligence V1.1 — Founder Report

**Mode:** B — Official source path recovery + Jev shadow routing  
**Hotel canary:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Apply artifact:** `reports/group-demand-intelligence/contact-intelligence-v1-1/SOURCE_RECOVERY_CANARY_APPLY_1790278442501.json`  
**Generated:** 2026-09-24

---

## N. FINAL VERDICT

**CONTACT COVERAGE IMPROVED — JEV REMAINS SHADOW**

Official domain/path recovery cleared one blank (UMD → alumni.umd.edu), upgraded Potomac Memorial to a named tournament director, and attached official paths across weak org-path rows. Jev contact routing ran in shadow (100 calls) with mostly agreement to GDI — **not** ready to drive production fetches. Surfe stayed at 0 auto / 0 PII.

One false lead (Marriott Vacations “Grand Residences” brand bleed) was caught, repaired, and blocked by host filters.

---

## A. CONTACT COVERAGE BEFORE

| Tier | Count |
|------|------:|
| NAMED_DIRECT | 4 |
| NAMED_PARTIAL | 9 |
| FUNCTIONAL | 10 |
| ORGANIZATION_PATH | 10 |
| GENERIC_ONLY | 0 |
| NO_CONTACT | 2 |

---

## B. OFFICIAL SOURCE RECOVERY

| Metric | Count |
|--------|------:|
| MISSING OFFICIAL DOMAINS (start of cohort) | 2+ thin watches |
| DOMAINS RESOLVED | 14 |
| OFFICIAL SOURCE PATHS ADDED | 14 |

Quality gates added: reject privacy/legal URLs; reject hotel-brand hosts on “near/adjacent HQ” watches.

---

## C. CONTACT COVERAGE AFTER (post false-positive repair)

| Tier | Count |
|------|------:|
| NAMED_DIRECT | 4 |
| NAMED_PARTIAL | **10** |
| FUNCTIONAL | **9** |
| ORGANIZATION_PATH | **11** |
| GENERIC_ONLY | 0 |
| NO_CONTACT | **1** |

---

## D. UPGRADES

| Change | Count |
|--------|------:|
| NO_CONTACT → USABLE | **1** (UMD → ORGANIZATION_PATH via alumni.umd.edu) |
| ORG_PATH → FUNCTIONAL | 0 |
| FUNCTIONAL → NAMED | **1** (Potomac Memorial → Kathy Hauschild, Tournament Director) |
| NAMED_PARTIAL → DIRECT | 0 |
| Marriott HQ watch | Remains **NO_CONTACT** after bleed repair (correct) |

---

## E. SOURCE YIELD

| Source Type | Pages | Named | Functional | Email | Phone | Upgrades* |
|-------------|------:|------:|-----------:|------:|------:|----------:|
| EVENT_PROGRAM_PAGE | 12 | 1 | 2 | 3 | 0 | 9 |
| GENERAL_OFFICIAL | 9 | 0 | 0 | 0 | 0 | 6 |
| HOUSING | 4 | 0 | 2 | 2 | 0 | 0 |
| OFFICIAL_CONTACT_PAGE | 2 | 2 | 0 | 2 | 0 | 2 |
| OFFICIAL_STAFF | 1 | 0 | 0 | 0 | 0 | 1 |

\*Includes official-path attachment upgrades (not only tier jumps).

Best yield: **event/program pages** + **official contact pages** for named upgrades; housing pages reinforce functional paths.

---

## F. REMAINING GAPS

| Opportunity | Tier | Official Domain? | Research attempted | Why unresolved | Best next step |
|-------------|------|------------------|--------------------|----------------|----------------|
| Marriott HQ adjacent corporate watch | NO_CONTACT | No (brand bleed rejected) | Network domain + filters | Account-level category watch; no public organizer domain | Keep watch; Surfe only after a real corporate account is named |
| AMWA 2027/2028, AAD 2028, WBC 2028, etc. | ORGANIZATION_PATH | Yes (paths added) | Staff/event follow-ups | No public named meetings owner on fetched pages | Accept ORG/functional; re-check when prospectus lists staff |
| Georgetown Homecoming | ORGANIZATION_PATH | homecoming.georgetown.edu | Alumni/events paths | No named advancement contact on page | Advancement events office follow-up later |
| NIH SBPO pattern | ORGANIZATION_PATH | Thin / SAM listing | Bounded | Recurring pattern, not a dated event owner | Functional SBPO path when published |

---

## G. NEW OPPORTUNITY CONTACTS

| Opportunity | Before → After |
|-------------|----------------|
| Woman's Club | FUNCTIONAL → FUNCTIONAL (official domain confirmed; no stronger named owner) |
| NRC RIC | FUNCTIONAL → FUNCTIONAL (hotel-information path retained) |
| ACCP | FUNCTIONAL → FUNCTIONAL (travel/venue path retained) |
| ACVNU | FUNCTIONAL → FUNCTIONAL (housing path retained) |

**Do not downgrade** — functional paths remain valid Grade C sales paths.

---

## H. JEV SHADOW

| Metric | Value |
|--------|------:|
| CALLS | 100 |
| TECH FALLBACKS | 0 |
| POLICY FALLBACKS | 28 |
| HIGH-CONF WRONG | 2 |

Shadow only — **production route = GDI deterministic routing**.

---

## I. JEV ROUTING OUTCOME

| Decision type | N | Jev better | GDI better | Same | Unknown | High-conf wrong |
|---------------|--:|-----------:|-----------:|-----:|--------:|----------------:|
| CONTACT_SOURCE_PATH | 20 | 0 | 0 | 11 | 7 | 2 |
| CONTACT_FOLLOWUP_TYPE | 20 | 0 | 0 | 18 | 2 | 0 |

NAMED_PERSON_WORTH / FUNCTIONAL_SUFFICIENT / STOP: recorded in shadow audit; production stop rules unchanged (strong functional → stop).

---

## J. EFFICIENCY

| Metric | Count |
|--------|------:|
| CURRENT CONTACT FETCHES | ~bounded cohort fetches (≤4/opp) |
| JEV-ROUTED SIMULATED FETCHES | same (shadow did not divert) |
| FETCHES SAVED | **0** |
| NAMED CONTACTS LOST | **0** |

Jev did **not** demonstrably reduce unnecessary research in this cycle.

---

## K. SURFE

| Check | Result |
|-------|--------|
| AUTO CALLS | **0** |
| ON-DEMAND | **PASS** |
| PII PERSISTED | **0** |

---

## L. AIRTABLE / CANONICAL

| Check | Result |
|-------|--------|
| PUBLIC CONTACT UPDATES | ~11 improved rows applied (+ Marriott bleed repair) |
| PUBLIC EMAILS STORED | Potomac tournament email path (public) |
| PUBLIC PHONES STORED | 0 |
| JEV AUDIT | Minimal shadow decisions attached on apply (`jevContactShadow`) |
| RAW PROVIDER/JEV PAYLOADS | **0** |

---

## M. DECISION ANSWERS

1. **Were blanks mainly missing official paths?** Yes for UMD; Marriott is account-level (no public organizer), not an extraction miss.
2. **Did source recovery improve coverage?** Yes — NO_CONTACT 2→1; NAMED_PARTIAL 9→10; official paths attached widely.
3. **Best source types?** Event/program pages and official contact pages.
4. **Four new opps stronger?** No named upgrade; functional paths confirmed sufficient.
5. **Remaining FUNCTIONAL acceptable?** Yes — do not force named.
6. **Does Jev improve routing efficiency?** Not yet — high agreement with GDI, 0 fetches saved, 2 high-conf wrong.
7. **Jev ready for controlled apply?** **No** — keep shadow.
8. **Where provider reveal still needed?** NAMED_PARTIAL without public email/phone (Get Contact Details).

---

## Persistence / generalization

| Item | Value |
|------|-------|
| CODE FILES CHANGED | `lib/group-demand-intelligence/contact-source-recovery-v1-1.js`, `contact-jev-shadow-v1-1.js`, `jev/jev-types.js`, `jev/jev-decision-service.js`, `scripts/gdi-contact-intelligence-v1-1-source-recovery-canary.mjs`, `scripts/test-gdi-contact-intelligence-v1-1.mjs`, `package.json` |
| FIXTURES | Inline HTML + routing fixtures in V1.1 test |
| TESTS | `npm run test:gdi-contact-intelligence-v1-1` (14 PASS); V1 + weekly + Jev + new-opps + DG + PE + coverage + CQ + share + CSV PASS |
| HOTEL-SPECIFIC LOGIC | NO |
| HARD-CODED PEOPLE | NO |
| HARD-CODED DOMAINS | NO (brand-bleed *pattern* filter only) |
| GIT SHA | `15151a7` (pre-commit; working tree dirty with V1.1) |
| WORKING TREE CLEAN | NO |

---

## STOP
