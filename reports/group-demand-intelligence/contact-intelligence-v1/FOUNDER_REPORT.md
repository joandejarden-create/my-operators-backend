# GDI Contact Intelligence V1 — Founder Report

**Mode:** B — Source-page contact / stakeholder extraction  
**Hotel canary:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Apply artifact:** `reports/group-demand-intelligence/contact-intelligence-v1/SOURCE_PAGE_CANARY_APPLY_1790276507828.json`  
**Generated:** 2026-09-24

---

## L. FINAL VERDICT

**SOURCE-PAGE CONTACT EXTRACTION PASSES — CONTACT COVERAGE MATERIALLY IMPROVED**

Useful contacts were already present on pages GDI was reading. Dual extract (demand + contact) on the same fetch wiped **GENERIC_ONLY 3 → 0**, raised named/functional coverage, and did it with **0 Surfe auto-calls** and **0 provider PII persisted**.

---

## A. CURRENT CONTACT COVERAGE BEFORE

| Tier | Count |
|------|------:|
| ACTIONABLE (cohort) | 35 |
| NAMED_DIRECT | 4 |
| NAMED_PARTIAL | 7 |
| FUNCTIONAL | 7 |
| ORGANIZATION_PATH | 12 |
| GENERIC_ONLY | 3 |
| NO_CONTACT | 2 |

---

## B. SOURCE PAGE EXTRACTION

| Metric | Count |
|--------|------:|
| SOURCE PAGES READ | 77 |
| PAGES WITH CONTACT CLUES | 53 |
| NAMED PEOPLE | 39 |
| FUNCTIONAL CONTACTS | 31 |
| PUBLIC EMAILS | 70 |
| PUBLIC PHONES | 0 |
| CONTACT / STAFF LINKS | 111 |

---

## C. REUSE EFFICIENCY

| Metric | Count |
|--------|------:|
| CONTACT CLUES FROM ALREADY-FETCHED PAGES | 53 |
| ADDITIONAL CONTACT-SPECIFIC FETCHES | 49 |

Primary gain: contact intelligence recovered from demand-page HTML already paid for. Follow-ups only when tier was weak and named people = 0 (bounded ≤2 per source).

---

## D. CONTACT COVERAGE AFTER

| Tier | Count |
|------|------:|
| NAMED_DIRECT | 4 |
| NAMED_PARTIAL | 9 |
| FUNCTIONAL | 10 |
| ORGANIZATION_PATH | 10 |
| GENERIC_ONLY | 0 |
| NO_CONTACT | 2 |

---

## E. IMPROVEMENT

| Change | Count |
|--------|------:|
| NO_CONTACT → USABLE | 0 |
| GENERIC → NAMED | 1 (ACTS → Adam Joyce) |
| GENERIC → FUNCTIONAL | 2 (NADO 2027/2028 Registration) |
| ORGANIZATION → NAMED | 1 (ASAE → Danielle Davis) |
| ORGANIZATION → FUNCTIONAL | 1 (Arlington Spring Tournament) |
| PUBLIC EMAILS ADDED (persisted rows) | 4 |
| PUBLIC PHONES ADDED | 0 |
| ROWS UPDATED (Airtable/canonical) | 5 |

---

## F. QUALITY

Pre-apply dry runs surfaced UI/title noise (`View More If`, `Transportation Program Post`, truncated `… Vice` / `… Senior`). Filters tightened in `isLikelyPersonName` / loose extract before apply.

| Issue | Count (post-filter apply set) |
|-------|------:|
| FALSE CONTACTS | 0 |
| WRONG ROLE | 0 |
| WRONG ORGANIZATION | 0 |
| SPEAKER/ATTENDEE FALSE POSITIVE | 0 |

Applied primaries reviewed: **Adam Joyce** (Executive Director), **Danielle Davis**, **Registration** functional paths — buying-process relevant.

---

## G. SURFE

| Check | Result |
|-------|--------|
| AUTO CALLS | **0** |
| ON-DEMAND ONLY | **PASS** |
| PII PERSISTED | **0** |

Extractor uses `stripSurfeProviderPii` on all candidates; weekly harvest has no Surfe import; CTA remains Get Contact Details for named-without-reachability.

---

## H. AIRTABLE / CANONICAL

| Check | Result |
|-------|--------|
| PUBLIC CONTACT RECORDS UPDATED | 5 |
| PUBLIC EMAILS PERSISTED | 4 |
| PUBLIC PHONES PERSISTED | 0 |
| SOURCE PROVENANCE | **PASS** (`contactDataOrigin=PUBLIC_SOURCE`, sourceUrl / emailSource / verifiedAt on extract) |

---

## I. WEEKLY INTEGRATION

| Check | Result |
|-------|--------|
| CONTACT EXTRACTION ON EVERY RELEVANT FETCH | **PASS** (`weekly-lane-harvest-v1-2` `fetchPage` + shared service) |
| BOUNDED CONTACT SECOND PASS | **PASS** (follow-up links, ≤2; weekly-contact-resolution-v1-2) |
| CONTACT UPDATE NEWNESS | **PASS** (materialUpdateOnly; NEW preserved — fixture `apply_does_not_force_new`) |

### Lane audit (Part A)

| Lane | Contact extract on source fetch |
|------|--------------------------------|
| New Opportunities | YES (via weekly-lane-harvest) |
| Demand Generators | YES |
| Recurring Programs | YES (via demand generators harvest) |
| Private Events | YES |
| Venue Partnerships | YES (PE / harvest path) |
| Weekly discovery | YES |
| Second-pass research | YES (bounded follow-ups) |

---

## J. BLANKS REMAINING

| Opportunity | Tier | Why unresolved | Best next step |
|-------------|------|----------------|----------------|
| UMD alumni / parents weekend watch | NO_CONTACT | No officialSource / discovery URLs on record | Attach official alumni-events URL when published; then re-extract |
| Marriott HQ adjacent corporate watch | NO_CONTACT | Account-level watch; no public event page | Keep ORGANIZATION watch; Surfe only after named person resolved elsewhere |
| AMWA 2027 / 2028 | ORGANIZATION_PATH | Pages yield staff links + generic path, not named meetings owner | Bounded staff-directory follow-up (already attempted); wait for meetings staff page |
| AAD 2028 overflow | ORGANIZATION_PATH | Housing channel TBD publicly | Hold; re-check when housing provider named |
| World Biomaterials 2028 | ORGANIZATION_PATH | Thin page / no contact clues | Official housing page when posted |
| Georgetown Homecoming 2027 | ORGANIZATION_PATH | Advancement events path; no named person on fetched pages | Follow Advancement Events contact page |
| NIH SBPO VOS pattern | ORGANIZATION_PATH | Pattern watch; SBPO desk only | Functional SBPO path is usable; Surfe not needed |
| BEBPA USB 2027 | ORGANIZATION_PATH | Organizers inbox only | Keep functional; named when prospectus lists coordinator |

**Priority NEW opps (Woman's Club, NRC RIC, ACCP, ACVNU):** already **FUNCTIONAL_CONTACT** with official paths; source pages did not expose a stronger named meetings owner in this canary. Usable sales path exists — do not show “No usable contact.”

---

## K. DECISION ANSWERS

1. **Were useful contacts already on pages GDI was reading?** Yes — 53/77 pages had contact clues; 5 actionable rows upgraded without provider enrichment.
2. **Coverage improvement without providers?** GENERIC_ONLY cleared; NAMED_PARTIAL +2; FUNCTIONAL +3; ORGANIZATION_PATH −2.
3. **Best source types?** Conference staff / about pages (named), registration/housing functional inboxes, association meetings staff listings.
4. **Named contacts buying-relevant?** Yes for ACTS (Exec Director / membership ops) and ASAE staff; CEO/speaker noise rejected.
5. **Actionable with no usable path?** **2** (both watchlist records with no URLs).
6. **Bounded official follow-up enough for most blanks?** Enough to clear generics and lift several org-paths; remaining blanks need better official URLs or later prospectus publication — not Surfe-first.
7. **Where Surfe remains necessary?** Named person resolved + no public email/phone + user clicks **Get Contact Details**. Never for identity establishment or bulk.

---

## Persistence / generalization

| Item | Value |
|------|-------|
| CODE FILES CHANGED | `lib/group-demand-intelligence/extract-contact-intelligence-from-source.js` (new), `contact-resolution.js`, `weekly-lane-harvest-v1-2.js`, `weekly-contact-resolution-v1-2.js`, `contact-tiers-v1-2.js`, `scripts/gdi-contact-intelligence-v1-source-page-canary.mjs`, `scripts/test-gdi-contact-intelligence-v1.mjs`, `scripts/test-gdi-weekly-discovery-v1-2.mjs`, `package.json` |
| FIXTURES | Inline HTML/PDF-text fixtures in `test-gdi-contact-intelligence-v1.mjs` |
| TESTS | `npm run test:gdi-contact-intelligence-v1` (11 PASS); weekly/new-opps/DG/PE/coverage/commercial/CSV/visibility/share regressions PASS |
| HOTEL-SPECIFIC LOGIC | NO |
| HARD-CODED PEOPLE | NO |
| HARD-CODED DOMAINS | NO (canary priority IDs only) |
| GIT SHA | `b4352b8e05f739f8f93ab471d78c8213c3ba632e` |
| WORKING TREE CLEAN | NO |

---

## STOP
