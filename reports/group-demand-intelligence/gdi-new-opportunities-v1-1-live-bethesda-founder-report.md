# GDI New Opportunities V1.1 — Live Bethesda Demand-Lane Audit

**Generated:** 2026-09-23  
**Mode:** LIVE SerpAPI + fetch + extract · dry-run qualify · **NO live write · NO deploy**  
**Hotel:** Bethesda Marriott `recLuxvwwxID7U2B8`  
**Baseline:** 38 canonical opportunities (untouched)

---

## PART A — FREEZE

| Item | Value |
|------|--------|
| BRANCH | `deploy/adp-final-trust-closure-20260910` |
| BASE SHA | `bf532f9` |
| WORKING TREE | Dirty (New Opp V1 + V1.1 audit script; not committed) |

**FILES CHANGED (preserved V1 + V1.1 audit):**
- `lib/group-demand-intelligence/demand-signal-types.js`
- `lib/group-demand-intelligence/discovery-hygiene-v3.js`
- `lib/group-demand-intelligence/discovery-recall-v4.js`
- `lib/group-demand-intelligence/weekly-delta.js`
- `lib/group-demand-intelligence/opportunity-list-dto.js`
- `lib/group-demand-intelligence/index.js`
- `lib/group-demand-intelligence/contact-candidate/ontology.js`
- `public/js/group-demand-intelligence/dealality-gdi-ui.js`
- `scripts/test-gdi-new-opportunities-v1.mjs`
- `scripts/gdi-new-opportunities-v1-offline-canary.mjs`
- `scripts/gdi-new-opportunities-v1-1-live-bethesda-audit.mjs` **(new)**
- `package.json`
- fixtures + offline reports

Architecture was **not** rewritten for this audit (only hygiene reprocess bugfix: use `hygiene.rows`).

Artifacts: `reports/group-demand-intelligence/gdi-new-opportunities-v1-1-live-bethesda-audit/`

---

## A. LIVE RESEARCH

| Metric | Count |
|--------|-------|
| TOTAL QUERIES | **30** (15 lanes × 2) |
| TOTAL URLS | **267** |
| OFFICIAL URLS (candidate-attested .gov/.edu/.org) | low / not reliably stamped on extract |
| TOTAL CANDIDATES | **32** raw → **31** after dedupe |
| QUALIFIED (hygiene-scored) | **31** |
| TRUE_ACTIONABLE | **0** |
| VALID_WATCH | **9** |
| VALID_FUTURE | **2** |
| INSUFFICIENT | **1** |
| INVALID | **19** (all `PAST_EVENT`) |

Fixtures were **not** counted toward live metrics.

---

## B. LIVE MORE AT-BATS

| Metric | Count |
|--------|-------|
| EVENT-ONLY NEW (any state) | 14 |
| EXPANDED-DEMAND NEW (any state) | 17 |
| OVERLAP | 0 |
| EVENT-ONLY NEW **TRUE_ACTIONABLE** | **0** |
| EXPANDED-DEMAND NEW **TRUE_ACTIONABLE** | **0** |
| **INCREMENTAL CREDIBLE AT-BATS** (TRUE_ACTIONABLE) | **0** |

Secondary (not promotion-ready): **11** NEW WATCH/FUTURE rows survived hygiene — see §C / §F for which are noise vs secondary-research candidates.

---

## C. LIVE NEW OPPORTUNITIES (TRUE_ACTIONABLE)

**None.** Live NEW TRUE_ACTIONABLE = 0.

### Credible WATCH / FUTURE (manual review — not TRUE)

| Demand Type | Organization | Opportunity | Timing | Location | Room-Demand Thesis | Source | Suggested Action | Why New | Manual Audit |
|-------------|--------------|-------------|---------|----------|--------------------|--------|------------------|---------|--------------|
| Training | SANS Institute | SANS DC Metro September 2026 | 2026-09-28 | Bethesda, MD | Multi-day training lodging plausible | extract weak / no official URL stamped | WATCH — qualify open housing | Not in 38 | **WATCH-KEEP** — strongest live hit; needs official page + housing status |
| Medical | ACCP | 2026 ACCP Annual Meeting | 2026-09-27 | Bethesda, MD claimed | Conference lodging if geo real | weak | WATCH | Not in 38 | **WATCH-VERIFY-GEO** — confirm real venue city before pursuit |
| Nonprofit | AFCPE | 2026 AFCPE Symposium | 2026-11-01 | Bethesda claimed | Multi-day program if geo real | weak | WATCH | Not in 38 | **WATCH-VERIFY-GEO** |
| Event | AAM | GRx+Biosims 2026 | 2026-11-02 | Bethesda claimed | Association meeting lodging | weak | WATCH | Not in 38 | **WATCH-VERIFY-GEO** |
| Overflow | ACRM | ACRM 2026 | 2026-10-02 | Washington, DC | Overflow / room block play | weak | WATCH | Not in 38 | **WATCH-KEEP** — DC overflow pattern; need housing bureau evidence |
| Gov/Contractor | NIH | Construction Services for NIH Campus Renovation | 2026-01-01 | Bethesda | Trigger only unless traveling crew lodging | weak | WATCH | Not in 38 | **TRIGGER** — not opportunity without lodging thesis |
| Gov/Contractor | DoD | Integrated Lodging Program Sites | 2026-01-01 | Bethesda | Program listing, not discrete demand | weak | WATCH | Not in 38 | **NOISE** — listing != demand signal |
| Relocation | Blueground | Corporate Relocation Housing | 2026-09-23 | Bethesda | Vendor marketing | weak | WATCH | Not in 38 | **NOISE** — product page, not buyer demand |
| Sports | Bethesda Soccer | USL Youth Summer League Tryouts | 2026-01-01 | Bethesda | Local tryouts, weak overnight | weak | WATCH | Not in 38 | **NOISE** — local / no room thesis |
| Overflow | CSH | Maryland Lobbying & 2027 Advocacy | 2027-01-01 | Bethesda | Future advocacy lodging unclear | weak | FUTURE | Not in 38 | **WEAK FUTURE** |
| Event | CARB | California Air Resources Board Meeting | 2027-02-25 | Bethesda claimed | Geo hallucination | weak | FUTURE | Not in 38 | **FALSE GEO** — reject |

---

## D. QUALITY

| Metric | Value |
|--------|-------|
| LIVE NEW ACTIONABLE (TRUE) | **0** |
| TRUE after audit | **0** |
| FALSE | **0** (no TRUE to falsify) |
| PRECISION (TRUE) | **n/a** (0/0) |
| SOURCE on TRUE | n/a |
| THESIS on TRUE | n/a |
| ACTION on TRUE | n/a |
| GEOGRAPHY on TRUE | n/a |

WATCH quality after manual audit: **~2–4 keep-for-secondary-research** out of 11; rest noise / geo fail / trigger-only.  
**Do not promote.** TRUE bar held (not lowered to inflate counts).

---

## E. LANE YIELD

| Lane | Queries | URLs | Candidates | Qualified* | Actionable (TRUE) | New (any) |
|------|---------|------|------------|------------|-------------------|-----------|
| EVENT_ASSOCIATION | 2 | 18 | 5 | — | 0 | high PAST pollution |
| TRAINING | 2 | 18 | 3 | — | 0 | 1 WATCH (SANS) |
| GOVERNMENT_CONTRACTOR | 2 | 18 | 2 | — | 0 | 2 WATCH (trigger-heavy) |
| PROJECT_TEAM | 2 | 18 | 1 | — | 0 | 0 useful |
| RELOCATION | 2 | 18 | 1 | — | 0 | 1 noise (Blueground) |
| MEDICAL | 2 | 18 | 5 | — | 0 | 1 WATCH (ACCP) |
| UNIVERSITY | 2 | 18 | 4 | — | 0 | mostly PAST |
| SPORTS_ACADEMIC | 2 | 18 | 1 | — | 0 | 1 local noise |
| CONSULTING_PROJECT | 2 | 18 | 0 | — | 0 | EXTRACTION_FAILURE / empty |
| CORPORATE_MEETING | 2 | 18 | 2 | — | 0 | PAST / weak |
| BOARD_COMMITTEE | 2 | 18 | 0 | — | 0 | EXTRACTION_FAILURE |
| NONPROFIT | 2 | 18 | 2 | — | 0 | 1 WATCH (AFCPE) |
| PROFESSIONAL_CREW | 2 | 15 | 2 | — | 0 | weak |
| INCENTIVE_RETREAT | 2 | 18 | 0 | — | 0 | empty |
| OVERFLOW | 2 | 18 | 4 | — | 0 | 2 WATCH (ACRM + weak) |

\*Qualified = hygiene-scored rows attributed after merge (lane attribution imperfect when signal reclassified).

**Yield classes:** TRAINING / MEDICAL / EVENT / OVERFLOW = candidate volume; **none HIGH for TRUE**. CONSULTING / BOARD / INCENTIVE = LOW/ZERO useful.

---

## F. FAILURE / NOISE

| Class | Count / notes |
|-------|----------------|
| PAST_EVENT | **19** — SERP returned 2026 titles that hygiene dated as past vs as-of (date extraction / year-only → Jan 1 issues) |
| NO_OPEN_SOURCING_EVIDENCE | Dominant WATCH fail — extract did not stamp open housing / official hotel program |
| TRIGGER_WITHOUT_DEMAND | NIH construction award-style rows |
| LOCAL_NO_ROOM | Youth tryouts |
| OUT_OF_MARKET / FALSE GEO | California Air Resources Board → Bethesda |
| EXTRACTION_FAILURE / empty | Consulting, Board, Incentive lanes |
| SOURCE_WEAK | Official URL rarely persisted onto candidate |
| DUPLICATE | Cross-lane dedupe removed 1 |
| Closed/placed | Not primary this run |

---

## G. SPEED

| Item | Value |
|------|--------|
| TOTAL (live SERP run) | **~91s** |
| BY LANE | ~6–48s (BOARD slowest ~48s) |
| PARALLELIZED | **YES** (concurrency 4) |
| ORG CACHE HITS | process-local reuse during run |
| DUPLICATE FETCHES AVOIDED | 1 candidate dedupe |

Fast enough for weekly **if** query budget stays ~30. Not yet converting to TRUE.

---

## H. COST

| Item | Value |
|------|--------|
| SEARCH/API (est. Serp) | **~$0.30** (30 × $0.01) |
| OpenAI extract | billed separately (~30 batch calls across lanes) |
| SURFE | **0** |
| PDL | **0** |

---

## I. REGRESSION

| Suite | Result |
|-------|--------|
| NEW OPPS | **PASS** 17/17 |
| V3 | **PASS** |
| V4 | **PASS** 16/16 |
| CQ | **PASS** 15/15 |
| WEEKLY | **PASS** |
| SHARE | **PASS** |
| CSV | **PASS** 13/13 |

Baseline opportunities.json still **38**. No share/token changes. No deploy.

---

## J. DECISION

1. **Did live expanded discovery generate incremental credible TRUE at-bats?** **No** (0).
2. **How many?** **0** TRUE; ~2–4 WATCH worth secondary research only.
3. **Which lanes generated the best signals?** TRAINING (SANS), MEDICAL (ACCP verify), OVERFLOW (ACRM), EVENT (GRx verify geo).
4. **Which lanes were mostly noise?** RELOCATION (vendor SEO), SPORTS tryouts, GOVERNMENT award/listing pages, BOARD/CONSULTING/INCENTIVE empties, PAST_EVENT flood.
5. **Did live precision remain acceptable?** TRUE precision n/a; WATCH precision **not acceptable for promotion** (~half noise/geo).
6. **Triggers → hotel demand?** Partially — NIH construction stayed WATCH/trigger; correctly not TRUE without lodging thesis.
7. **Demand-generator research useful?** Weak this budget — org reuse cache present; little durable generator graph built from SERP snippets.
8. **Fast enough for weekly?** Runtime yes (~90s / 30 queries); **conversion to TRUE no**.
9. **Costs controlled?** Yes (~$0.30 Serp + extract; Surfe/PDL 0).
10. **Existing GDI regress?** No — regressions green; 38 untouched.
11. **Material gap before live promotion?** **Yes:**
    - Official URL / open-sourcing evidence must survive extract → qualify
    - Suppress PAST_EVENT / year-only Jan-1 pollution earlier in extract
    - Geo hallucination guard (org HQ city ≠ event city)
    - Second-pass enrichment on WATCH-KEEP only (official page fetch) before any TRUE promotion
    - Do not lower TRUE_ACTIONABLE to count WATCH as success

---

## K. NEXT STEP

**LIVE YIELD TOO LOW — DISCOVERY ROUTING NEEDS IMPROVEMENT**

Offline architecture still stands. Live SERP proved lanes fire and candidates appear, but **zero** NEW TRUE_ACTIONABLE under the real bar. Controlled promotion is **not** justified.

Recommended small cycle (not this report):
1. Extract system: force official URL + housing language fields  
2. Pre-filter PAST / geo-implausible before qualify  
3. Secondary official-page pass for WATCH-KEEP (SANS, ACRM, geo-verified medical/nonprofit)  
4. Re-run Bethesda live audit; require ≥1 TRUE with 100% source/thesis/action/geo before promotion discussion

---

## STOP

- No live write  
- No deploy  
- No contact cycle  
- TRUE_ACTIONABLE bar not lowered  
- Fixtures not counted as live success
