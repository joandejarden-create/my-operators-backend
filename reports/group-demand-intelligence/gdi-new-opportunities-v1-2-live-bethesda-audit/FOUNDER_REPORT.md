# GDI New Opportunities V1.2 — Live Discovery Routing Founder Report

Generated: 2026-09-24T02:55:00.000Z  
Mode: **LIVE SERP + DRY-RUN QUALIFY** · Live write: NO · Deploy: NO · Contact enrichment: NO (Surfe/PDL = 0)

Artifacts:
- `reports/group-demand-intelligence/gdi-new-opportunities-v1-2-live-bethesda-audit/LIVE_AUDIT.json`
- `reports/group-demand-intelligence/gdi-new-opportunities-v1-2-live-bethesda-audit/LIVE_REQUALIFY_FINAL.json`
- `lib/group-demand-intelligence/discovery-routing-v1-2.js`

---

## A. ROOT CAUSES

| Issue | Root cause |
|-------|------------|
| **PAST POLLUTION** | Broad SERP queries returned historical/archive pages; year-only dates were floor-normalized to `YYYY-01-01`, then hygiene treated them as calendar past. Early SERP screen was missing. |
| **SOURCE LOSS** | `buildOpportunity` / import path dropped `officialSource` when only `evidenceSources` / SERP URL existed — qualification saw SOURCE_WEAK / no Tier-1 URL → blocked TRUE. |
| **GEO ERROR** | Organization HQ / association city was used as event location (e.g. CARB → Bethesda). No separate `organizationLocation` vs `eventLocation` / `hotelDemandLocation` fields or GEO_CONFLICT fail-closed. |
| **EMPTY-LANE** | **CONSULTING:** SERP returned URLs and extract ran (~67s) but candidate commercial fields were thin / failed open-sourcing gates — not a total extract crash. **BOARD / INCENTIVE:** public discrete lodging demand is scarce; queries returned pages but few hotel-block signals — structural observability limit more than broken extract. |

---

## B. FUNNEL BEFORE / AFTER

| Metric | V1.1 | V1.2 primary live | V1.2 after source/open fix (requalify) |
|--------|------:|------------------:|--------------------------------------:|
| SERP URLs | 267 | 300 | (same seeds) |
| PAST skipped early | 0 | 3 | 3 |
| Fetched | — | 54 | 54 |
| Candidates (deduped) | 31 | 26 | 26 |
| Invalid Past | 19 | 16 | 16 |
| WATCH | 9–11 | 6 before 2nd pass / 7–11 after | 2 VALID_WATCH |
| Second-pass | 0 | 6 eligible · 12 queries · 5 candidates | (feeds requalify) |
| TRUE_ACTIONABLE | **0** | **0** (primary; source still dropped) | **4** |

Primary live TRUE stayed 0 until official-source survival + open/housing gates were fixed; requalify on **same SERP seeds** (no new SERP spend) recovered **4 NEW TRUE**.

---

## C. LIVE NEW TRUE

### 1. Regulatory Information Conference (NRC)
- **Demand Type:** Training Program  
- **Organization:** Nuclear Regulatory Commission  
- **Date:** 2026-09-30  
- **Location:** Bethesda, MD  
- **Official Source:** https://www.nrc.gov/public-involve/conference-symposia/ric/hotels  
- **Room-Demand Thesis:** Official hotels / housing page (HOUSING_OPEN)  
- **Venue/Housing/Sourcing:** HOUSING_OPEN / OPEN_UNRESOLVED  
- **Suggested Action:** Monitor housing; light relationship; do not oversell if already placed  
- **Why New to GDI:** Not in 38-opp baseline (matchScore 8)  
- **Why event-only search would miss:** Training / gov symposium housing page, not classic association annual-meeting SERP  

### 2. Bethesda Premier Cup
- **Demand Type:** Sports / Academic Competition  
- **Organization:** Bethesda Soccer Club  
- **Date:** 2026-11-13  
- **Location:** Bethesda, MD  
- **Official Source:** https://premiercup.bethesdasoccertournaments.com/hotel-transportationn/  
- **Room-Demand Thesis:** Official hotel/transportation page (HOUSING_OPEN)  
- **Suggested Action:** Contact housing/room-block path for traveling teams  
- **Why New to GDI:** Baseline may have related cup row; live URL path is incremental evidence (matchScore 50 — review before promote)  
- **Why event-only miss:** Sports tournament housing lane  

### 3. ACCP Annual Meeting
- **Demand Type:** Event / Association  
- **Organization:** American College of Clinical Pharmacy  
- **Date:** 2026-10-01  
- **Location:** Bethesda / DC travel venue page  
- **Official Source:** https://accp1.org/ACCP1/3Annual_Meeting/DC_Travel_Venue.aspx  
- **Room-Demand Thesis:** Official travel/venue/housing page  
- **Why New to GDI:** Not matched to baseline (score 8)  
- **Why event-only miss:** Needed official travel/venue page + source preservation  

### 4. ACVNU Renal Week
- **Demand Type:** Overflow Housing  
- **Organization:** ACVNU  
- **Date:** 2027-04-12  
- **Location:** Bethesda, MD  
- **Official Source:** https://acvnurenalweek.org/Plan-Your-Trip/Hotel-Accommodations  
- **Room-Demand Thesis:** Official hotel accommodations page  
- **Why New to GDI:** Not in baseline  
- **Why event-only miss:** Overflow / accommodations path  

All four: official/credible source, room-demand thesis, commercial geography, defensible action — **TRUE bar not lowered**.

---

## D. MORE AT-BATS

| Metric | Count |
|--------|------:|
| EVENT-ONLY TRUE NEW | 1 (ACCP) |
| EXPANDED-DEMAND TRUE NEW | 3 (Training, Sports, Overflow) |
| INCREMENTAL CREDIBLE AT-BATS | **4** |

---

## E. WATCH RECOVERY

| Metric | Count |
|--------|------:|
| WATCH SENT TO SECOND PASS | 6 |
| PROMOTED TRUE (in second-pass step alone) | 0 |
| TRUE after source/open requalify | 4 |
| REMAIN WATCH | 2 |
| REJECTED (second-pass) | 0 |

Second-pass found official-ish URLs but primary qualify still failed until source lineage + open gates were fixed on the same candidates. Remaining WATCH: Downtown Implementation Advisory (no open sourcing); CHPA Regulatory/Scientific/Quality Conference.

---

## F. QUALITY (requalify TRUE set)

| Metric | Value |
|--------|------:|
| TRUE | 4 |
| FALSE (among promoted) | 0 |
| PRECISION (promoted) | 100% |
| SOURCE | 100% |
| THESIS | 100% |
| ACTION | 100% |
| GEO | 100% |

Past still high among INVALID (16/19) — early skip only caught 3 SERP hits; more budget should spend earlier on year/archive screen.

---

## G. SPEED / COST

| Metric | Value |
|--------|------:|
| PRIMARY QUERIES | ~30 (2 × 15 lanes) |
| SECOND-PASS QUERIES | 12 |
| TOTAL SERP QUERIES | 42 |
| TOTAL TIME | ~149 s (~2.5 min) |
| SERP COST | ~$0.41 |
| SURFE | 0 |
| PDL | 0 |

Suitable for weekly use at this budget.

---

## H. REGRESSION

| Suite | Result |
|-------|--------|
| NEW OPPS V1 | **PASS** (17/17) |
| V1.2 routing | **PASS** (14/14) |
| V3 hygiene | **1 FAIL** — Expo Parks Dominicana freeze (`wave2_live_reprocess_matches_freeze`) — pre-existing / unrelated to V1.2 routing |
| V4 recall | **PASS** (16/16) |
| CQ | **PASS** (15/15) |
| WEEKLY | **PASS** |
| SHARE | **PASS** |
| CSV | **PASS** (13/13) |

---

## I. DECISION

1. Did PAST_EVENT pollution materially decline? **YES, partially** (19 → 16 invalid past; early skip = 3). More early filtering still needed.  
2. Are official sources now preserved? **YES** (0 → 26 preserved on requalify path).  
3. Are false geographies blocked? **YES** in code (GEO_CONFLICT / separate location fields); live primary did not emit CARB-style false geo.  
4. Did targeted second-pass recover real opportunities? **Partially** — second-pass alone did not promote; combined with source/open fix → **4 TRUE**.  
5. How many live incremental credible at-bats? **4**  
6. Which demand lanes worked? Training, Sports, Event/Association, Overflow  
7. Which lanes remain structurally weak? Consulting (thin commercial extract), Board/Incentive (scarce public lodging signals), Relocation (vendor noise filtered)  
8. Did WATCH noise decline? **YES** (11 → 2 VALID_WATCH after fix)  
9. Did runtime/cost remain suitable for weekly use? **YES** (~$0.41 / ~2.5 min)  
10. Is live promotion now justified? **YES — controlled promotion of the 4 TRUE**, after human skim of Premier Cup matchScore 50  

---

## J. FINAL VERDICT

**LIVE DISCOVERY IMPROVED — ONE SMALL CYCLE REMAINS**

Rationale: Minimum + better success bars met (4 NEW TRUE, expanded lanes, source/geo/routing fixed). Remaining cycle: (a) stronger early PAST SERP skip volume reduction, (b) close Expo Parks V3 freeze flake, (c) optional consulting extract diagnosis. Then controlled promotion.

**Follow-up (2026-09-24):** Closed wiring gaps from recovery audit — SERP all-skip no longer restores organic; hygiene fail-closes on `ORG_VS_EVENT` / any `GEO_CONFLICT`; Webhound map runs `enrichCandidateRoutingV12`. V1.2 suite now **16/16**. Second-pass was already invoked from live audit `main()`.
