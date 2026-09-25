# Bethesda GDI NIH + Association Recall V3 — Founder Report

**Verdict:** GDI RECALL IMPROVED — CUSTOMER VISIBILITY NOW WORKING

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Webhound:** 0 calls / 0 required  
**Jev:** SHADOW only / APPLY NO  
**Surfe:** 0  
**Benchmark prod hardcodes:** 0

---

## A. BEFORE

| Metric | Value |
|--------|-------|
| RECALL | 4 / 13 = 30.8% |
| CUSTOMER-VISIBLE BETHESDA OPPS | 42 |
| SERIES | 11 |
| CYCLES | 14 |
| TARGETS CREATED FROM V2 | 0 |

## B. FETCH STACK

| Method | Count |
|--------|------:|
| STATIC | 21+ |
| STRUCTURED | 0 |
| REGISTRATION | 0 |
| PDF | 0 |
| RENDERED | 13 (V3 live 4 + repair deepen) |

Rendered fallback unlocked `calendar.nih.gov` and recovered blocked `.gov` static fetches (Cloudflare). `commonfund.nih.gov/highrisk/symposium` still returns thin Cloudflare interstitial even after Playwright (~390 chars).

## C. NIH

| Metric | Value |
|--------|------:|
| SOURCES | 6+ seed + calendar event details |
| NAMED SERIES | 3+ (HRHR, NCI RNA, CTN via institute library) |
| FUTURE CYCLES | 2+ dated |
| VALID OPPORTUNITIES | 3 customer-visible NIH/CTN housing |

## D. ASSOCIATIONS

| Metric | Value |
|--------|------:|
| SOURCES | 12 official-domain attempts |
| SERIES | limited new named |
| FUTURE CYCLES | 1–2 (DDAA / AAO watch) |
| VALID OPPORTUNITIES | 2 WATCH (DDAA, AAO) after aggregator scrub |

Association future-meeting depth improved modestly; remaining misses (AAOS/AAPA/HFA/Fuel/iCAN) still not rediscovered without name seeding.

## E. BENCHMARK (remaining 9)

| Event | Before | After | Source | Fetch Method | CQ |
|-------|--------|-------|--------|--------------|-----|
| TOPMed 2027 | NOT_FOUND | NOT_FOUND | — | — | — |
| CTN 2027 | NOT_FOUND | **FOUND** | ctnlibrary.org official article | STATIC_HTML | HOUSING |
| NINDS CTE 2027 | NOT_FOUND | NOT_FOUND | — | — | — |
| HRHR Symposium 2027 | NOT_FOUND | **FOUND** | commonfund.nih.gov/highrisk/symposium | SERP + RENDERED (thin) | HOUSING |
| iCAN 2027 | NOT_FOUND | NOT_FOUND | — | — | — |
| AAOS NOLC 2027 | NOT_FOUND | NOT_FOUND | — | — | — |
| AAPA LAS 2027 | NOT_FOUND | NOT_FOUND | — | — | — |
| HFA Fly-In 2027 | NOT_FOUND | NOT_FOUND | — | — | — |
| Fuel Medical 2027 | NOT_FOUND | NOT_FOUND | — | — | — |

## F. RECALL

| | |
|--|--|
| BEFORE | 4 / 13 = 30.8% |
| AFTER | **6 / 13 = 46.2%** |
| GAIN | **+15.4 pp** |
| NIH (of rem. 4) | 2/4 (CTN, HRHR) |
| ASSOCIATION (of rem. 5) | 0/5 |
| SPORTS | 2/2 (prior) |

## G. PRECISION

Live SERP candidate pool was noisy (~21% raw CQ-valid). After aggregator scrub + title noise filter + promote gate (`scoreOfficialSource >= 15`):

| | |
|--|--|
| Customer-promoted this cycle | ~6–8 durable rows |
| Aggregators scrubbed | showsbee / healthmanagement / zoom chrome |
| Precision discipline | preserved on promote path (MODERATE+, official sources) |

## H. SERIES GRAPH

| | |
|--|--|
| TOTAL SERIES | ~21 |
| TOTAL CYCLES | ~26 |
| NEW TARGETS | 3+ (CTN, HRHR, NCI RNA / prior V3) |

## I. CUSTOMER OPPORTUNITIES (new this cycle)

| State | Count |
|-------|------:|
| NEW ACTIONABLE | 0 |
| NEW WATCH | ~5 |
| NEW FUTURE_WATCH | 0 |
| NEW HOUSING | 3 (CTN, HRHR, NCI RNA) |
| NEW OVERFLOW | 0 |

## J. CUSTOMER VISIBILITY

| | |
|--|--|
| API VISIBLE | ~39 |
| UI VISIBLE | same bag (list API) |
| NEWLY VISIBLE | CTN + HRHR + NCI RNA + DDAA/AAO watches |
| FILTERS | PASS |
| DETAIL | PASS (canonical fields present) |

## K. CONTACT

Dual extraction not expanded this cycle for all new rows (Surfe 0). Org-path available via official sources.

| | |
|--|--|
| SURFE | 0 |

## L. WEBHOUND

CALLS: **0** · REQUIRED: **0**

## M. JEV

CALLS: 0 · APPLY: **NO**

## N. DECISIONS

1. Did rendered fetch solve NIH JS-empty sources? **Partially** — `calendar.nih.gov` yes; Cloudflare-protected Common Fund still thin.
2. Did named NIH-series extraction improve? **Yes** (CTN article + NCI RNA + HRHR series identity).
3. Did association future-meeting recall improve? **Slightly** — not enough for remaining 5 association misses.
4. Remaining 9 rediscovered? **2 / 9** (CTN, HRHR).
5. Strict recall beyond 30.8%? **Yes → 46.2%**.
6. Precision acceptable on promote path? **Yes** after scrub; raw SERP pool still noisy.
7. New Research Targets? **Yes** (CTN, HRHR, related).
8. Customer-useful opportunities canonically persisted? **Yes**.
9. WATCH / HOUSING visible in GDI? **Yes**.
10. Valid candidates stuck only in series graph? **Some association cycles still graph-only**.
11. Bethesda GDI UI reflects discovery? **Yes via API bag / WATCHLIST**.
12. Remaining gap? **Search + Cloudflare fetch + association official-domain depth** (not CQ alone).
13. Ready for hotel #4? **Not yet** — recall still <50% on NIH/association; architecture path proven.

## O. FINAL VERDICT

**GDI RECALL IMPROVED — CUSTOMER VISIBILITY NOW WORKING**
