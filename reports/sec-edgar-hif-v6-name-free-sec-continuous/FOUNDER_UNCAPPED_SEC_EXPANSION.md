# HIF V6 — Uncapped SEC Discovery & Expansion Report

**As of:** 2026-11-21  
**Publication:** CUSTOMER_BLOCKED  
**Research cost:** $0  
**Milestone policy:** 1,000 / 2,500 / 5,000 / 10,000 are progress markers — **not ceilings**

## 1) Post-#520 canonical inventory

| Metric | Count |
|--------|------:|
| Distinct events | 829 |
| Active dated | **453** |
| Active undated | 159 |
| Historical | 217 |
| **Toward first 1,000** | **612** |
| Agreement type unresolved (among toward) | 490 |
| Franchise resolved | 74 |
| Management resolved | 48 |
| Country | US 612 |

PR #520 MERGED — SHA `a8f49857af822a92828a6f12986b86b2661826db`

Naive arithmetic is rejected; event-key reconcile is authoritative.

## 2–4) Continuous name-free wave

| | |
|--|--:|
| Docs processed | 50 (all OK) |
| Extractable schedules | 34 |
| Net-new events | **308** |
| New active-dated | **227** |
| Historical preserved | 81 |
| Duplicates excluded | 69 |
| Trailing yield | still productive (2–12 net-new/doc; not exhausted) |

Evidence examples:
- Holiday Inn Express Grove City Outlet Center — 2034-09-05 — combined column UNRESOLVED — [424B2](https://www.sec.gov/Archives/edgar/data/850779/000153949719002207/n1864-x17_424b2.htm)
- Hampton Inn Exton — 2029-12-31 — UNRESOLVED
- Holiday Inn Express Savannah Historic District — 2027-12-20 — UNRESOLVED

## 5–6) Agreement-type resolution

- Attempted 50 near-term unresolved rows across 20 docs
- Resolved FRANCHISE: **3** (all historical Candlewood/Homewood — adjacent franchisor column)
- Resolved MANAGEMENT: **0**
- Still unresolved: 47 (6% resolution rate on this batch)
- Limitation: combined CMBS columns rarely carry exclusive franchisor vs manager labels without EX-10 / footnote depth

## 7–9) SEC family expansion (probe)

- 18 documents (primarily 10-K hits this batch)
- Management schedule candidates: **7**
- Franchise schedule candidates: **11**
- Auto-qualified commercial opportunities: **0** (by design)

## 10–12) Geography, dedupe, cost

- Country: still US-dominant from SEC CMBS engine
- MX/BR/GB/CA: continue Track B native sources (do not force SEC country-keyword bleed)
- Cost: **$0**

## 13) Open PRs / integration

| PR | Status |
|----|--------|
| #520 scale-up | **MERGED** `a8f49857a` |
| #523 CCOD/OCOD activation | OPEN (Track B) |
| This continuous wave | new PR (Track A) |

IC dirty primary preserved. Product HE/ADP/GDI/BE dirty trees preserved.

## 14) Recommended next allocation

1. Continue Related License 424B2 pagination (yield not exhausted)
2. Deepen agreement resolution via EX-10 / property footnotes for near-term dates only
3. Extract property-level rows from the 7× 10-K management schedule candidates
4. Track B: native MX/BR/GB/CA (not SEC keyword bleed)
5. Keep ownership enrichment deferred

**Qualified commercial BCT/MCT promotions this wave:** 0 new auto-promotions (unresolved types retained as contractual intelligence, not inflated milestone opportunities).
