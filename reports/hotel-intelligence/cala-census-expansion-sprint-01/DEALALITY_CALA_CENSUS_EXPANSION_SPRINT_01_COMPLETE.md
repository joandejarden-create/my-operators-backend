# DEALALITY_CALA_CENSUS_EXPANSION_SPRINT_01_COMPLETE

**Generated:** 2026-08-10  
**Production writes:** **0** · Live census unchanged at **5,956**

## 1. Safety

```text
Production writes: 0
Census writes: 0
Automatic merges: 0
Schema changes: 0
Secrets exposed: false
```

`ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0` · `ENABLE_HBX_CENSUS_WRITES=0`

## 2. Baseline (`SPRINT_01_BASELINE_LOCKED`)

```text
Live hotels: 5,956
Estimated universe: 16,168
Coverage: 36.8%
Distance to 15K: 9,044
```

Zero-coverage (est. universe > 0): **18** · Countries <20%: **38** · <50%: **47** · ≥95%: **2**

## 3. Existing Candidate Inventory

| Source | Stock |
| --- | ---: |
| Brazil holds | 4,842 |
| Brazil all Cvent-derived | 5,165 |
| Prior READY staged | 91 |
| Prior REVIEW staged | 156 |
| Track B stock (8 geos) | ~378 |

Sprint reused existing holds/Cvent inventory — no rediscovery.

## 4. Sprint Selection (`SPRINT_01_COUNTRY_SELECTION`)

| Track | Country | Current | Est. universe | Coverage | Stock | Target | Reason |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| A | Brazil | 494 | 5,336 | 9.3% | 4,842 | 750 @ offset 250 | Largest gap; validated factory |
| B | Turks and Caicos Islands | 0 | 57 | 0% | 57 | 57 | Zero + stock |
| B | Bonaire | 0 | 51 | 0% | 51 | 51 | Zero + stock |
| B | Martinique | 0 | 64 | 0% | 64 | 50 | Zero + stock |
| B | U.S. Virgin Islands | 0 | 51 | 0% | 51 | 50 | Zero + stock |
| B | Anguilla | 0 | 25 | 0% | 25 | 25 | Zero floor |
| B | Montserrat | 0 | 50 | 0% | 50 | 50 | Zero + stock |
| B | Guadeloupe | 0 | 32 | 0% | 32 | 32 | Zero + stock |
| B | Saint Lucia | 3 | 48 | 6.3% | 48 | 45 | Near-zero floor |

```text
Track A candidate target: 750
Track B candidate target: 360
Total planned: 1,110
```

## 5. Track A Results (Brazil ×750)

| Metric | Value |
| --- | ---: |
| Processed | 750 |
| READY_FOR_IMPORT | 257 (34.3%) |
| REVIEW_REQUIRED | 478 (63.7%) |
| Matched existing | 8 (1.1% dup) |
| Rejected | 7 |
| Avg identity confidence | 0.903 |

Quality remains consistent with prior Brazil×250 factory performance (dup <5%, Tier A ~34%).

## 6. Track B Results (country-by-country)

| Country | Raw | Existing | READY | Review | Rejected | Projected READY census | Coverage Δ |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Turks and Caicos | 57 | 0 | 0 | 57 | 0 | 0 | 0% → 0% |
| Bonaire | 51 | 3 | 0 | 48 | 0 | 0 | 0% → 0% |
| Martinique | 50 | 1 | 0 | 49 | 0 | 0 | 0% → 0% |
| U.S. Virgin Islands | 50 | 0 | 0 | 50 | 0 | 0 | 0% → 0% |
| Anguilla | 25 | 0 | 0 | 25 | 0 | 0 | 0% → 0% |
| Montserrat | 50 | 0 | 0 | 50 | 0 | 0 | 0% → 0% |
| Guadeloupe | 32 | 1 | 0 | 31 | 0 | 0 | 0% → 0% |
| Saint Lucia | 45 | 1 | 0 | 44 | 0 | 0 | 6.3% unchanged via READY |

**All Track B new candidates staged as REVIEW_REQUIRED** (identity ~0.83–0.88; below Tier A 0.90). Geographic floor lift requires review promotion or city corroboration (GIATA/SerpApi) — not auto-READY.

## 7. Identity Results

```text
Existing: 14
New high-confidence (READY): 257
Review: 832
Ambiguous: 2
Duplicates: 14
Rejected: 7
```

## 8. Quality Validation

**Track A:** Names + cities present; `dhl_` IDs assigned; low duplicate rate; classification reasonable.  
**Track B:** Legitimate hotels (e.g. Andaz Turks & Caicos, Salterra) with city from Cvent URL slug, but confidence band forces REVIEW — correct under existing thresholds. Not non-hotels.

## 9. Provider Usage

```text
GIATA Drive calls: 8 (3 index + ~5 detail; selective TC/BQ/AI)
SerpApi calls: 0
HBX calls: 0
Other calls: 0
```

## 10. Discovery Efficiency

```text
Raw processed: 1,110
Net-new READY: 257 (23.2% yield)
Duplicates prevented: 14
Review rate: 75.0%
Net-new READY per external API call: ~32 (inventory-first)
```

## 11. Geographic Improvement (if READY imported)

```text
Zero countries before: 18
Zero countries projected after: 18   (Track B had 0 READY)

<20% before / projected after: 38 / ~34–38
<50% before / projected after: 47 / ~43–47

Brazil coverage: 9.3% → 14.1% (494 → 751 if READY imported)
```

## 12. 15K Progress

```text
LIVE: 5,956
READY STAGED THIS SPRINT: 257
MERGED READY QUEUE (incl. prior): 348
PROJECTED AFTER THIS-SPRINT READY IMPORT: 6,213

Remaining to 10K: 3,787
Remaining to 12.5K: 6,287
Remaining to 15K: 8,787
```

Overall coverage if READY imported: **36.8% → ~38.4%**

## 13. Coverage Dashboard Refresh

Live dashboard regenerated (`npm run hotel-intelligence:cala-coverage-dashboard`):

| | LIVE | PROJECTED (sprint READY only) |
| --- | ---: | ---: |
| Hotels | 5,956 | 6,213 |
| Coverage | 36.8% | ~38.4% |
| READY queue | 348 | — |
| REVIEW queue | 988 | — |

## 14. Exceptions / Blockers

1. **Track B review wall** — 360 candidates need REVIEW before they move zero-coverage countries.  
2. Brief bad projected dashboard write during sprint — **fixed** by regenerating official coverage dashboard.  
3. GIATA MultiCodes/MHG production still pending (not required this sprint).

## 15. SPRINT 02 RECOMMENDATION (not executed)

```text
TRACK A
Country: Brazil
Target: 1,000 (continue offset 1,000; ~3,842 holds remain)

TRACK B
Countries: Bahamas, Dominica, Saint Barthélemy, Paraguay, Belize
Targets: 80, 23, 50, 80, 100

Plus: process Track B Sprint-01 REVIEW queue with GIATA Drive / selective SerpApi city corroboration to promote Tier B → A where safe

Expected candidates: ~1,333 (+ review uplift)
Expected net-new: ~300 READY (at ~23% yield) + review promotions TBD
Expected geographic improvement: reduce zero-coverage Caribbean/SA pockets after review promotion
```

## 16. Verdict

```text
REVIEW_AMBIGUOUS_BATCH_FIRST
```

Track A quality supports continued Brazil scaling, but **832 REVIEW** (especially 360 Track B) should be triaged before claiming geographic completeness wins. Import gate remains separate — no READY import in this task.

---

Artifacts: `reports/hotel-intelligence/cala-census-expansion-sprint-01/`  
Staged: `data/hotel-intelligence/cala-census-expansion-sprint-01/` + merged factory queues  
Script: `npm run hotel-intelligence:cala-census-expansion-sprint-01`
