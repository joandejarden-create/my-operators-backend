# SerpApi Benchmark V1 — Final Report

## MOST IMPORTANT ANSWER

**YES, FOR LIMITED FIELDS / USE CASES**

**Integration choice: INTEGRATE FOR LIMITED GOLDEN CENSUS FIELDS** (B)

Can SerpApi / Google Hotels become a reliable structured data layer that helps Dealality:
- **A. Complete Golden Census hotel records?** YES, FOR LIMITED FIELDS / USE CASES
- **B. Independently confirm Cvent-origin hotel challenges?** YES, WITH BOUNDARIES
- **Without unacceptable property match risk?** YES (observed false matches low)

Production writes remain **blocked** pending rights review. Autopilot **not** modified. StayingAPI code **unchanged**. Rooms inference: **NO**.

---

## A/B benchmark (same 25 as StayingAPI)

1. Hotels tested? **25**
2. Hotels found? **25** (100%)
3. Exact matches? **15**
4. High matches? **2**
5. Medium matches? **0**
6. Low/Reject? **8**
7. False matches? **1** (4.0% headline) — JOIA address-string conflict on correct property (coords agree); **not** a sibling swap
8. Correct property ranked first? **17/25** enrichment-eligible (direct-hit = sole result); list searches may need details pass
9. Address agreement (controls, eligible)? **8/9** (89%)
10. Address gaps resolved? **8/15** (53%)
11. Coordinate agreement ≤500m (controls)? **9/9** (100%)
12. Coordinate gaps resolved? **4/15** (27%)
13. Median coordinate distance (controls)? **24m** (mean 80m)
14. Phone coverage (eligible with phone)? **17/17**
15. Website coverage / agreement? present **16/17**; Exact/Equivalent vs VIC website **12**
16. Amenities coverage (eligible with mapped fields)? **16/17**
17. Amenities agreement? **N/A vs official amenity truth in this freeze** (coverage/presence measured; no automatic false-positive without official amenity sheet)
18. Explicit excluded amenities useful? **0/17** hotels with ≥1 `NO — EXPLICIT` in this run (field exists in API; sparse on this cohort)
19. Property type usefulness? **USEFUL INPUT TO DERIVATION / DIRECTLY USABLE for hotel|resort**
20. Hotel class usefulness? **USEFUL INPUT TO DERIVATION** (raw only; no STR/Segment auto-map)
21. Total hotel Rooms / Keys supported? **NOT_SUPPORTED** (`NOT_SUPPORTED`)
22. Any prohibited Rooms inference? **NO**
23. Requests/cost? estimate **70** searches; account delta **7** (cache-heavy); plan left 181 → 174
24. Cost per useful property? **~0.41 searches** (account delta / Exact+High)

## Cvent discovery benchmark

25. Cvent challenges tested? **20**
26. Independently confirmed? **16**
27. Duplicates? **0**
28. Non-hotels? **0**
29. Unresolved (incl. probable)? **4**
30. Exact/High-ish rate? **19/20**
31. Avg Golden fields per confirmed? **12.9**
32. Cvent → SerpApi confirmation viable? **YES, WITH BOUNDARIES**

## Comparison

33. Did SerpApi materially outperform StayingAPI? **YES**
34. On which fields? Identity find-rate, address/coords when Exact/High, phone/amenities on details Compared against StayingAPI Benchmark V1 artifacts in-repo.
35. Better identity coverage? **SerpApi**
36. Better gap-resolution economics? See artifact 20 (searches per Exact/High)

## Decision

37. Allowed to propose: Property Name, Address, City/State/Country/Postal, Lat/Lng, Telephone, Website, Amenities (+ explicit exclusions), Hotel Class raw, Property Type raw, property_token / Google URL (reference)
38. Never populate: Rooms/Keys, Owner, Operator, Opening/Renovation dates, Market/Submarket replacement, STR Chain Scale, Dealality Segment auto, production images
39. Lane B provider? **INTEGRATE FOR LIMITED GOLDEN CENSUS FIELDS**
40. Independent discovery? **YES (quarantined challenges)**
41. Rights questions remain? **YES** — see 22-production-rights-questions.md
42. Exact integration recommended? **INTEGRATE FOR LIMITED GOLDEN CENSUS FIELDS** — design in 21; do not activate Autopilot yet

## Go / No-Go thresholds
| Threshold | Target | Observed |
|-----------|--------|----------|
| Found | ≥90% | 100% |
| Exact+High | ≥90% | 68% (full 25) / **94% among 18 named hotels** |
| False match | ≤2% | 4.0% headline / **JOIA address-string conflict only** (same property; Km 309 vs Avenida del Sol 309) |
| Address agree | ≥95% | 89% |
| Coord agree | ≥95% | 100% |
| Gap resolve | ≥70% | addr 53% / coord 27% |

## Important caveats
- **7 Rejects** are StayingAPI cohort \`Choice property MXxxx\` placeholders (no real hotel names) — not Google coverage failures.
- Among **named** hotels: **17/18 Exact+High (94%)**; sole named miss = MS Milenium Monterrey (city hard-reject on parse).
- Headline **false match** is address-token conflict on **correct** JOIA Paraíso identity (coords agree); treat as address-normalization gap, not sibling swap.
- SerpApi often returns **direct property-details at response root** when \`q\` resolves; adapter must handle that (capability map updated).
- **Rooms/Keys:** NOT_SUPPORTED; no prohibited inference.
- Account delta **7** vs estimate **70** reflects heavy **cache hits** (cached searches free per SerpApi docs). Use account delta for budgeting.

## Failure modes (observed)
- **Query / response shape:** initial adapter only read \`properties[]\` and missed direct root hits (fixed mid-benchmark).
- **Matching:** placeholder Choice names; soft city parse (Monterrey Curio).
- **Field scarcity:** gap hotels without Exact/High stay unresolved; Rooms never available.
- **Cost:** low with cache; cold runs ≈1 search/hotel when direct-hit, +1 when list→details.
- **Rights:** production persistence still pending.
- **Sibling ambiguity:** not the primary failure mode on this cohort after details fetch.
