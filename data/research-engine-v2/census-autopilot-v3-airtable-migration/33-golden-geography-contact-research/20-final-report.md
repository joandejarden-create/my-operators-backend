# V3.0.2 Final Report

**Cohort:** same 150 V3 properties (`cav3_2026-08-08T15-04-05-566Z`)  
**Airtable writes:** NONE  
**SerpApi:** not run (no `SERPAPI_API_KEY` in env) — official-only research  
**V3.1:** NOT LAUNCHED

## STATE / REGION
1. Baseline: **32/150 (21%)**
2. Final staging: **55/150 (36.7%)**
3. Production-eligible: **55/150**
4. Primary method: Hilton GraphQL / official page `addressRegion` + Dealality city→admin derivation
5. Unresolved: **95**
6. Main reason: Brazil/Argentina postal-as-city labels + sparse page region fields

## ADDRESS
7. Baseline = **0**
8. Final independently researched: **92/150 (61.3%)**
9. Production-eligible: **92/150**
10. Official-source: **92**
11. SerpApi-only: **0**
12. Unresolved: **58**

## PHONE
13. Baseline = **0**
14. Final researched (any): **55/150**
15. Property-direct: **54/150 (36%)**
16. Central-reservation-only: **1**
17. Production-eligible: **54/150**
18. Unresolved (no property-direct): **96**

## SUBMARKET
19. Baseline matched = **46**
20. Baseline no_corridor = **104**
21. Final matched = **46**
22. Applicable resolution % = **30.7%**
23. No meaningful Submarket (class H) = **0** (classifier under-assigned H; many island/single-market cases still tagged F/D)
24. Remaining genuine taxonomy gaps = **104**
25. Main failure reasons: missing State/Region (53), market exists but corridor rule missing (38), missing coords (10), municipality vs destination (3)

## COORDINATES
26. Existing 60 preserved? **YES**
27. Additional official coordinates found? **55**
28. Final coordinate coverage: **115/150**
29. Coordinate regression? **NO (0)**

## CLAIMS
30. Prior verified claims survived? **YES**
31. Later incomplete object erase prior? **NO**
32. Blocked SerpApi cannot suppress official? **YES** (claim store; SerpApi not invoked this run)

## COMPLETENESS
33. Baseline production Priority: **48.2%** (57.7% after V3.0.1 coords)
34. Final staging Priority: **72.3%**
35. Final production-eligible Priority: **72.2%**
36. Excluding Rooms diagnostic: **72.2%**
37. Hotels ≥95% excl Rooms: **4%**

## COST
38. Official page fetches: **150**
39. SerpApi requests: **0**
40. Directory lookups / Hilton GraphQL: **300 / 40**
41. Approx fields resolved per official fetch: **~0.97**

## PRODUCTION BACKFILL (DRY-RUN ONLY — NOT APPLIED)
42. State/Region blank fills: **31**
43. Address: **78**
44. Submarket: **8**
45. Latitude/Longitude: **45**
46. Phone: **47**
47. Records affected: **89**
48. Overwrite: **NO**
49. Cvent: **0**
50. Legacy: **0**

## GOLDEN SCHEMA
51. Phone: **CONDITIONAL REQUIRED** — do not block Golden ≥95% when only central-reservations exist after deep official research
52. Submarket: **APPLICABILITY-BASED** — Market-level only where corridor segmentation has no business meaning
53. Joan decides before changing Golden completeness denominator

## V3.1 GATES
54. State/Region ≥90%? **NO (36.7%)**
55. Address ≥80%? **NO (61.3%)**
56. Phone ≥70%? **NO (36%)**
57. Submarket ≥90% applicable? **NO (30.7%)**
58. Safety regression? **NO**
59. V3.1 READY? **NO**

## MOST IMPORTANTLY
60. Remaining incompleteness mainly Rooms? **NO** — geography/contact still material (esp. Submarket + Phone + State for BR/AR)
61. Can Autopilot routinely populate these before write? **YES** — deep research path + claim store now exist; coverage still needs SerpApi staging pass + taxonomy expansion
62. Will next wave be richer than V3? **YES** — once backfill authorized and gates improve (Address already +92 official)

## FINAL VERDICTS
| Area | Verdict |
|------|---------|
| **GOLDEN GEOGRAPHY/CONTACT** | **PARTIAL** |
| **PRODUCTION BACKFILL** | **READY FOR AUTHORIZATION** |
| **V3.1** | **NOT READY** |

### Next research blockers (exact)
1. **SerpApi staging pass** for remaining Address/Phone gaps (key not present this run)
2. **Brazil CEP→locality** + metro corridor aliases for Submarket
3. **Mexico state-as-city** → destination corridor rules
4. **Phone** property-direct coverage on IHG/Marriott/Choice pages
