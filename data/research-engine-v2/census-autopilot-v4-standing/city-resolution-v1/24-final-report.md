# City Resolution V1 — Final Report (Dry Run)

**DO NOT APPLY · V4 PAUSED**

Full-table research across **1,437** live Hotel Property Census records.

## Verdicts

| | |
| --- | --- |
| CITY RESOLUTION | **READY** |
| UNKNOWN PLACEHOLDERS | **REMOVE FROM FACTUAL FIELDS** |
| DOWNSTREAM GEOGRAPHY | **READY** (cascade proven; limited unlock volume) |
| CITY CORRECTIVE MANIFEST | **READY FOR AUTHORIZATION** |
| V4 | **READY AFTER CITY CORRECTION** |

---

## CITY BASELINE
1. Total Census records? **1437**
2. Valid City? **1360**
3. Literal Unknown City? **39**
4. Blank City? **38**
5. Conflict? **0**
6. Known-invalid? **0**

## ROOT CAUSE
7. Unknown because research never attempted? **1** (no address/coords/url)
8. Existing claim not propagated? **0** recovered
9. Address exists but City not extracted? **4**
10. Coordinates exist but City not derived? **4**
11. Source blocked? **0**
12. SerpApi candidate? **76**
13. True ambiguity / steward? **0**

## RESOLUTION
14. Existing claims resolved? **0**
15. Structured Address resolved? **0**
16. Coordinate lookup resolved (production)? **0** (Mapbox reverse: no production-eligible place hits on the 4 coord candidates)
17. Official-source resolved? **0**
18. SerpApi resolved? **9** production-eligible (+1 search hit not promoted)
19. Total new City resolutions? **9**
20. Final VALID City (projected)? **1369** (**95.3%**)
21. Final Unknown (projected)? **0** (literals cleared or replaced)
22. Final blank (projected)? **68**
23. Final conflict? **0**
24. Final known-invalid? **0**

## STATE
25. State before? **1306**
26. New State fills from City repair? **0**
27. Final State coverage (projected)? **1306**

## MARKET
28. Market before (valid)? **1052**
29. New deterministic Market unlocked by City? **1**
30. Final valid Market coverage (projected)? **1053**
31. Remaining Market unresolved? **384**
32. New Market registry candidates? **6**

## SUBMARKET
33. New Submarket resolutions (MATCHED fills)? **1**
34. MATCHED final (projected populated)? **660**
35. N/A final? **0** (Airtable field; applicability in queues)
36. UNRESOLVED final (blank)? **777**
37. Applicable resolution %? **45.9**

## ADDRESS / COORDS
38. Address claims/fills gained? **8**
39. Coordinate fills gained? **8** pairs (**16** field mutations)
40. Any `[object Object]` generated? **0**

## SERPAPI
41. Properties queried? **77**
42. Searches? **77**
43. Detail calls? **0**
44. Cache hits? **0**
45. City resolutions per paid search? **0.13**

## PLACEHOLDERS
46. Should literal "Unknown" remain in City factual field? **NO**
47. Recommendation? **blank factual field + research status UNRESOLVED**
48. Other fields with literal placeholders? **0** in this audit

## CORRECTIVE DRY RUN (`21-city-corrective-dry-run.json`)
49. City fills? **9**
50. Unknown clears? **33**
51. State fills? **0**
52. Market corrections? **1**
53. Submarket corrections? **1**
54. Address fills? **8**
55. Coordinate fills? **8** (pairs)
56. Steward? **0**
57. Rights blocked? **0**
58. Unsupported overwrite? **0**

## AUTOPILOT
59. Can future V4 resolve City before Market? **YES**
60. Can City Unknown trigger remediation instead of acting as geography? **YES**
61. Can new Address/Coords reopen City research? **YES**
62. Remaining City Unknowns legitimate bounded gaps? **YES** (68 blank after hygiene; queued)
63. After applying City corrective manifest, can V4 safely resume even with small unresolved City set? **YES**

---

## Interpretation

Most post-cleanup gaps (**77**) are hard cases: weak free evidence (few addresses/coords/URLs). SerpApi Exact/High recovered **9** Cities and unlocked limited downstream Market/Submarket. Projected **≥95%** valid City without inventing values. Remaining blanks stay in `CITY_RESEARCH_EXHAUSTED` with reopen triggers.

**Do not apply. Do not resume V4.**
