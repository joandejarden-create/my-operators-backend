# Full Production Census Retroactive Cleanup — Dry Run Final Report

**Status:** DO NOT APPLY · V4 PAUSED  
**Audited:** 2026-08-08 · Entire live `Hotel Property Census` (`tbl9aY5ijiuIzzWam`)  
**Manifest:** `16-full-cleanup-manifest-dry-run.json` (`FULL_PRODUCTION_CENSUS_CLEANUP_MANIFEST`)  
**Scope:** Full table (**1,437** records) — **not** limited to V3/V3.1 400

---

## Verdicts

| Gate | Verdict |
| --- | --- |
| FULL-TABLE AUDIT | **COMPLETE** |
| RETROACTIVE CLEANUP | **READY FOR AUTHORIZATION** |
| PRODUCTION DATA QUALITY | **PARTIAL** (until SAFE apply) |
| V4 | **NEEDS MORE WORK** (apply cleanup + post-write validation first) |

---

## Exact answers (Q1–75)

### FULL TABLE
1. Total Hotel Property Census records? **1437**
2. Records audited? **1437**
3. Property Identity missing? **0**

### ADDRESS
4. Valid Address? **838**
5. Blank Address? **561**
6. `[object Object]`? **38**
7. Other serialization artifacts? **0**
8. Safe Address corrections/fills? **38** (all `SAFE_INVALID_CLEAR` of object serialization — no recoverable claim formatter hit)
9. Address requiring research? **599**

### CITY
10. Valid City? **1344**
11. Blank? **15**
12. Unknown? **39**
13. Country-as-City? **16**
14. Postal/CEP-as-City? **3**
15. Marketing/invalid City? **20** (invalid residual after country/postal buckets)
16. Safe City corrections? **39** (16 value corrections + 23 invalid clears)

### STATE
17. State/Region populated? **1270**
18. Blank? **167**
19. Safe deterministic fills? **36**
20. Unresolved? **131**

### MARKET
21. Canonical valid Market? **947** (919 canonical + 28 city=market via explicit registry)
22. Country-as-Market? **0** (prior incident clears held)
23. State-as-Market invalid? **72**
24. City-as-Market invalid? **120**
25. Blank/Unresolved? **298** (293 blank + 5 invalid)
26. Safe Market corrections? **105**
27. Safe invalid Market clears? **141**
28. New Market registry candidates? **141**

### SUBMARKET
29. MATCHED? **462**
30. NOT_APPLICABLE? **0**
31. UNRESOLVED? **975**
32. Safe Submarket corrections? **240**

### COORDS
33. Valid Lat/Lng? **828**
34. Missing? **609**
35. Invalid? **0** (malformed/out-of-bounds not detected in this pass)
36. Safe fills? **0** (no production-eligible claim recovery in dry-run)

### PHONE
37. Valid property-direct Phone? **616** (populated; type not fully split in this pass)
38. Missing? **821**
39. Central-reservation only? **null / not classified in this dry-run**

### CURRENT BRAND
40. Correct Current Brand? **1400**
41. Parent/family contamination? **13** (2 Choice Hotels; 11 Wyndham/Hyatt/Marriott family labels)
42. Blank? **24**
43. Safe brand corrections? **0** (all 13 → STEWARD_REVIEW; no auto brand overwrite)

### GOLDEN QUALITY
44. Current average completeness? **75.7**
45. Current average quality? **65.6**
46. Expected completeness after safe cleanup? **~78.9**
47. Expected quality after safe cleanup? **~74.1**
48. Records currently semantically invalid? **273**
49. Expected invalid after safe cleanup? **13** (remaining steward brand cases)

### CLEANUP
50. Total records with ≥1 proposed change? **540**
51. Total field mutations? **612**
52. Invalid-value corrections? **16**
53. Object-format corrections? **0** (objects cleared, not reformatted)
54. Brand corrections? **0**
55. Blank fills? **0**
56. Derived geography? **36**
57. Invalid clears? **202**
58. Market corrections? **105**
59. Submarket corrections? **240**
60. Steward review? **13**
61. Rights blocked? **0**
62. Cvent evidence used? **NO**
63. Legacy evidence used? **NO**
64. Unsupported overwrite? **0**

### AUTOPILOT
65. Will V4 retroactively maintain existing hotels? **YES** (design in `22-v4-retroactive-maintenance-design.md`)
66. Will incomplete hotels remain in remediation queues? **YES**
67. Can V4 revisit when new source/adapter available? **YES**
68. Can V4 correct future semantic problems before writing? **YES** (gates already in stack)
69. Can `[object Object]` reach Address after repair? **NO**
70. Can Country auto-fill Market? **NO**
71. Can parent/family auto-fill Current Brand? **NO**

### MOST IMPORTANTLY
72. Dry-run covers ENTIRE live Census (not only 400)? **YES**
73. After apply, Autopilot continuously improve old + new? **YES** (when V4 resumes under maintenance design)
74. Issues remaining after safe full-table cleanup?
   - Address/City/Market/Coord/Phone/Rooms research queues
   - ~141 Market registry candidates needing curated activation
   - 13 Current Brand steward cases
   - Submarket UNRESOLVED where corridors exist but no match
   - Legitimate Unknown geography
75. Full retroactive cleanup ready for authorization? **YES** (SAFE classes only; steward held)

---

## Inventory snapshot

| Country | n |
| --- | ---: |
| Mexico | 834 |
| Dominican Republic | 194 |
| IHG family rows | 263 |
| Marriott | 452 |
| Choice | 168 |
| Hilton | 199 |

All **1,437** records have Property Identity Key.

---

## Paid research

Deferred (`14-research-results.json`). Plan in `13-paid-research-plan.json` — claims/cache → official → deterministic → SerpApi high-value only after authorization.

---

## Next step (human)

1. Review `16-full-cleanup-manifest-dry-run.json`
2. Authorize SAFE apply (pilot then remainder) — **do not resume V4 yet**
3. Post-write re-audit → then V4 resume readiness re-check
