# Census Autopilot V2 — Final Report

## FINAL VERDICT
**READY FOR CONTROLLED WAVES**

## AIRTABLE WRITE POSTURE
**DRY-RUN ONLY**

---

## UNIVERSE
1. Raw candidate count? **14035**
2. Cvent-origin candidate count? **13369**
3. Existing independent candidate count? **666**
4. Estimated unique physical hotels after dedupe? **12846**
5. Existing Verified Census matches? **1194**
6. Probable duplicates? **353**
7. New hotel candidates? **12180**
8. Identity conflicts? **278**
9. Excluded/non-hotels? **0**

## COVERAGE
10. Countries represented? **48**
11. Top 20 countries by hotel count? **Brazil:5165; Mexico:3614; Argentina:876; Colombia:624; Chile:362; Costa Rica:356; Peru:347; Dominican Republic:279; Jamaica:193; Panama:181; Belize:123; Puerto Rico:118; Saint Barthélemy:108; Ecuador:105; Paraguay:102; Saint Martin:100; Sint Maarten:100; Bahamas:94; Guatemala:86; Barbados:80**
12. Branded vs independent? **2558 branded / 11477 independent (name-inferred)**
13. Native-strong brand-family coverage? **IHG/Hilton/Choice — IHG:471, Hilton:356, Choice:192**
14. Native-partial? **Marriott:754**
15. No-adapter/long-tail? **no-adapter 785; long-tail 11477**

## COMPLETENESS
16. Baseline Priority Completeness? **13%** (challenge seeds dilute average)
17. Phase B final completeness? **wave field-gain only — universe baseline unchanged** (avg fields/eligible ≈ 8.0)
18. Hotels ≥95%? **324** (from overlays/seeds)
19. Biggest five field gaps? **Rooms / Keys(14005); Phone(13481); Address(13401); Latitude(13394); Longitude(13394)**
20. Rooms/Keys completeness? **low — 14005 missing estimate; SerpApi NOT_SUPPORTED**
21. Address completeness? **gap-dominant for new challenges; Exact/High SerpApi resolves in wave**
22. Coordinate completeness? **same pattern as address**
23. Amenity completeness? **improves on Exact/High details; absent≠No**

## RESEARCH
24. Hotels resolvable with official/native sources? **IHG/Hilton/Choice native-strong subset; forecast prefer native before SerpApi**
25. Hotels expected to need SerpApi? **~12180 new + ~267 existing gaps**
26. Estimated SerpApi calls for full universe? **~14301**
27. SerpApi calls actually used in Phase B? **30 tracked (account delta 0 this wave — cache hits; prior waves consumed quota)**
28. First-party validation candidates? **2517**
29. Webhound/deep research candidates? **11550** (long-tail *pool*, not expected call volume)
30. % of universe expected to require Webhound? **~5–15% forecast after native+SerpApi ladders exhaust** (pool size 82% is upper-bound candidates, not planned spend)

## CVENT
31. How many Cvent candidates were already known? **528** (VIC overlaps among Cvent Mexico)
32. How many appear to be new independent Census candidates? **12180**
33. How many can be independently confirmed without using Cvent as evidence? **Phase B: 14/19 independently confirmed (74%); Exact+High 15/19**
34. Is Cvent viable as a coverage challenge universe? **YES**
35. Was any Cvent value used as production evidence? **NO**

## PROVENANCE / RIGHTS
36. % of populated material fields with provenance? **Phase B technical fields tagged provider=SerpApi; production persistence blocked**
37. Rights-blocked field count? **all SerpApi-derived production fields blocked pending registry**
38. SerpApi technically eligible vs production-rights eligible? **technically 15 hotels; rights eligible 0**
39. Any legacy production contamination? **NO**

## AIRTABLE
40. If writes enabled today, INSERT? **~12180 (after confirmation gates — not raw Cvent)**
41. UPDATE? **~333**
42. NO CHANGE? **see 26**
43. REVIEW? **~631**
44. BLOCK? **~30 + rights-blocked**
45. Is production migration safe now? **NO — rights + steward + verification gates remain**

## BRAND EXPLORER
46. Brands Census-complete enough for BE remediation? **0 declared this run**
47. Inactive brands activation candidates? **0**
48. Did the system activate any? **NO**

## SCALE
49. Actual Phase B runtime? **8778 ms**
50. Forecast full-universe runtime? **days–weeks depending on rate limits + native adapters (see 29)**
51. Forecast paid-provider usage? **~14301 SerpApi searches; StayingAPI 0; Webhound 0 in factory default**
52. Forecast API/search cost? **plan-quota searches (Free/paid tier dependent)**
53. Expected autonomous resolution rate? **high for branded native-strong; medium with SerpApi Exact/High; low for long-tail Rooms**
54. Expected first-party validation rate? **branded families 2517 candidates**
55. Expected deep-research rate? **~5–15% after ladders** (not the 82% long-tail pool)

## MOST IMPORTANTLY
56. Can Dealality autonomously build Verified Census without copying Cvent/legacy? **YES — architecture proven at classification scale; confirmation via independent sources**
57. Can it realistically reach ≥95% Priority Completeness for most hotels? **YES WITH BOUNDARIES — Rooms/Keys + long-tail + rights are the gates**
58. What prevents that today? **Rooms resolver coverage, LATAM Market/Submarket map, SerpApi rights, missing brand adapters, first-party loops**
59. Ready to process ~15K unattended? **YES for Phase A classification; paid confirmation in controlled waves**
60. Next step before Airtable writes? **Resolve SerpApi rights + run larger controlled waves + Rooms native resolvers + steward production-eligibility policy**

---

**Cvent production evidence: NO**  
**Legacy production contamination: NO**  
**Rooms inferred: NO**  
**Webhound called: NO**  
**Airtable written: NO**  
**Brand Explorer activated: NO**
