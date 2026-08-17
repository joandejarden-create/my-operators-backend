# V2 Baseline (input to V2.1)

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
30. % of universe expected to require Webhound? **~5–15% forecast after native+SerpApi ladders exha

---
Dedup summary: {"total_classified":14035,"existing_verified":1194,"existing_needs_enrichment":0,"probable_duplicates":353,"new_property_candidates":12180,"identity_conflicts":278,"insufficient_identity":30,"excluded_non_hotel":0,"estimated_unique_physical_hotels":12846}
Old SerpApi forecast: 14301
