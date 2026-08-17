# Census Autopilot V2.1 — Final Report

## RESEARCH VERDICT
**READY FOR CONTROLLED WAVES**
(toward FULL AUTONOMOUS RESEARCH — blocked by Rooms scale + SerpApi rights + LATAM geography depth)

## AIRTABLE VERDICT
**DRY-RUN ONLY**

## SERPAPI VERDICT
**TECHNICALLY READY — RIGHTS BLOCKED**

---

1. Hotels in controlled wave? **250** (frozen 250)
2. Countries represented? **9** — Bahamas, Brazil, Chile, Costa Rica, Dominica, Grenada, Mexico, Panama, U.S. Virgin Islands…
3. Branded vs independent? **137 / 113**
4. Cvent-origin vs existing? **195 / 55**
5. Independently confirmed? **129** (plus **47** existing VIC enrichment successes)
6. Exact? **24** (confirmation class) / best_level Exact also present in match rows
7. High? **105**
8. Probable? **25**
9. Duplicate? **0 in-wave (pre-deduped)**
10. Identity conflict? **0 auto-classified in-wave**
11. Non-hotel? **0** (note: some weak Cvent titles are non-property noise → insufficient)
12. Unresolved? **49** (41 insufficient + 8 CVENT_ONLY_UNRESOLVED)
13. Confirmation rate overall? **51.6% independently confirmed; 70.4% useful (incl. VIC enrichment)**
14. Named branded? **80.3% useful / independently+enrichment**
15. Named independent? **62.1%**
16. Cvent challenges? **66.2%**
17. Baseline Priority Completeness (proxy)? **26%**
18. Final Priority Completeness (proxy)? **83%**
19. Hotels ≥95%? **155**
20. % hotels ≥95%? **62%**
21. Average fields added? **7.3**
22. Rooms baseline (wave native targets)? **0 resolved pre-resolver**
23. Rooms final? **0/55**
24. Rooms resolver success rate? **0%** (honest Unknown — IHG page extract returned no High/Medium keys this wave; no inference)
25. Biggest remaining Rooms source gap? **Official structured Rooms still thin on IHG hoteldetail HTML; Choice 403; Hilton not in this VIC seed slice; independents need first-party/deep research**
26. Address completeness (wave technical)? **176/250**
27. Coordinates? **176/250**
28. Phone? **171/250**
29. Website? **169/250**
30. Amenities? **172/250**
31. SerpApi calls used? **tracked 370; account delta 353**
32. Calls per independently confirmed? **2.74**
33. Calls per production candidate? **n/a (rights blocked)**
34. Old full-universe forecast? **14301**
35. Revised forecast? **12376** minimization floor; wave economics ~2.74/confirmed ⇒ do **not** extrapolate to 15k blindly — prefer official-first + cache + PID dedupe waves
36. Expected searches saved vs naive 14.3k? **≥1.9k (13.5%)** from minimization plan; more with stronger official-first
37. AUTO_WRITE_ELIGIBLE if rights allowed? **129** independently confirmed staging candidates
38. Steward review? **74**
39. First-party validation? **portfolio-scale (Rooms gaps dominate)**
40. Deep research? **55+ Rooms escalations this wave + long-tail pool**
41. Eligibility policy strong enough for unattended routine writes? **YES for Class A/B after rights; NO overall until rights + Rooms**
42. Class A fields? **Dealality geo (Country/Continent/Sub-Continent/Market/Submarket/City/State)**
43. Class B/C/D/E? **See 15-field-write-classes.json**
44. SerpApi rights questions remaining? **All 12 in 05-serpapi-rights-decision-needed.md** (Joan message ready)
45. SerpApi technical integration ready? **YES**
46. SerpApi production persistence ready? **NO**
47. Top 10 first-party targets? **Independent, Marriott, IHG, Hilton, Wyndham, Accor, Choice, Hyatt, Melia, Minor**
48. Full-universe Webhound escalation %? **~5–15% after ladders**
49. Autonomously research without Joan per step? **YES (routine)**
50. Autonomously decide stop? **YES (stop conditions defined)**
51. Autonomously decide escalation? **YES (policy in 21)**

## Failure analysis (non-confirmed)
- query/name mismatch: 32
- insufficient independent evidence: 21
- Google coverage/empty: 9
- bad Cvent identity: 8
- city/match gate: 4

## MOST IMPORTANTLY
52. Process ~12,846 unique properties in controlled autonomous waves? **YES**
53. Reach ≥95% for MOST hotels without weakening evidence? **NOT YET — Rooms + rights + LATAM geo are gates; trajectory yes (proxy 62% ≥95% this wave after SerpApi fields)**
54. What prevents it? **Rooms coverage (0/55 this wave); SerpApi persistence rights; Market/Submarket LATAM map; missing brand adapters; cohort country breadth (9 countries — expand next wave)**
55. Gates before governed Airtable writes? **Written SerpApi rights; steward Class A/B enablement; Rooms Class D path; duplicate-safe INSERT/UPDATE transactions; no Cvent contamination**

---

Cvent production evidence: **NO**  
Rooms inferred: **NO**  
Airtable written: **NO**  
Webhound called: **NO**
