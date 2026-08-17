# Census Autopilot V2.2 — Final Report

**Version:** census-autopilot-v2.2-official-first-rooms  
**Mode:** DRY-RUN ONLY · No Airtable · No Webhound · Cvent never production evidence

---

## OFFICIAL-FIRST

1. **% universe with official/native research paths:** **18%** (branded+directory families; Independent remain long-tail)
2. **% likely confirmable without SerpApi:** **~5%** of unique hotels (native-strong/partial absorb estimate)
3. **Top 10 families by Census ROI:** IHG, Hilton, Marriott, Choice, Wyndham, Accor, Hyatt, Melia, Barcelo, Best Western
4. **New/strengthened adapters:** resolveFromOfficialSources (family-routed); Rooms Resolver V3 (JSON-LD + embedded + owner + Hilton GraphQL + FP classification); SerpApi EV gate + one-call analyzer; Dealality Market/Submarket expansion helpers; First-party validation ingestion model
5. **Official property-ID coverage before vs after:** VIC with IDs **666/666** → wave captured **120** official IDs (24% of wave)

## ROOMS

6. **Why V2 failed 0/55:** Correct IHG hoteldetail pages (200) with explicit empty `numberOfRooms`; no High/Medium total in HTML/JSON-LD; Hilton/Choice not in seed slice. Parser correctly refused to invent.
7. **V3 new sources:** JSON-LD `numberOfRooms`, embedded state totals, owner/operator standalone ladder, Hilton GraphQL shortDesc prose, first-party validation classification for empty IHG fields
8. **Rooms baseline coverage:** **0%** (V2.1 wave Rooms success)
9. **Rooms final coverage (confirmed):** **3%**
10. **Rooms V3 success rate:** **8%** (10/120)
11. **Strong Rooms solutions:** None fully strong in public HTML for IHG Mexico; Hilton GraphQL prose **occasional**; Marriott/Accor/Hyatt **partial via fact sheets/owner**
12. **Require first-party validation:** IHG (empty numberOfRooms cohort), Choice sparse, most Melia/Minor/RIU/Barceló
13. **Require deep research:** Independents without owner pages; blocked official HTML

## SERPAPI

14. **Prior full-universe forecast:** **14301**
15. **New forecast:** **9560**
16. **% reduction vs old full:** **33%** (vs V2.1 minimized: **23%**)
17. **Production-wave searches used:** **654**
18. **Searches per confirmed hotel:** **1.74**
19. **Searches per ≥95% hotel:** **65.4**
20. **% SerpApi hotels one-call:** **77%**
21. **Second calls avoided:** **90**

## 500-PROPERTY REAL WAVE

22. **Hotels processed:** **500**
23. **Independently confirmed (incl. enrichment):** **375** (75%)
24. **Official-only confirmed:** **120**
25. **SerpApi-assisted confirmed:** **255**
26. **Exact/High:** **375**
27. **Probable:** **49**
28. **Unresolved:** **76**
29. **Duplicates:** **0** (wave excluded prior V2.1 IDs; PID dedupe at freeze)
30. **Identity conflicts:** **0** flagged in-wave (conflicts remain in universe queue outside wave)

## COMPLETENESS

31. **Baseline Priority Completeness (avg):** **36%**
32. **Final average (confirmed):** **91%**
33. **Median (confirmed):** **93%**
34. **Hotels ≥95%:** **10**
35. **% ≥95%:** **3%**
36. **% ≥95% excluding Rooms (diagnostic):** **66%**
37. **Otherwise Golden-complete except Rooms:** **238**

## FIELDS (confirmed hotels)

38. **Rooms:** **3%**
39. **Address:** **100%**
40. **Coordinates:** **100%**
41. **Phone:** **95%**
42. **Website:** **94%**
43. **Amenities:** **65%**
44. **Market:** **100%**
45. **Submarket:** **14%**
46. **Official property ID:** **32%**

## ESCALATION

47. **First-party validation:** **490** / **98%** of wave
48. **Deep research:** **4** / **1%**
49. **Webhound candidates:** **129** / **26%** (queue only — not called)
50. **Top escalation reasons:** see `24-webhound-escalation-queue.json`

## VERIFIED CENSUS

51. **Can VERIFIED while Rooms pending?** **YES** (recommended)
52. **Lifecycle model:** VERIFIED — GOLDEN COMPLETE · VERIFIED — MATERIAL GAPS · VERIFIED — ROOMS PENDING · VERIFIED — FIRST-PARTY VALIDATION PENDING · PARTIAL — IDENTITY/RESEARCH
53. **VERIFIED PROPERTY:** **375**
54. **VERIFIED — GOLDEN COMPLETE:** **4**
55. **VERIFIED — ROOMS PENDING:** **244**

## AIRTABLE (dry-run counts)

56. **AUTO_WRITE_SAFE:** **1673**
57. **CORROBORATED_WRITE:** **2220**
58. **STEWARD_REVIEW:** **0**
59. **FIRST_PARTY_VALIDATION:** **106**
60. **BLOCKED_RIGHTS:** **1495**
61. **PROHIBITED:** **375**
62. **If SerpApi rights approved:** Address/coords/phone/website/amenities Exact·High staging fields become CORROBORATED_WRITE-eligible (still not Rooms)

## SCALE

63. **Revised full-universe SerpApi demand:** **9560**
64. **Forecast native/official resolution %:** **~18%** path-available / **~5%** likely without SerpApi
65. **Forecast first-party validation %:** **~98%**
66. **Forecast deep research/Webhound %:** **~26%**
67. **Estimated full-universe runtime:** see `25-scale-economics.json`
68. **$150/mo SerpApi tier sufficient for initial reconstruction?** **LIKELY YES if routing holds**

## MOST IMPORTANTLY

69. **Paid searches only where material Census value?** **YES** — EV gate + official-first + phone-only skip
70. **Materially reduced SerpApi dependence via official-first?** **YES** — forecast 12400 → 9560
71. **Rooms/Keys technically solvable for meaningful portion?** **PARTIAL** — public IHG structured Rooms largely absent; hybrid required
72. **First-party validation solve meaningful Rooms gap?** **YES** — primary path for IHG empty-field cohort + Choice/Independents
73. **Begin real full Census universe in autonomous waves without generic benchmarking?** **YES — controlled autonomous waves** (not another 25/250 benchmark)
74. **Blockers before governed Airtable writes:** SerpApi written rights; Rooms/first-party pipeline; steward enablement of VERIFIED lifecycle; Class A geography auto-writes only until rights clear

---

## FINAL VERDICTS

| Area | Verdict |
|------|---------|
| **RESEARCH** | **READY FOR CONTROLLED WAVES** (autonomous, resumable, real queue — not generic benchmarks) |
| **ROOMS** | **HYBRID NATIVE + FIRST-PARTY REQUIRED** |
| **AIRTABLE** | **DRY-RUN ONLY** |
| **SERPAPI** | **TECHNICALLY READY — RIGHTS BLOCKED** |

---

### Change Impact Classification: **High** (research architecture / Rooms / eligibility) — no production writes executed.  
### Rollback: disable `npm run census:autopilot-v2-2-official-first-rooms`; delete or ignore `data/research-engine-v2/census-autopilot-v2-2-official-first-rooms/`.
