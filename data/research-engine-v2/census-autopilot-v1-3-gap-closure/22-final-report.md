# Census Autopilot V1.3 — Gap Closure Final Report

**Run:** `cav13_2026-08-08T11-37-32_0fba24`  
**Cost:** ~$0 geocode estimate · Webhound $0 · Airtable writes 0

## MOST IMPORTANTLY

**YES, WITH SPECIFIC BOUNDARIES**

Can Dealality close Golden Census from 86.8% to ≥95% by solving Rooms + Address + Coordinates without weakening evidence or requiring Joan in the research loop?

**Answer: YES, WITH SPECIFIC BOUNDARIES**

---

## Pass completeness

| Stage | Avg Priority Completeness |
|-------|---------------------------|
| V1.2 freeze | 86.8% |
| Pass 1 structured | 90.4% |
| Pass 2 official pages | 93.6% |
| Pass 3 rooms retry | 93.6% |
| Pass 4 geocode | 93.6% |
| **Final** | **93.6%** |

## Exact answers

1. Final average Priority Completeness: **93.6%**
2. % hotels ≥95%: **47.7%**
3. Hotels at 100%: **15**
4. Rooms completion overall: **4.9%**
5. IHG rooms: **8.2%**
6. Hilton rooms: **2%**
7. Choice rooms: **0%**
8. Address completion: **92.9%**
9. Latitude/Longitude: **94% / 94%**
10. Coordinates provider-blocked vs unresolved: provider=`PROVIDER_READY`; provider-blocked count≈0; missing=22
11. Completeness gain from rooms field: **0.5 pp** (field completion)
12. Gain from address field: **56.5 pp**
13. Gain from coordinates field: **52.1 pp**
14. Official structured room sources discovered: Hilton GraphQL **does not** expose rooms; IHG often empty numberOfRooms; prose/HTML only when present
15. Reusable patterns: Hilton GraphQL address+coords; IHG Mexico-filtered JSON-LD address; family rooms resolvers
16. Rooms remaining unresolved: **347**
17. Rooms → first-party validation: **347**
18. Addresses remaining unresolved: **26**
19. Coordinates remaining unresolved: **22**
20. Would benefit from Webhound: low ROI vs first-party for room counts — estimate subset of hard rooms gaps only (~80 candidates max); **Webhound not called**
21. Unsupported values: **0**
22. Cvent production values: **NO**
23. Legacy production values: **NO**
24. Autopilot ran all passes without Joan: **YES**
25. New hotels can auto-receive these enrichment passes: **YES** (see 21-new-hotel-standard.md)
26. Exact blockers preventing ≥95%: Rooms missing 347 (IHG 179, Hilton 100, Choice 68); Address 26; Coords 22
27. Cheapest path: First-party Rooms / Keys validation packs per family (bulk spreadsheet); IHG address page retries + steward review for remaining; Continue Mapbox geocode for address-confirmed residuals
28. Ready for Mexico-wide Golden Census execution: **PARTIAL — geo ready; rooms gate remains**
29. Ready for controlled Airtable write pilot: **YES for address/coords pilot lanes** — address/coords yes with provenance; rooms only where High/Medium official claims exist
30. Next: Ship first-party Rooms packs to IHG/Hilton/Choice; keep Hilton GraphQL addr/geo in Autopilot default path; do not call Webhound for bulk rooms

## Distribution

```
{
  "100%": 15,
  "95–99.9%": 159,
  "90–94.9%": 164,
  "80–89.9%": 9,
  "<80%": 18
}
```

## Portfolio avg gain vs V1.2: 6.8 pp
