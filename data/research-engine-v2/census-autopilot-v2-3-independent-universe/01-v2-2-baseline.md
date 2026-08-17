# V2.2 Baseline → V2.3

Version: census-autopilot-v2.3-independent-universe

## What V2.2 proved
- Real 500-wave: 375/500 confirmed (75%), SerpApi ~1.74/confirmed, forecast 14.3k→9.6k
- Rooms hybrid + first-party; 238 hotels Golden-except-Rooms
- Official-first works; Cvent already quarantined as non-production evidence

## What V2.3 must prove
**Can Dealality build its own hotel universe without depending on Cvent as the seed list?**

Cvent → temporary blind coverage challenge / recall benchmark — NOT discovery dependency.

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
15. **New f
