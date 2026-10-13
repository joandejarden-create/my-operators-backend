# GDI Live Commercial Quality V1 — Bethesda Dry-Run / Final

Marker: `gdi_live_commercial_quality_v1`
As-of: `2026-09-22`
Apply: **YES**
IDs preserved: **YES**

TOTAL LIVE OPPORTUNITIES: **38**

| Class | Count |
|---|---:|
| NO_CHANGE | 0 |
| DATE_CORRECTION | 1 |
| FUTURE_CYCLE_CORRECTION | 8 |
| GEOGRAPHY_CORRECTION | 0 |
| VENUE_CORRECTION | 0 |
| OVERFLOW_CORRECTION | 0 |
| ROOM_DEMAND_CORRECTION | 0 |
| ATTENDANCE_ENRICHMENT | 0 |
| PEAK_ROOMS_ENRICHMENT | 0 |
| CONTACT_UPGRADE | 0 |
| SOURCE_CORRECTION | 2 |
| ACTION_CORRECTION | 3 |
| SERIES_GROUPING | 38 |
| DUPLICATE_RESOLUTION | 0 |
| NEEDS_REVIEW | 0 |

## Completeness (post-projection)

| Metric | % |
|---|---:|
| date | 81.6 |
| attendance | 18.4 |
| peakRooms | 42.1 |
| venue | 60.5 |
| roomDemandThesis | 100 |
| namedWho | 28.9 |
| contactableWho | 31.6 |
| source | 94.7 |
| defensibleAction | 26.3 |

## Sample corrections

- **NADO & DDAA Washington Conference 2028 — Arlington, VA (hotel not yet named)** — SERIES_GROUPING · date `2028-03-19` → display `2028-03-19 – 2028-03-22`
- **AHIMA Advocacy Summit 2027 — Washington, DC (hotel not specified)** — SERIES_GROUPING · date `2027-03-15` → display `2027-03-15 – 2027-03-16`
- **NADO & DDAA Washington Conference 2027 — Crystal Gateway host (overflow play)** — SERIES_GROUPING · date `2027-03-07` → display `2027-03-07 – 2027-03-10`
- **2027 MSYSA Spring State Cup Championships — multi-weekend SoccerPlex demand** — SERIES_GROUPING · date `null` → display `Date not yet confirmed`
- **ACC Legislative Conference 2027 — Washington, DC (hotel not named)** — SERIES_GROUPING · date `2027-10-24` → display `2027-10-24 – 2027-10-26`
- **2027 AAN Annual Meeting — Washington, DC (likely convention-scale; not a Bethesda Marriott host fit)** — SERIES_GROUPING · date `2027-05-01` → display `2027-05-01 – 2027-05-05`
- **47th Annual Potomac Memorial Tournament 2027 — stay-to-play weekend demand** — SERIES_GROUPING · date `null` → display `Date not yet confirmed`
- **18th Annual NICE Conference and Expo 2027 — location TBD (Jun 7–9)** — SERIES_GROUPING · date `2027-06-07` → display `2027-06-07 – 2027-06-09`
- **SHOW 2026 — NIH Research Conference on Sleep and the Health of Women (Oct 14–16)** — SERIES_GROUPING · date `2026-10-14` → display `2026-10-14 – 2026-10-16`
- **Loudoun Soccer College Showcase 2027 — stay-to-play (HBC housing)** — SERIES_GROUPING · date `2027-03-05` → display `2027-03-05 – 2027-03-07`
- **2027 NTCA Legislative and Policy Conference — Hyatt Regency Washington on Capitol Hill** — FUTURE_CYCLE_CORRECTION, SERIES_GROUPING · date `2027-04-18` → display `2027-04-18 – 2027-04-20`
- **AMWA 113th Annual Meeting 2028 — destination TBD** — FUTURE_CYCLE_CORRECTION, SERIES_GROUPING · date `2028-03-23` → display `2028-03-23 – 2028-03-26`
- **World Biomaterials Congress 2028 — WEWCC (medical overflow watch)** — SERIES_GROUPING · date `2028-04-24` → display `2028-04-24 – 2028-04-29`
- **ASAE Annual Meeting & Exposition 2029 — Washington, DC (early watch)** — SERIES_GROUPING · date `2029-08-11` → display `2029-08-11 – 2029-08-14`
- **2027 PTAB Bar Association Annual Conference — Ritz-Carlton Washington, DC** — FUTURE_CYCLE_CORRECTION, SERIES_GROUPING · date `2027-03-17` → display `2027-03-17 – 2027-03-19`
- **Arlington Spring Tournament 2027 — hotel information forthcoming** — SERIES_GROUPING · date `2027-02-26` → display `2027-02-26 – 2027-03-07`
- **2027 National Conference on Trusteeship — Washington Hilton (contracted)** — SERIES_GROUPING · date `2027-03-13` → display `2027-03-13 – 2027-03-15`
- **Georgetown Homecoming Weekend 2027 — recommended-hotel outreach window** — FUTURE_CYCLE_CORRECTION, SERIES_GROUPING · date `null` → display `Date not yet confirmed`
- **ADA 2027 Scientific Sessions — Washington D.C. (convention-scale; primary host unfit)** — SERIES_GROUPING · date `null` → display `Date not yet confirmed`
- **2026 NIH Research Festival — campus event with vendor/attendee overflow lodging potential** — DATE_CORRECTION, ACTION_CORRECTION, SERIES_GROUPING · date `2026-09-14` → display `2026-09-14 – 2026-09-18`
- **University of Maryland alumni / parents weekend demand — monitoring for 2027 dates** — SOURCE_CORRECTION, ACTION_CORRECTION, SERIES_GROUPING · date `null` → display `Date not yet confirmed`
- **AMWA 112th Annual Meeting 2027 — Washington, DC area (hotel/venue not named)** — SERIES_GROUPING · date `2027-03-11` → display `2027-03-11 – 2027-03-14`
- **NDSS Down Syndrome Advocacy Conference 2027 — hotel block TBA** — SERIES_GROUPING · date `2027-04-19` → display `2027-04-19 – 2027-04-21`
- **ASA ADVANCE 2027 — already contracted at Gaylord National (National Harbor)** — SERIES_GROUPING · date `2027-01-22` → display `2027-01-22 – 2027-01-24`
- **NAR GAD Institute 2027 — Arlington, VA (hotel/venue not named)** — SERIES_GROUPING · date `2027-07-27` → display `2027-07-27 – 2027-07-29`
