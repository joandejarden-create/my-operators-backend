# Addendum — Six leisure-demand categories integration audit

**Hotel reference:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Spend:** $0 · No STAGED/CURRENT changes · No new paid discovery  
**Date:** 2026-11-24  

## Scope clarification

These six categories are the **Leisure Demand Intelligence (LDI) V1 modules**  
(`lib/group-demand-intelligence/leisure-demand-intelligence/`).  

They are **not** the three local-demand research tracks:

| Local-demand track (separate) | Segments |
|-------------------------------|----------|
| `social_celebrations` | social_special_events, corporate_celebrations_private_events, local_catering_day_meetings |
| `planner_partnerships` | professional_event_planners, travel_trade, preferred_lodging, corporate_meetings_incentives |
| `venue_lodging_partnerships` | preferred_lodging, local_catering, social_special_events, professional_event_planners |

LDI wraps subsets of the **14-segment** taxonomy with leisure subtype + pathway qualification.  
**Canonical Full E2E default: leisure OFF** (`runCanonicalFullE2eResearch` has **no** leisure import/call).  
Portfolio can opt in via `-LeisureModules` / `-LeisureOnly` on `Invoke-GdiPortfolioProcessing.ps1`.

---

## Six-row leisure integration matrix

| Category | Code owner | Discovery implemented | Executed in standard E2E | Contact workflow | Candidate projection | Bethesda coverage | Gap | Status |
|----------|------------|----------------------|---------------------------|------------------|----------------------|-------------------|-----|--------|
| 1. Festivals, concerts and cultural events | `leisure_events` → `gdi-leisure-module-registry-v1` + PS `Invoke-GdiLeisureEvents.ps1`; reuses `associations_conferences_events`, `social_special_events`, `corporate_celebrations_private_events` | **Partial** — segment executors + Exa mission mentions “cultural festivals with lodging”; LDI fixture/`mock_live` path exists; **no dedicated leisure festival query pack auto-bound into canonical entry** | **No (explicit)** — overlapping MICE/social segments **yes**; leisure subtype filters **only if LDI opted in** | LDI gate may accept campaign/partnership entry without Full E2E WHO→ALT→HOW; Full E2E contact chain runs only on standard chain candidates | LDI can `planLeisureStagedPackage` (plan only); not auto-merged into hotel-wide FQO candidate | Organic overlap only (e.g. AHIMA/chamber-style rows) — **not** produced by LDI module for Bethesda | Connect LDI events module into canonical post-discovery; hotel-specific leisure queries; FQO-aligned named-person path | **PARTIALLY_IMPLEMENTED** |
| 2. Participatory sports and recreational travel | `leisure_sports` → wraps `sports_entertainment` + sports housing hunter | **Partial** — `sports_entertainment` discovery **yes** in 14-segment; LDI adds participatory filter (wrapper) | **Partial** — sports segment **runs** in standard 14-segment; LDI participatory specialization **does not** | Same as above: sports chain candidates get WHO/HOW; LDI-only path does not auto-run Full E2E contact ladder | Sports segment opps can reach presentation; LDI sports contracts need separate projection | **Strong organic:** MSYSA State Cup, Potomac Memorial, Loudoun showcase, Bethesda Premier Cup, Alexandria/Arlington tournaments, IWLCA — from historical sports/group research, **not** LDI sports module | Wire LDI sports filter into standard sports segment output; avoid double-count vs existing cups | **PARTIALLY_IMPLEMENTED** |
| 3. Attractions and destination experience partnerships | `leisure_attractions` → `preferred_lodging_partnerships`, `travel_trade` + venue lodging track | **Partial** — partnership segment queries exist; leisure “attraction/experience” specialization is LDI wrapper + fixtures | **No (explicit)** — PLP/travel_trade **run**; attractions leisure framing **not** default | Partnership contacts may appear via PLP/planner tracks; not LDI-specific WHO | PLP rows can project; dedicated attractions leisure candidates only via LDI | Thin organic (Strathmore / SoccerPlex-adjacent partnership language); **no** Bethesda LDI attractions run | Dedicated attraction/experience partnership queries + Bethesda module selection | **PARTIALLY_IMPLEMENTED** |
| 4. Universities and VFR demand | `leisure_universities` → `education_scientific` (+ university hunter); LDI specializes graduation / parents weekend / VFR / alumni | **Partial** — education segment is **conference-heavy**; VFR/graduation leisure queries are LDI specialization, not default education missions | **No (explicit)** — education_scientific **runs**; VFR leisure lens **not** | Education opps get contact chain; VFR campaign pathway often lacks named buyer unless researched | Education/alumni rows can appear; pure VFR campaign rows rarely survive FQO without named person | **Partial organic:** Georgetown Homecoming, UMD Alumni Weekend watch, some NIH/education adjacency; research has extras (e.g. NIH Research Festival) | Explicit VFR/graduation query pack; named pathway for campaign-driven university leisure | **PARTIALLY_IMPLEMENTED** |
| 5. Destination demand shifts and special occasions | `leisure_destination` — registry coverage **GAP**; social/corporate celebration segments + need-period strategies | **Defined, thin** — specialization over existing queries; **no** strong dedicated destination-shift discovery pack | **No** — social/celebration segments may run; destination-shift leisure module **not** executed in standard E2E | Not connected as LDI destination path for Bethesda | No LDI destination candidate stream in canonical hotel-wide | Heuristic keyword hits inflate “overflow/homecoming” overlaps; **true destination-shift LDI products absent** | Build destination-shift queries + need-period linkage; connect to canonical | **DEFINED_NOT_EXECUTED** |
| 6. Leisure distribution and referral relationships | `leisure_distribution` → `travel_trade`, `preferred_lodging`, `professional_event_planners` | **Partial** — trade/PLP/planner discovery **yes**; leisure DMC/OTA/CVB/referral specialization is LDI wrapper | **Partial** — underlying segments **run**; leisure-distribution module **opt-in only** | Planner/DMC contacts via those segments’ contact research when in chain | Distribution/planner rows present in gold; LDI distribution contracts separate | **Organic:** Cohera DMC, Linder Global Events, Strathmore partnership-ish, PLP/PEP live rows, Spargo as agency route on events | Leisure-specific referral qualification + dedupe vs planner track; Bethesda LDI preset | **PARTIALLY_IMPLEMENTED** |

**None of the six are FULLY_EXECUTED inside today’s default canonical Full E2E.**  
**None are NOT_IMPLEMENTED** — all six have registry IDs, PowerShell entrypoints, fixtures, contracts, and dedupe.

---

## Canonical wiring facts (do not infer from segment names)

| Check | Result |
|-------|--------|
| Import/call from `gdi-canonical-full-e2e-entry-v1.js` | **Absent** |
| Import/call from `original-full-e2e-research-chain-v1.js` | **Absent** |
| Default portfolio leisure | **Off** unless `-LeisureModules` / `-LeisureOnly` |
| Bethesda LDI preset (`LEISURE_HOTEL_PROFILE_PRESETS`) | **None** |
| Bethesda auto-select without tags | **`moduleIds: []`** |
| Design Hotels LDI cohort presets | LaLuna / Memmo / Condesa only |
| Cross-segment leisure dedupe | Implemented (`gdi-leisure-cross-segment-dedupe-v1`) — used on LDI runs, not on default E2E |
| LDI finalization vs FQO | LDI gate ≠ FQO (can allow campaign/partnership without person-specific channel) |

---

## Bethesda replay implications

1. **Preserve approved 67** — historical baseline unchanged; pointers untouched.  
2. **Earlier Bethesda Full E2E** produced leisure-*adjacent* inventory mainly via **14-segment sports / social / education / travel-trade**, not via the six LDI modules.  
3. **Already in saved research (86)** beyond gold keyword samples: e.g. NIH Research Festival, additional education/overflow IDs — review commercially; do not auto-add.  
4. **New discovery required** for true LDI gaps, especially:
   - festivals/concerts as nonlocal leisure lodging (not association MICE)
   - attractions/experience partnerships
   - VFR / parents-weekend / graduation leisure
   - destination demand shifts / special occasions (**DEFINED_NOT_EXECUTED**)
   - leisure OTA/CVB/referral relationships beyond generic planner/DMC rows  
5. Final candidate **may exceed 67** only if new rows independently meet founder FQO rules (plausible demand, hotel fit, timing, named person, outreach pathway, specific action). **No numerical target.**

---

## Machine-readable matrix

See `14_SIX_LEISURE_CATEGORIES_MATRIX.json`.

## STOP

Audit only. No LDI↔canonical wiring changes, no paid leisure discovery, no STAGED activation until founder reviews Bethesda replay **and** this matrix.
