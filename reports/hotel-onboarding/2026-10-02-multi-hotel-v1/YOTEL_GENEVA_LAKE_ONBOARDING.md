# YOTEL Geneva Lake — Full New Onboarding

**HPC:** `recrPQcZg7SFARRb2`  
**Identity key:** `yotel_ch_geneva_lake_founex`  
**ADP property id:** `adp_yotel_geneva_lake`  
**Official URL:** https://www.yotel.com/en/hotels/yotel-geneva-lake  
**Meetings URL:** https://www.yotel.com/en/hotels/yotel-geneva-lake/meetings-conferences-events  
**Press (237 rooms):** https://www.yotel.com/en/press/yotel-geneva-lake-officially-opens-its-doors

## Phase 0 — Identity

| Item | Value |
|------|-------|
| Pre-create search | 0 matches (YOTEL / Geneva Lake / Founex) |
| Created | 2026-10-03 via gated stewardship (`scripts/yotel-geneva-lake-hpc-stewardship-apply.mjs`) |
| City | Founex |
| Region | Vaud |
| Country | Switzerland |
| Market | Lake Geneva / La Côte |
| Submarket | Founex / Nyon corridor |
| Rooms | 237 |
| Address | Chemin Ballessert 1, 1297 |
| Geocode | Deferred (address-only; permanent terms not applied) |
| Identity confidence | HIGH |
| Duplicates / collisions | 0 |

## First-party verified capability

| Fact | Value | Source |
|------|-------|--------|
| Guestrooms | 237 | Official press |
| Meeting rooms | 6 flexible | Meetings page + hotel overview |
| Room sizes | ~25–352 sqm (Copenhagen 352; overview also cites 352) | Meetings / overview |
| Max delegates | ~250 | Meetings page |
| Auditorium / Conference Hall | ~270 sqm, up to 250 | Meetings page |
| Hybrid AV | Yes | Meetings page |
| Airport | ~10–15 min; free shuttle (limited hours); park & fly | Official FAQ / overview |
| Nyon | ~10 min | Official |
| Geneva | ~25 min train (via Coppet) | Official FAQ |
| Lausanne | ~30–40 min | Official |
| Need periods | NOT_PROVIDED | No hotel-supplied calendar |

**Geography law:** Do not classify immediate market as downtown Geneva CBD.

## Demand territory model (GDI config)

PRIMARY: Founex / Nyon / Coppet / La Côte  
SECONDARY: Geneva Airport; selected Geneva org/corporate with friction  
ADJACENT: Lausanne / selected cross-border stretch only when realistic  

Config: `config/group-demand-intelligence/hotels/recrPQcZg7SFARRb2.json`

## HI / ADP / GDI state

| Gate | Status |
|------|--------|
| HI_COMPLETE | YES (6/6) |
| Active ADP attributes | 14 |
| ADP_READY_FOR_BASELINE | YES |
| OFFICIAL_BASELINE_PUBLISHED | **NO** |
| GDI seed | WEEKLY_READY (applied) |
| GDI Strict Ready | 0 |
| Future Watch | 0 this sprint |

## ADP demand families (framework)

corporate_business_travel · geneva_airport_stays · meetings_events_corporate · nyon_la_cote_business · international_org_adjacent_travel · airport_park_and_fly · lake_geneva_short_break · lausanne_linked_business_stretch

## Remaining blockers

1. Persist Meeting Room Count = 6 + meeting space totals into HI commercial → clear ADP missingCritical  
2. Persist demand nodes to HI Airtable after Webhound close  
3. Permanent geocode  
4. Run first market-first GDI discovery cycle  
5. Do not publish official baseline until pilot instruction  

## Verdict

**ADP READY — GDI MORE RESEARCH REQUIRED**
