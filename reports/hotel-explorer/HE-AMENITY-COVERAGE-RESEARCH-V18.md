# HE Amenity Coverage Research & Enforcement (HE-QL-030 / V18)

**Date:** 2026-11-19  
**PR:** [#463](https://github.com/joandejarden-create/my-operators-backend/pull/463)  
**Lesson:** HE-QL-030 · Quality Standard v1.19 §3  
**Hotel:** Bethesda Marriott `recLuxvwwxID7U2B8`

## Diagnosis — why only nine appeared

| Factor | Detail |
| --- | --- |
| Previous displayed count | **9** guest amenities |
| ADP profile tags | 15 total; **6** geo/brand/loyalty/full-service correctly filtered out |
| Remaining guest tags | Meeting & Event Space, Ballroom, Cooper's Mill, Outdoor Pool, 24-Hour Fitness, Peloton, M Club, NIH Access, DC Metro Access |
| Blocked themes | Profile `notes` marked parking / pets / accessibility as **UNVERIFIED**, suppressing recovery |
| UI merge | `resolveAmenitiesDisplay` used **exclusive** verified list when present, so richer HPC `amenitiesDisplay` could not fill the gap |
| Max-20 cap | Not the cause (`overflow=0`) |

Root cause: thin verified guest catalog + outdated UNVERIFIED notes + exclusive verified-only selection — not a display-cap bug.

## Research (no paid APIs)

First-party Marriott.com WASBT overview (already the ADP canonical URL) publishes additional guest amenities:

- Business Center  
- Concierge desk  
- Room Service  
- Complimentary Wi-Fi  
- Pet-Friendly (dogs; fee/limits published)  
- On-Site Parking (paid rates published)  
- Complimentary Metro Shuttle  
- Accessible rooms & facilities  

EV charging explicitly **absent** on the same page — not added.

## Recovery & mapping

| Attribute ID | Label | Source | Prior state |
| --- | --- | --- | --- |
| `business_center` | Business Center | Marriott.com overview | Missing from profile |
| `concierge` | Concierge Service | Marriott.com overview | Missing |
| `room_service` | Room Service | Marriott.com overview | Missing (+ new ATTRIBUTE_DEFINITIONS) |
| `free_wifi` | Complimentary Wi-Fi | Marriott.com overview | Missing (+ def) |
| `pet_friendly` | Pet-Friendly | Marriott.com overview | Blocked UNVERIFIED |
| `on_site_parking` | On-Site Parking | Marriott.com overview | Blocked UNVERIFIED (+ def) |
| `metro_shuttle` | Complimentary Metro Shuttle | Marriott.com overview | Missing (+ def) |
| `room_accessibility` | Accessible Rooms & Facilities | Marriott.com overview | Blocked UNVERIFIED (+ def) |

## Final Bethesda coverage

- Previous displayed: **9**  
- Recovered / independently verified from official materials: **+8**  
- Final verified guest amenities: **17**  
- Displayed: **17** (≤20)  
- Research status: `COVERAGE_OK`  
- Customer completeness badge: **none**

## Persistence

- Fixture SoT updated: `fixtures/ai-demand-positioning/bethesda-marriott-property-profile.json`  
- Airtable apply: **forbidden** (`applyAuthorized: false`)  
- Dry-run machine report: `HE-AMENITY-COVERAGE-RESEARCH-V18.json`  
- Classification: `PERSISTENCE_DESIGNED`

## Shared automation

- `mergeAmenityDisplaySources` — union verified + HPC (dedupe; never invent)  
- `assessAmenityCoverageResearchNeed` — `RESEARCH_GAP` when &lt;12  
- Gates: `amenities_kgpv_strip_and_display_cap` + `amenities_verified_coverage_research_trigger`  
- Lesson **HE-QL-030** / standard **v1.19**

## Multi-hotel validation

See JSON report for KGPV + third hotel enrichment coverage under the same shared selectors.
