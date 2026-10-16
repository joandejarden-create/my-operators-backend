# Hotel identity resolution — four missing Marriott properties

**Generated:** 2026-10-16T07:44Z (search) / preparation docs updated same day  
**Workroot:** `C:\Dev\dealality\my-operators-backend` @ `origin/main` `248a3098`  
**Mode:** Read-only identity resolution + local configuration preparation. **No production HPC creates.**

**Evidence:** `HOTEL_IDENTITY_HPC_SEARCH_V4.json` (Airtable access **200 OK** — failed access would be LOOKUP_BLOCKED, not absence).

## Is HPC membership required for GDI identity?

**Yes for this US Marriott queue.** Verified control: Renaissance NY Times Square (`recG66DQJKP2c0UNh`) uses the HPC record id as `hotelId` / `aliases.censusRecordId`. Coordinator `loadHotelConfig` keys off that id. Local GDI configs, hotel data dirs, and ADP/fixture scans found **no** alternate canonical registry entries for WASFP / WASPY / CLTCC / TPAAP. Creating a non-HPC duplicate would not satisfy the supported identity path — it would invent a parallel key.

## Dispositions (search complete)

| Key | Hotel | Disposition | Matched ID | Notes |
|-----|-------|-------------|------------|-------|
| WASFP | Falls Church Marriott Fairview Park | **VERIFIED_ABSENT** | — | 0 candidates across URL/code/name/address/city+brand; 0 local registry hits |
| WASPY | Renaissance Arlington Capital View | **VERIFIED_ABSENT** | — | 0 candidates; Residence Inn Capital View **not** used; Capital View near-miss control = 0 |
| CLTCC | Charlotte Marriott City Center | **VERIFIED_ABSENT** | — | 1 excluded false positive: Charlotte Amalie USVI Autograph (`rec2lHaSs1FtMX4ae`) — not Charlotte NC |
| TPAAP | Tampa Airport Marriott | **VERIFIED_ABSENT** | — | Excluded: Sao Paulo Airport Marriott; Westin Tampa Waterside. True TPAAP address/URL = 0 |

**None** are EXISTING_CANONICAL_MATCH, AMBIGUOUS_MATCH, or LOOKUP_BLOCKED.

Earlier `NOT_FOUND_IN_HPC` labels were unresolved lookups until V4 completed with access OK + false-positive filters.

### Lookup scope (each hotel)

1. HPC Official Property URL / Source URL / Website (Marriott code)  
2. HPC Property Name / Canonical Property Name  
3. HPC Address  
4. HPC City + Marriott/Renaissance brand  
5. HPC Country US/blank + name  
6. Local `config/group-demand-intelligence/hotels/*`  
7. Local `data/group-demand-intelligence/hotels/*`  
8. ADP/fixture path scan for Marriott codes  

## Prepared creation payloads (not applied)

Canonical creation path (same pattern as W Rome / YOTEL Geneva Lake stewardship — **gated**, not free invent-and-insert):

```bash
# Dry-run evidence pack first (hotel-specific stewardship script when authored), then:
# node scripts/<hotel>-hpc-stewardship-apply.mjs --apply \
#   --confirm-<hotel>-census-steward-insert \
#   --confirm-hotel-property-census-only \
#   --confirm-no-legacy-census-writes \
#   --confirm-no-owner-operator \
#   --enable-production-writes
```

Alternate steward route: census autopilot / coverage-insert allowlist via `createHotelPropertyCensusRecords` after founder-approved evidence pack. **Do not** invent `rec…` IDs or bypass confirms.

### Shared required / allowlisted field skeleton

Use coverage/stewardship allowlist fields only (no owner/operator, no invented rooms until High-confidence official count):

| Field | WASFP | WASPY | CLTCC | TPAAP |
|-------|-------|-------|-------|-------|
| Property Name | Falls Church Marriott Fairview Park | Renaissance Arlington Capital View Hotel | Charlotte Marriott City Center | Tampa Airport Marriott |
| Canonical Property Name | (same) | (same) | (same) | (same) |
| Property Identity Key | `ind_marriott_us_wasfp` | `ind_marriott_us_waspy` | `ind_marriott_us_cltcc` | `ind_marriott_us_tpaap` |
| Current Brand | Marriott Hotels | Renaissance Hotels | Marriott Hotels | Marriott Hotels |
| Brand Family | Marriott International | Marriott International | Marriott International | Marriott International |
| Affiliation Status | Affiliated (verify option) | Affiliated | Affiliated | Affiliated |
| City | Falls Church | Arlington | Charlotte | Tampa |
| State / Region | Virginia | Virginia | North Carolina | Florida |
| Country | United States | United States | United States | United States |
| Address | 3111 Fairview Park Drive, Falls Church, Virginia, USA, 22042 | 2800 South Potomac Ave, Arlington, Virginia, USA, 22202 | 100 West Trade Street, Charlotte, North Carolina, USA, 28202 | Tampa International Airport, 4200 George J. Bean Parkway, Tampa, Florida, USA, 33607 |
| Official Property URL | https://www.marriott.com/en-us/hotels/wasfp-falls-church-marriott-fairview-park/overview/ | https://www.marriott.com/en-us/hotels/waspy-renaissance-arlington-capital-view-hotel/overview/ | https://www.marriott.com/en-us/hotels/cltcc-charlotte-marriott-city-center/overview/ | https://www.marriott.com/en-us/hotels/tpaap-tampa-airport-marriott/overview/ |
| Source URL | (same official) | (same) | (same) | (same) |
| Family / Source Family | Marriott | Marriott | Marriott | Marriott |
| Source Type / Confidence | official_property_page / High | … | … | … |
| Identity Confidence | High (code+URL+address) | High | High | High |
| Production Use Status | Census-eligible draft per steward policy | … | … | … |
| Rooms / Keys | **omit until High official count** | omit | omit | Map PDF lead 298 — **verify before write** |
| Meeting sq ft | Marketing leads only — see `OFFICIAL_PROPERTY_FACT_LEADS.json` — **do not** copy unverified into capability | … | conflicting figures | … |

### Post-create GDI configuration (after real `rec…` exists)

1. Write `config/group-demand-intelligence/hotels/<recId>.json` mirroring Renaissance (`hotelId` = HPC id, `aliases.censusRecordId`, Marriott code, demandTerritory, capability from **verified** sources only).  
2. Link ADP / HI only if profiles exist — do not invent.  
3. Only then is coordinator `--hotel <recId>` eligible.

### Identity cautions

- **WASPY:** Connected Residence Inn Arlington Capital View is a **different** hotel — never merge.  
- **TPAAP:** Not Sao Paulo Airport Marriott; not Westin Tampa Waterside; terminal connection ≠ every departure gate.  
- **CLTCC:** Not Charlotte Amalie / USVI properties.

## Already resolved (no action)

| Hotel | Canonical ID | Status |
|-------|--------------|--------|
| Renaissance New York Times Square | `recG66DQJKP2c0UNh` | EXISTING — config ready |
| Waterstone Resort & Marina Boca Raton | `recgMYovrrZDJMqzX` | EXISTING — config ready; reuse-first |
