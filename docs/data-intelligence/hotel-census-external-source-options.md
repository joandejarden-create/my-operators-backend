# Hotel Census External Source Options

**Purpose:** Decision support for founder approval before any secondary/licensed integration.  
**Status:** Evaluation only — **no integration / no Census writes** from these sources until policy approval.  
**Census write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`) only.

## Policy baseline (current)

| Rule | Setting |
| --- | --- |
| Official sources | Preferred for all Level 2 fields |
| `ENABLE_SECONDARY_HOTEL_DATA_SOURCES` | Default `0` — opportunities reported, no secondary writes |
| Webhound | Pattern discovery only — never Census SoT |
| Mapbox | Coordinates only after High Address + Address Source URL |
| Google | Not used as primary stored address/phone source without legal review |
| OTAs | Rejected as Census SoT |

## Options matrix

| Source | Address | Phone | Hotel URL | Rooms | Coords | Coverage (CALA) | Storage / license | Cost | API | Recommendation |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Official parent/brand pages + directories | Yes | Often | Yes | Sometimes | Rare | High for chained brands | Public pages; respect ToS/robots | Free (ops) | Scrape/official APIs | **Primary — keep investing** |
| Official hotel microsites | Yes | Yes | Yes | Often | Sometimes | Medium | Public; property-specific | Free (ops) | Fetch | **Primary fallback** |
| Official factsheets / press | Yes | Yes | — | Yes (exact) | Rare | Sparse | Public PDFs/news | Free (ops) | Fetch/PDF | **Primary for rooms gaps** |
| Mapbox Permanent Geocoding | No (forbidden as address SoT) | No | No | No | Yes | High when High Address exists | Storage allowed with Permanent + terms flag | Paid | Yes | **Approved for coords only** |
| Google Places API | Yes | Yes | Sometimes | No | Yes | Very high | **Storage restricted** without Places attribution + ToS review | Paid | Yes | **Hold — legal/storage review required** |
| CoStar / STR licensed hotel DB | Yes | Sometimes | Sometimes | Yes | Yes | High commercial | **Licensed — product-facing CoStar exposure forbidden** per Dealality rules | High | Vendor | **Internal ops only if licensed; never product-facing CoStar** |
| Data Appeal / similar hospitality datasets | Yes | Yes | Sometimes | Sometimes | Yes | Medium–High | License-dependent | Mid–High | Often | **Evaluate under secondary policy if licensed for storage** |
| Geoapify / Foursquare / HERE / TomTom | Yes | Sometimes | Sometimes | No | Yes | Medium–High | License-dependent; often no bulk store | Paid | Yes | **Coords/POI secondary candidate only after license** |
| Tourism board / destination open data | Sometimes | Rare | Rare | Sometimes | Rare | Country-specific | Often open | Free–low | Varies | **Secondary for sparse markets when property-exact** |
| Owner/developer websites | Yes | Yes | Yes | Often | Rare | Low volume | Public | Free (ops) | Fetch | **Secondary rooms/address when exact property match** |
| Hospitality trade press | — | — | — | Sometimes | — | Sparse | Public | Free | Fetch | **Secondary rooms (exact count only)** |

## Fit by field

### Address / Phone
1. Official property page / brand directory / JSON-LD  
2. Official hotel website linked from parent  
3. Licensed hospitality dataset (if storage allowed)  
4. Google Places — **blocked pending legal/storage review**

### Hotel URL
1. Official parent sitemap/directory  
2. Brand property URL patterns keyed by property code (Choice MX###, MARSHA, etc.)

### Rooms
1. Official factsheet / property page exact count  
2. Official press release (exact property)  
3. Official hotel website  
4. Licensed dataset / trade press — secondary only

### Coordinates
1. Official coords when present  
2. **Mapbox Permanent** after High Address  
3. Other geospatial APIs only if storage licensed

## Recommended decision sequence

1. Keep Autopilot on **official-only** (`ENABLE_SECONDARY_HOTEL_DATA_SOURCES=0`).  
2. Expand official adapters (Choice property pages, Marriott DAM index for in-Census MARSHAs, Hilton/IHG/Accor directories).  
3. Founder decides whether to allow **one** licensed secondary for address/phone gaps (not Google until reviewed; not CoStar product-facing).  
4. If approved, implement secondary writers with mandatory Source URL / Type / Evidence Tier / Confidence / Reviewed Date.

## Explicit non-goals until approval

- No Google Places writes to Census  
- No CoStar fields in product-facing Census enrichment  
- No OTA (Booking/Expedia/Tripadvisor) as SoT  
- No silent secondary writes
