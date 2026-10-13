# GDI Discovery Recall V4 — Funnel

Marker: `gdi_discovery_recall_v4_20260922`  
Baseline SHA: `1a3ebc79d0fe299c4b13d5df704247556e91c3a2`  
As-of: `2026-09-22`

## Wave 3 baseline collapse (pre-V4)

| Hotel | Fetched pages | Extract batches | Empty extracts | Candidates |
|---|---:|---:|---:|---:|
| Radisson Hotel Santo Domingo | 39 | 6 | 3 | 2 |
| Casas del XVI | 37 | 6 | **6** | **0** |
| Faranda Collection Bogotá | 40 | 6 | **6** | **0** |

### Casas / Faranda loss classification (Wave 3)

Exact question: why did ~37–40 fetched pages become zero candidates?

| Loss class | Casas | Faranda | Notes |
|---|---:|---:|---|
| QUERY_SOURCE_MISS | 0 | 0 | Pages were fetched; queries returned URLs |
| FETCH_FAILURE | ~low | ~low | Fetch succeeded for the counted pages |
| JS_RENDER_REQUIRED | 0* | 0* | Not diagnosed as primary; static text present |
| TEXT_EXTRACTION_FAILURE | 0 | 0 | Searchable text reached extract |
| **EVENT_EXTRACTION_FAILURE** | **6/6 batches** | **6/6 batches** | OpenAI returned `{"candidates":[]}` (completion_tokens≈8) |
| DATE_EXTRACTION_FAILURE | n/a | n/a | Never reached entity creation |
| LOCATION_EXTRACTION_FAILURE | n/a | n/a | Never reached entity creation |
| CANDIDATE_CREATION_TOO_STRICT | contributing | contributing | Extract system biased to TBD/RFP / open-sourcing |
| PRE_HYGIENE_FILTER | 0 | 0 | Nothing to filter |
| DUPLICATION_COLLAPSE | 0 | 0 | Nothing to dedupe |
| NO_REAL_EVENT_CONTENT | possible secondary | possible secondary | But extract refused even event-bearing pages |
| OTHER | SERP locale | SERP locale | `hl=en,gl=us` + flat query budget truncating incentive/housing families |

**PRIMARY BOTTLENECK (Wave 3): EVENT_EXTRACTION_FAILURE** (with CANDIDATE_CREATION_TOO_STRICT + English SERP / truncated locale query families as amplifiers).

## V4 funnel

| Hotel | Queries | URLs | Fetched | Empty extracts | Raw | After dedupe | Qualified (pre-V3) |
|---|---:|---:|---:|---:|---:|---:|---:|
| Radisson Hotel Santo Domingo | 36 | 319 | 70 | 1/8 | 17 | 12 | 12 |
| Casas del XVI (Vignette Collection) | 36 | 305 | 63 | 0/8 | 25 | 14 | 14 |
| Faranda Collection Bogotá | 36 | 324 | 67 | 1/8 | 17 | 13 | 13 |

### Locale / archetype

| Hotel | SERP locale | Demand archetype |
|---|---|---|
| Radisson | `hl=es, gl=do` | URBAN_BUSINESS_MEETINGS |
| Casas del XVI | `hl=es, gl=do` | LUXURY_DESTINATION_SMALL |
| Faranda | `hl=es, gl=co` | CORPORATE_SECONDARY |

### Extract recovery vs Wave 3

| Hotel | Wave 3 empty extract batches | V4 empty extract batches | Candidate delta |
|---|---:|---:|---|
| Radisson | 3/6 | 1/8 | 2 → **12** |
| Casas | **6/6** | **0/8** | 0 → **14** |
| Faranda | **6/6** | 1/8 | 0 → **13** |

### Hygiene V3 (unchanged gates) on V4 candidates

| Hotel | Candidates | TRUE_ACTIONABLE | VALID_WATCH | INSUFFICIENT | INVALID |
|---|---:|---:|---:|---:|---:|
| Radisson | 12 | 0 | 5 | 1 | 6 |
| Casas | 14 | 0 | 2 | 0 | 12 |
| Faranda | 13 | 0 | 3 | 1 | 9 |

Dominant V3 reason class across hotels: **TOO_EARLY** / future-cycle monitoring (as-of 2026-09-22 many mid-year 2026 dates are already PAST; remaining are unconfirmed cycles). Contact-not-relevant appears on Faranda (2).

**Interpretation:** Discovery recall recovered. Commercial qualification correctly refused to invent TRUE_ACTIONABLE sales noise.
