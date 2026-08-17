# GHL Hoteles Narrow Extraction

Generated: 2026-07-06T14:24:51.745Z
Mode: **apply**
Operator: GHL Hoteles (GHL Holding) — `reciI2tYQBfMoMK9G`

## Summary

- Sources in scope: `recvjfaDa9AnCJkNx`, `reckrUB2WmnSm02g3`, `recy337fP8zhpvePy`, `recqLGiIQAEP1I1Hv`, `recoOcRjSD3VZb3qt`
- Sources excluded: `recFqJpw4wJbMmVSF` (Spanish home — not in allowlist)
- Target fact keys requested: 9
- Registry-supported keys: 6
- Registry-unsupported keys: 3
- HTML text clean (all sources): **yes**
- Existing facts (allowlisted sources): 0
- Clean facts that would be created on apply: **7**
- Skipped candidates: 92
- Duplicate warnings: 0

## Extraction Quality Assessment

- Overall: **good_for_steward_review**
- Readable sources: 5
- Substantive facts: 6
- Has company name: yes
- Has regions: yes
- Has brand families: yes
- Has offered services: yes
- Apply recommended: **yes — pending founder review**
- Governance publish still blocked: **yes** — Facts must be created, steward-approved, and governance publish dry-run must pass before publish.

Apply completed — facts remain Pending; steward review required.

## Target Fact Keys

| Priority | Field key | Registry supported | Display label |
|----------|-----------|-------------------|---------------|
| P0 | `op.snapshot.companyName` | yes | Company Name |
| P0 | `op.snapshot.companyDescription` | yes | Company Description |
| P0 | `op.snapshot.primaryServiceModel` | yes | Primary Service Model |
| P0 | `op.markets.regionsSupported` | yes | Regions Supported |
| P1 | `op.brand.familiesOperated` | yes | Brand Families Operated |
| P0 | `op.platform.offeredServices` | yes | Offered Services |
| P2 | `op.capabilities.managementServices` | **no** | — |
| P2 | `op.portfolio.scale` | **no** | — |
| P2 | `op.events.miceCapability` | **no** | — |

## Unsupported Registry Keys

_These keys were requested but are not in the Operator Explorer registry — no writes._

- `op.capabilities.managementServices`
- `op.portfolio.scale`
- `op.events.miceCapability`

## Source: GHL Hoteles home EN (`recvjfaDa9AnCJkNx`)

- Validation: pass
- Extraction quality: html_clean (5353 chars)
- HTML text clean: yes
- Document kind: html (5353 chars)
- Raw candidates (target keys): 6
- Clean candidates (this source): 4
- Skipped (this source): 6

### Clean candidates

#### `op.snapshot.companyName` (P0)

- Value: GHL Hoteles
- Evidence: Welcome to GHL Hotels, Latin America Official Website
- Extraction: Directly Stated · Confidence: High
- Enriched by GHL narrow script

#### `op.snapshot.companyDescription` (P0)

- Value: With presence in 35 hotels, 4 countries, 18 destinations, 3.433 rooms and 2.000 collaborators, GHL operates hotels across Latin America.
- Evidence: With presence in 35 hotels, 4 countries, 18 destinations, 3.433 rooms and 2.000 collaborators
- Extraction: Directly Stated · Confidence: High
- Enriched by GHL narrow script

#### `op.markets.regionsSupported` (P0)

- Value: Latin America, Colombia, Peru, Chile, Guatemala
- Evidence: With presence in 35 hotels, 4 countries, 18 destinations, 3.433 rooms and 2.000 collaborators
- Extraction: Directly Stated · Confidence: High
- Enriched by GHL narrow script

#### `op.brand.familiesOperated` (P1)

- Value: Geotel, GHL Collection, GHL Relax, GHL Style, GHL
- Evidence: GHL STYLE
- Extraction: Directly Stated · Confidence: High
- Enriched by GHL narrow script

### Skipped candidates

- `op.snapshot.companyName`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.snapshot.companyDescription`: weak_company_description — "Book always on the official website of GHL Hotels. The best price online."
- `op.snapshot.primaryServiceModel`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.platform.offeredServices`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.brand.familiesOperated`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.markets.regionsSupported`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."

## Source: GHL Hoteles destinations (`reckrUB2WmnSm02g3`)

- Validation: pass
- Extraction quality: html_clean (1810 chars)
- HTML text clean: yes
- Document kind: html (1810 chars)
- Raw candidates (target keys): 6
- Clean candidates (this source): 3
- Skipped (this source): 7

### Clean candidates

#### `op.snapshot.companyName` (P0)

- Value: GHL Hoteles
- Evidence: GHL Hotels’s Destinations in Latin America
- Extraction: Directly Stated · Confidence: High
- Enriched by GHL narrow script

#### `op.markets.regionsSupported` (P0)

- Value: Latin America, Colombia, Peru, Chile, Guatemala
- Evidence: Destinations in Latin America
- Extraction: Directly Stated · Confidence: High
- Enriched by GHL narrow script

#### `op.brand.familiesOperated` (P1)

- Value: GHL
- Evidence: GHL
- Extraction: Directly Stated · Confidence: High
- Enriched by GHL narrow script

### Skipped candidates

- `op.snapshot.companyName`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.snapshot.companyDescription`: weak_company_description — "Discover all the destinations of GHL Hotels. Choose your destination and make your reservations."
- `op.snapshot.primaryServiceModel`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.platform.offeredServices`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.brand.familiesOperated`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.markets.regionsSupported`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.snapshot.companyDescription`: weak_company_description — "Discover all the destinations of GHL Hotels. Choose your destination and make your reservations."

## Source: GHL Hoteles hotels portfolio (`recy337fP8zhpvePy`)

- Validation: pass
- Extraction quality: html_clean (6548 chars)
- HTML text clean: yes
- Document kind: html (6548 chars)
- Raw candidates (target keys): 6
- Clean candidates (this source): 4
- Skipped (this source): 7

### Clean candidates

#### `op.snapshot.companyName` (P0)

- Value: GHL Hoteles
- Evidence: Hotels GHL Hotels, Latin America
- Extraction: Directly Stated · Confidence: High
- Enriched by GHL narrow script

#### `op.markets.regionsSupported` (P0)

- Value: Latin America, Colombia, Peru, Chile, Guatemala
- Evidence: Destinations in Latin America
- Extraction: Directly Stated · Confidence: High
- Enriched by GHL narrow script

#### `op.snapshot.primaryServiceModel` (P0)

- Value: Hotel operations
- Evidence: hotels operated by GHL Hotels propose stays in prime city locations accompanied by excellent services and personalized attention.
- Extraction: Directly Stated · Confidence: Medium
- Enriched by GHL narrow script

#### `op.brand.familiesOperated` (P1)

- Value: Geotel, GHL Collection, GHL Relax, GHL Style, Irotama Resort, Latam Hotel Corporation, GHL
- Evidence: Latam Hotel Corporation
- Extraction: Directly Stated · Confidence: High
- Enriched by GHL narrow script

### Skipped candidates

- `op.snapshot.companyName`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.snapshot.companyDescription`: weak_company_description — "Discover all the hotels and destinations of GHL Hotels. The best information is available on this official website."
- `op.snapshot.primaryServiceModel`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.platform.offeredServices`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.brand.familiesOperated`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.markets.regionsSupported`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.snapshot.companyDescription`: weak_company_description — "Discover all the hotels and destinations of GHL Hotels. The best information is available on this official website."

## Source: GHL Hoteles brand GHL (`recqLGiIQAEP1I1Hv`)

- Validation: pass
- Extraction quality: html_clean (4231 chars)
- HTML text clean: yes
- Document kind: html (4231 chars)
- Raw candidates (target keys): 6
- Clean candidates (this source): 4
- Skipped (this source): 7

### Clean candidates

#### `op.snapshot.companyName` (P0)

- Value: GHL Hoteles
- Evidence: Get to know the hotels GHL, Latin America
- Extraction: Directly Stated · Confidence: High
- Enriched by GHL narrow script

#### `op.markets.regionsSupported` (P0)

- Value: Latin America, Colombia, Peru, Chile, Guatemala
- Evidence: Destinations in Latin America
- Extraction: Directly Stated · Confidence: High
- Enriched by GHL narrow script

#### `op.platform.offeredServices` (P0)

- Value: Full-service hotel guest services (rooms, meetings, and on-property services)
- Evidence: GHL hotels offer complete and comprehensive services to their guests.
- Extraction: Directly Stated · Confidence: Medium
- Enriched by GHL narrow script

#### `op.brand.familiesOperated` (P1)

- Value: Geotel, GHL Collection, GHL Relax, GHL Style, Irotama Resort, Latam Hotel Corporation, GHL
- Evidence: GHL hotels offer
- Extraction: Directly Stated · Confidence: High
- Enriched by GHL narrow script

### Skipped candidates

- `op.snapshot.companyName`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.snapshot.companyDescription`: weak_company_description — "Explore and discover all the possibilities offered by Hotels GHL in GHL Hoteles. Official website."
- `op.snapshot.primaryServiceModel`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.platform.offeredServices`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.brand.familiesOperated`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.markets.regionsSupported`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.snapshot.companyDescription`: weak_company_description — "Explore and discover all the possibilities offered by Hotels GHL in GHL Hoteles. Official website."

## Source: GHL Hoteles events (`recoOcRjSD3VZb3qt`)

- Validation: pass
- Extraction quality: html_clean (3016 chars)
- HTML text clean: yes
- Document kind: html (3016 chars)
- Raw candidates (target keys): 6
- Clean candidates (this source): 4
- Skipped (this source): 7

### Clean candidates

#### `op.snapshot.companyName` (P0)

- Value: GHL Hoteles
- Evidence: Events GHL Hotels in Latin America
- Extraction: Directly Stated · Confidence: High
- Enriched by GHL narrow script

#### `op.markets.regionsSupported` (P0)

- Value: Latin America, Colombia, Peru, Chile, Guatemala
- Evidence: Destinations in Latin America
- Extraction: Directly Stated · Confidence: High
- Enriched by GHL narrow script

#### `op.platform.offeredServices` (P0)

- Value: Events and celebrations (meetings and group events at GHL hotels)
- Evidence: guarantee success in all your celebrations and events thanks to their well-equipped facilities, superior category services, and expert advice from our team of qualified professionals.
- Extraction: Directly Stated · Confidence: Medium
- Enriched by GHL narrow script

#### `op.brand.familiesOperated` (P1)

- Value: Geotel, GHL Collection, GHL Relax, GHL Style, GHL
- Evidence: GHL Style
- Extraction: Directly Stated · Confidence: High
- Enriched by GHL narrow script

### Skipped candidates

- `op.snapshot.companyName`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.snapshot.companyDescription`: weak_company_description — "Organise all your events with GHL Hotels and achieve success. Check out all the available services."
- `op.snapshot.primaryServiceModel`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.platform.offeredServices`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.brand.familiesOperated`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.markets.regionsSupported`: gap_copy, data_gap, weak_evidence — "Not confirmed in available sources."
- `op.snapshot.companyDescription`: weak_company_description — "Organise all your events with GHL Hotels and achieve success. Check out all the available services."

## Proposed Facts (global, after dedupe)

| Source | Field key | Value preview |
|--------|-----------|---------------|
| GHL Hoteles events | `op.snapshot.companyName` | GHL Hoteles |
| GHL Hoteles home EN | `op.snapshot.companyDescription` | With presence in 35 hotels, 4 countries, 18 destinations, 3.433 rooms and 2.000 collaborators, GHL o |
| GHL Hoteles events | `op.markets.regionsSupported` | Latin America, Colombia, Peru, Chile, Guatemala |
| GHL Hoteles brand GHL | `op.brand.familiesOperated` | Geotel, GHL Collection, GHL Relax, GHL Style, Irotama Resort, Latam Hotel Corporation, GHL |
| GHL Hoteles hotels portfolio | `op.snapshot.primaryServiceModel` | Hotel operations |
| GHL Hoteles brand GHL | `op.platform.offeredServices` | Full-service hotel guest services (rooms, meetings, and on-property services) |
| GHL Hoteles events | `op.platform.offeredServices` | Events and celebrations (meetings and group events at GHL hotels) |

## Would Write On Apply

- Pending fact rows: 7
- Sources patched (Status → Extracted): 4

**Does not write:**
- Approved for Explorer Use
- Approved for Extraction (not auto-set)
- Human Review Status = Approved (facts remain Pending)
- Operator Setup profile governance fields
- Company Validated / Company Validation Date
- External Display Status / Show Trust Label
- Published Explorer Fields
- Gap facts / Not confirmed placeholders
- Spanish home source recFqJpw4wJbMmVSF
- All operator PI sources (allowlist only)

## Apply Result

- Run ID: `pi-ghl-extract-97c9395f`
- Sources patched: 4
- Facts created: 7

## Post-Apply Recommendation

```bash
npm run steward-partner-intelligence -- --entity-type operator --target-rec-id reciI2tYQBfMoMK9G --dry-run --recompute
```

_Manually review extracted facts before any approval._
