# Independent Hotel Census — Current Census Inventory (Read-Only)

**Purpose:** Inventory the existing Dealality `Hotel Census` before building a parallel independent hotel census from open/public sources. This document is read-only reference only.

**Constraints (pipeline rules):**

1. Do not overwrite, delete, rename, or restructure the current `Hotel Census` table.
2. Treat the current census as read-only for comparison and duplicate detection only.
3. All new open/public source data must go into new staging tables.
4. Do not connect the new census to production UI, scoring, matching, or user-facing pages yet.
5. Do not import or persist CoStar/STR-derived fields into the new census.
6. STR/CoStar files may only be used as restricted internal reference material and must not write to production or staging census fields unless explicitly approved.

**Status:** Documentation only. No schema or application changes implied by this file.

**Sources:** `reports/hotel-census-str-inventory-summary.json`, `reports/hotel-census-str-field-inventory.csv`, `lib/hotel-census/`, STR import tooling, and API usage in this repository (as of 2026-05-20).

---

## 1. Current table name

| Item | Value |
|------|--------|
| **Airtable table** | `Hotel Census` (override: `AIRTABLE_HOTEL_CENSUS_TABLE`) |
| **Base** | Deal Capture Platform — `AIRTABLE_BASE_ID_ALT` |
| **Table ID** | `tblgj2qEwxjTcg6q0` (from last inventory run) |
| **Code constant** | `lib/hotel-census/fields.js` → `HOTEL_CENSUS_TABLE` |

**Related table (not the census itself):** `Brand Alias Mapping` — maps canonical brand names to exact `Hotel Census`.`Affiliation` strings for read-only Brand Explorer rollups.

---

## 2. Current fields (103 columns)

Full list from `reports/hotel-census-str-field-inventory.csv` (metadata scan 2026-05-20):

| # | Field | Airtable type |
|---|--------|----------------|
| 1 | name | multilineText |
| 2 | Affiliation | multilineText |
| 3 | Parent Company | multilineText |
| 4 | status | multilineText |
| 5 | Latitude | number |
| 6 | Longitude | number |
| 7 | city | multilineText |
| 8 | country | multilineText |
| 9 | rooms | number |
| 10 | Location | multilineText |
| 11 | Property ID | number |
| 12 | STR Number | number |
| 13 | Star Rating | rating |
| 14 | Property Type | multilineText |
| 15 | Dealality Market | multilineText |
| 16 | Submarket | singleLineText |
| 17 | Region | multilineText |
| 18 | Development Cost | currency |
| 19 | Occupancy Rate | percent |
| 20 | ADR | currency |
| 21 | RevPAR | currency |
| 22 | Amenities | multilineText |
| 23 | Website | url |
| 24 | Chain Scale | multilineText |
| 25 | project_phase | multilineText |
| 26 | projected_open_date | date |
| 27 | Operation Type | singleSelect |
| 28 | Management Company | singleLineText |
| 29–30 | Address 1, Address 2 | singleLineText |
| 31–37 | All Suites, Boutique, Casino, Conference, Convention, Golf, Indoor Corridors (Y/N) | singleSelect |
| 38 | Chain ID | singleLineText |
| 39 | Class | singleLineText |
| 40 | Continent | singleLineText |
| 41 | County | singleLineText |
| 42–45 | Double High/Low Rate, Single High/Low Rate | number |
| 46–48 | Ex-Affiliation, Ex-Affiliation 2, Ex-Affiliation 3 | singleLineText |
| 49 | Fax | singleLineText |
| 50 | Floors | number |
| 51–52 | Resort, Restaurant, Ski, Spa (Y/N) | singleSelect |
| 53 | Largest Meeting Space | number |
| 54 | MSA | singleLineText |
| 55–60 | Mailing Address 1/2, City, Country, Postal Code, State | singleLineText |
| 61–69 | Management Address 1/2, City, Country, Contact Name/Title, Email, Phone, Postal Code, State, Website | mixed |
| 70 | Market | singleLineText |
| 71 | Open Date | date |
| 72 | Original Affiliation | singleLineText |
| 73–82 | Owner Address 1/2, City, Company, Contact Name/Title, Country, Email, Phone, Postal Code, State, Website | mixed |
| 83 | Postal Code | singleLineText |
| 84 | Price | singleLineText |
| 85 | State | singleLineText |
| 86 | Sub-Continent | singleLineText |
| 87–88 | Suite High/Low Rate | number |
| 89 | Telephone | singleLineText |
| 90 | Total Meeting Space | number |
| 91 | Tract | singleLineText |
| 92 | Year Affiliated | number |
| 93–94 | Year & Month Affiliated, Affiliated Month | formula |
| 95 | Last Modified | lastModifiedTime |
| 96 | Include in Brand Explorer | checkbox |
| 97 | Data Confidence | singleSelect |

**Application-facing subset** (`lib/hotel-census/fields.js` + Phase 1 docs):

`name`, `Affiliation`, `Parent Company`, `status`, `rooms`, `country`, `city`, `Market`, `Region`, `Chain Scale`, `Location`, `project_phase`, `Operation Type`, `Management Company`, optional `Include in Brand Explorer`, `Data Confidence`.

See also: `docs/brand-explorer-census-phase1-plan.md`.

---

## 3. Record count (available)

From `reports/hotel-census-str-inventory-summary.json` (full read-only load, 2026-05-20):

| Metric | Count |
|--------|------:|
| **Total records** | **15,635** |
| With STR Number | 15,284 |
| Missing STR Number | 351 |
| Duplicate STR ID groups | 121 (272 logged conflict rows) |
| Missing city / country / name | 0 each |
| With `Market` (STR market) | 14,856 |
| With `Submarket` | 0 |
| Distinct countries | 51 |
| Distinct STR markets | 74 |

**Refresh (read-only, no Airtable writes):**

```bash
node scripts/inventory-hotel-census-for-str-import.mjs
```

Outputs:

- `reports/hotel-census-str-field-inventory.csv`
- `reports/hotel-census-str-data-quality.csv`
- `reports/hotel-census-str-duplicates.csv`
- `reports/hotel-census-str-inventory-summary.json`

---

## 4. Likely unique identifiers

| Identifier | Role | Notes |
|------------|------|--------|
| **Airtable `recordId`** (`rec…`) | System primary key | Used in STR apply logs and APIs |
| **`STR Number`** | Primary external business key | 15,284 populated; chosen over `Property ID` for STR matching |
| **`Property ID`** | Alternate STR-style ID | Same semantic family as STR Number; do not reuse in open census |
| **`name` + `city` + `country`** | Fuzzy duplicate key | `lib/str-census-import/normalize.mjs` (`nameCityCountryKey`); used in duplicate reports |
| **`Chain ID`** | Chain-level code | Not validated as unique per property in inventory |
| **`Latitude` + `Longitude`** | Geo hint | Not used as unique key today; useful for open-source dedupe at ingest |
| **`Affiliation` + `Parent Company` + `status`** | Rollup keys | Brand Explorer matching, not row uniqueness |

**Recommended read-only duplicate checks** against current census when ingesting open candidates:

1. Normalized name + city + country (existing pattern).
2. Haversine or grid snap within ~100–250 m when lat/lng present.
3. Optional: website host / phone (low confidence only).
4. **Do not** match on STR Number or Property ID from open sources.

---

## 5. Fields that appear externally sourced or restricted

### A. STR / CoStar–derived (do not copy into independent staging or verified census)

Evidence: STR Excel import pipeline (`lib/str-census-import/`, `data/str-imports/`), field names, and `Development Cost` used as import batch markers.

| Field group | Fields |
|-------------|--------|
| STR IDs | `STR Number`, `Property ID`, `Chain ID` |
| STR geography | `Market`, `Submarket`, `MSA`, `Tract`, `Region`, `Dealality Market` |
| STR performance | `ADR`, `RevPAR`, `Occupancy Rate`, `Single/Double/Suite High/Low Rate`, `Star Rating` |
| STR import ops | `Development Cost` (values 1/2/3 used as batch markers in apply scripts) |

### B. Licensed / sensitive commercial contact and owner data

Treat as restricted; not for open republication. Likely STR or proprietary directory exports:

- Owner block: addresses, company, contacts, email, phone, website
- Management block: same pattern
- Mailing address block
- `Fax`, `Telephone`, `Price`

### C. Dealality-curated / product governance (current census only)

- `Affiliation`, `Parent Company`, `status`, `project_phase`, `Include in Brand Explorer`, `Data Confidence`
- Brand alias resolution via `Brand Alias Mapping` — comparison only for independent pipeline

### D. Mixed / usable only with explicit open license per source

May appear in OSM, Wikidata, or government data with different semantics. Store only in **Candidates** with source license; never auto-merge into current census:

- `name`, `city`, `country`, `State`, `County`, `Continent`, `Sub-Continent`, `Postal Code`, `Address 1/2`
- `Latitude`, `Longitude`, `Website`, `rooms`, `Floors`
- `Open Date`, amenity Y/N flags, `Property Type`, `Chain Scale`, `Operation Type`, `Management Company`

### E. STR/CoStar files in repo (reference only)

- `data/str-imports/*.xls`
- Scripts: `import-str-census-dry-run.mjs`, `apply-str-census-import.mjs`, `apply-str-import-remaining.mjs`
- Must not feed the independent pipeline unless explicitly approved.

---

## 6. Recommended new staging schema (Airtable)

Three **new** tables. **Do not** alter `Hotel Census`.

### Table A: `Independent Hotel Source Candidates`

One row per source record per import batch. All open/public ingest lands here only.

| Field | Type | Notes |
|-------|------|--------|
| Source Name | singleLineText | e.g. OpenStreetMap, Wikidata, brand directory |
| Source Type | singleSelect | `osm`, `wikidata`, `brand_directory`, `government_registry`, `manual_upload`, … |
| Source License | singleLineText | SPDX or URL (ODbL, CC0, public domain, etc.) |
| Source URL | url | Record or dataset URL |
| Source Record ID | singleLineText | OSM node/way id, Wikidata Q-id, registry id |
| Raw Hotel Name | singleLineText | |
| Raw Address | singleLineText | |
| Raw City | singleLineText | |
| Raw Country | singleLineText | |
| Raw Latitude | number | |
| Raw Longitude | number | |
| Raw Website | url | |
| Raw Phone | singleLineText | |
| Raw Brand | singleLineText | |
| Raw Payload JSON | longText | Full source JSON |
| Import Batch ID | singleLineText | UUID or `osm-2026-05-20` |
| Imported At | dateTime | |
| Review Status | singleSelect | `pending`, `in_review`, `approved`, `rejected`, `duplicate` |
| Possible Match in Current Census | link → **Hotel Census** | Read-only link for ops |
| Possible Match Confidence | singleSelect | `none`, `low`, `medium`, `high` |
| Recommended Action | singleSelect | `promote`, `merge_with_census`, `skip`, `needs_research` |
| Candidate Dedupe Key | formula or singleLineText | normalized `name\|city\|country` or geohash |
| Linked Evidence | link → Evidence | optional |

**Rules:** No auto-promotion. No writes to `Hotel Census`. STR/CoStar fields must not appear on this table.

### Table B: `Independent Hotel Source Evidence`

Audit trail for human review.

| Field | Type | Notes |
|-------|------|--------|
| Candidate | link → Candidates | |
| Evidence Type | singleSelect | `source_snapshot`, `geocode`, `manual_note`, `census_comparison`, `license_check` |
| Evidence URL | url | |
| Evidence Text | longText | |
| Captured At | dateTime | |
| Captured By | singleLineText | email or system |
| Compares To Census Record | link → Hotel Census | read-only comparison |
| Match Score | number | 0–100 |
| Match Reason | longText | |

### Table C: `Verified Independent Hotel Census`

Human-approved golden records only.

| Field | Type | Notes |
|-------|------|--------|
| Verified Hotel Name | singleLineText | |
| Verified Address | singleLineText | |
| Verified City / State / Country / Postal | singleLineText | |
| Verified Latitude / Longitude | number | |
| Verified Website / Phone | url / singleLineText | |
| Verified Brand Label | singleLineText | open-source brand string, not `Affiliation` |
| Primary Source Candidate | link → Candidates | |
| Approved At | dateTime | |
| Approved By | singleLineText | |
| Approval Notes | longText | |
| Census Reconciliation Status | singleSelect | `not_in_census`, `likely_duplicate`, `matched_to_census` |
| Linked Census Record | link → Hotel Census | optional read-only reference |
| Verified Dedupe Key | singleLineText | unique in ops process |
| Active | checkbox | soft delete |

**Promotion flow:** Candidate `Review Status = approved` → manual script or controlled automation **creates** a Verified row. Never updates `Hotel Census` without a separate, explicit migration project.

---

## 7. Proposed files, scripts, and routes (not built yet)

### Environment (`.env.example` — future)

```bash
# Independent hotel census (staging only — no production UI)
# AIRTABLE_INDEPENDENT_CENSUS_CANDIDATES_TABLE=Independent Hotel Source Candidates
# AIRTABLE_INDEPENDENT_CENSUS_EVIDENCE_TABLE=Independent Hotel Source Evidence
# AIRTABLE_INDEPENDENT_CENSUS_VERIFIED_TABLE=Verified Independent Hotel Census
# INDEPENDENT_CENSUS_PIPELINE_ENABLED=0
```

### Library layout

| Path | Purpose |
|------|---------|
| `lib/independent-census/fields.js` | Table names + field constants |
| `lib/independent-census/platform-base.js` | Staging base accessor |
| `lib/independent-census/normalize.js` | Name/address/country normalization |
| `lib/independent-census/match-current-census.js` | Read-only `Hotel Census` match fields |
| `lib/independent-census/sources/osm.js` | Overpass / extract → candidates |
| `lib/independent-census/sources/wikidata.js` | SPARQL / entity API |
| `lib/independent-census/sources/brand-directory.js` | Manual CSV/JSON uploads |
| `lib/independent-census/sources/government-registry.js` | Per-country adapters |
| `lib/independent-census/candidate-writer.js` | Batch create to Candidates only |
| `lib/independent-census/promote-verified.js` | Human-gated promote (CLI `--approved-by`) |

### Scripts

| Script | Behavior |
|--------|----------|
| `scripts/ensure-independent-census-tables.mjs` | Create 3 staging tables only |
| `scripts/inventory-current-census-for-reconciliation.mjs` | Read-only snapshot + CSV |
| `scripts/import-osm-candidates.mjs` | OSM → Candidates only |
| `scripts/import-wikidata-candidates.mjs` | Wikidata → Candidates only |
| `scripts/import-brand-directory-candidates.mjs` | fixtures/uploads CSV → Candidates |
| `scripts/import-government-registry-candidates.mjs` | Registry adapters |
| `scripts/match-candidates-to-current-census.mjs` | Updates match fields on Candidates only |
| `scripts/reconcile-independent-vs-current-census.mjs` | Coverage / duplicate report |
| `scripts/promote-candidate-to-verified.mjs` | Requires `--record-id` + `--approved-by` |

**Suggested `package.json` scripts:** `inventory-independent-census`, `import-osm-candidates`, `match-independent-candidates`, `reconcile-independent-census`.

### API routes (internal, disabled by default)

Do not wire to Brand Explorer, scoring, Scout, or Radar until a later phase.

| Route | Purpose |
|-------|---------|
| `GET /api/internal/independent-census/candidates` | Ops review list (`INDEPENDENT_CENSUS_PIPELINE_ENABLED`) |
| `GET /api/internal/independent-census/reconciliation` | Coverage vs `Hotel Census` |
| `POST /api/internal/independent-census/promote` | Human-approved promotion only |

### Reports (`reports/`)

- `independent-census-import-{batch}.json`
- `independent-census-reconciliation.csv`
- `independent-census-duplicate-candidates.csv`

### Related documentation (future)

- `docs/independent-hotel-census-pipeline.md` — licenses, source matrix, promotion SOP

---

## Current census touchpoints (do not extend for independent census)

| Area | Usage |
|------|--------|
| **APIs** | `api/brand-presence.js`, `api/brand-presence-summary.js`, `api/operators-by-brand-region.js`, `api/brand-fit-analyzer.js`, `api/brand-library.js` (`censusSummary` when `BRAND_EXPLORER_CENSUS_METRICS=1`) |
| **UI** | `public/brand-presence-mapping.js`, radar HTML pages, `public/dealality-scout.js`, `public/largest-operators-by-brand.html` |
| **Writes to Hotel Census** | `scripts/apply-str-census-import.mjs`, `scripts/apply-str-import-remaining.mjs`, `scripts/mark-str-import-census-rows.mjs`, `scripts/ensure-hotel-census-governance-fields.mjs` |

**Read-only comparison tooling to reuse:**

- `scripts/inventory-hotel-census-for-str-import.mjs`
- `scripts/audit-census-affiliations.mjs`
- `scripts/audit-brand-explorer-census-coverage.mjs`
- `lib/str-census-import/match-excel-to-census.mjs` (adapt matching logic without STR IDs or writes)

---

## Rule alignment checklist

| Rule | How this inventory supports it |
|------|--------------------------------|
| Current census untouched | Staging tables are net-new; current table only linked/read for match fields |
| Read-only census use | Duplicate/coverage via match + reconciliation reports |
| Open data → Candidates only | Section 6, Table A |
| No auto-promotion | Verified table + explicit promote script/API |
| No production UI yet | Internal routes behind flag |
| No STR/CoStar in new census | Section 5A blocklist; separate from `lib/str-census-import/` |

---

## Suggested implementation order (after approval)

1. Refresh inventory: `node scripts/inventory-hotel-census-for-str-import.mjs`
2. Create staging tables: `scripts/ensure-independent-census-tables.mjs` (new)
3. OSM ingest → Candidates only, then dry-run `match-candidates-to-current-census.mjs`
4. Human review in Airtable; promote only via explicit approve workflow
