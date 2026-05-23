# Independent Hotel Census Pipeline

Parallel hotel census built from **open/public/free** sources. The existing **`Hotel Census`** table (STR-backed, production) stays untouched.

**Inventory reference:** [independent-hotel-census-inventory.md](./independent-hotel-census-inventory.md)

**Code (Phase 1):**

- `lib/independent-census/fields.js` — table names and Airtable field constants
- `lib/independent-census/platform-base.js` — base accessor and pipeline enable flag

---

## Principles

| Rule | Detail |
|------|--------|
| Do not modify `Hotel Census` | No overwrite, delete, rename, or restructure |
| Read-only use of current census | Duplicate detection, coverage, reconciliation reports only |
| Ingest → Candidates only | OSM, Wikidata, brand directories, government registries, manual uploads |
| No STR/CoStar in staging | STR Excel and CoStar-derived fields are blocklisted for this pipeline |
| No auto-promotion | Human approval required before a **Verified** row exists |
| No production UI (yet) | Brand Explorer, Scout, Radar, brand presence, scoring unchanged |

---

## Airtable tables (manual create in Platform base)

Create these three tables in **Deal Capture Platform** (`AIRTABLE_BASE_ID_ALT`). Names must match env vars or defaults in `lib/independent-census/fields.js`.

### 1. Independent Hotel Source Candidates

| Airtable field | Type | Notes |
|----------------|------|--------|
| Source Name | Single line text | Dataset label, e.g. OpenStreetMap |
| Source Type | Single select | `osm`, `wikidata`, `brand_directory`, `government_registry`, `manual_upload` |
| Source License | Single line text | SPDX or license URL |
| Source URL | URL | Record or dataset URL |
| Source Record ID | Single line text | External id (node id, Q-id, registry id) |
| Raw Hotel Name | Single line text | |
| Raw Address | Single line text | |
| Raw City | Single line text | |
| Raw Country | Single line text | |
| Raw Latitude | Number | |
| Raw Longitude | Number | |
| Raw Website | URL | |
| Raw Phone | Single line text | |
| Raw Brand | Single line text | |
| Raw Payload JSON | Long text | Full source JSON |
| Import Batch ID | Single line text | |
| Imported At | Date/time | |
| Review Status | Single select | `pending`, `in_review`, `approved`, `rejected`, `duplicate` |
| Possible Match in Current Census | Link → **Hotel Census** | Comparison only |
| Possible Match Confidence | Single select | `none`, `low`, `medium`, `high` |
| Recommended Action | Single select | `promote`, `merge_with_census`, `skip`, `needs_research` |
| Candidate Dedupe Key | Single line text | Normalized dedupe key |
| Linked Evidence | Link → Independent Hotel Source Evidence | Optional |

### 2. Independent Hotel Source Evidence

| Airtable field | Type | Notes |
|----------------|------|--------|
| Candidate | Link → Candidates | |
| Evidence Type | Single select | `source_snapshot`, `geocode`, `manual_note`, `census_comparison`, `license_check` |
| Evidence URL | URL | |
| Evidence Text | Long text | |
| Captured At | Date/time | |
| Captured By | Single line text | |
| Compares To Census Record | Link → **Hotel Census** | Read-only comparison |
| Match Score | Number | 0–100 |
| Match Reason | Long text | |

### 3. Verified Independent Hotel Census

| Airtable field | Type | Notes |
|----------------|------|--------|
| Verified Hotel Name | Single line text | |
| Verified Address | Single line text | |
| Verified City | Single line text | |
| Verified State | Single line text | |
| Verified Country | Single line text | |
| Verified Postal Code | Single line text | |
| Verified Latitude | Number | |
| Verified Longitude | Number | |
| Verified Website | URL | |
| Verified Phone | Single line text | |
| Verified Brand Label | Single line text | Not `Hotel Census`.`Affiliation` |
| Primary Source Candidate | Link → Candidates | |
| Approved At | Date/time | |
| Approved By | Single line text | |
| Approval Notes | Long text | |
| Census Reconciliation Status | Single select | `not_in_census`, `likely_duplicate`, `matched_to_census` |
| Linked Census Record | Link → **Hotel Census** | Optional reference |
| Verified Dedupe Key | Single line text | |
| Active | Checkbox | Soft delete |

---

## Environment variables

Add to `.env` (see `.env.example`):

```bash
AIRTABLE_INDEPENDENT_CENSUS_CANDIDATES_TABLE=Independent Hotel Source Candidates
AIRTABLE_INDEPENDENT_CENSUS_EVIDENCE_TABLE=Independent Hotel Source Evidence
AIRTABLE_VERIFIED_INDEPENDENT_CENSUS_TABLE=Verified Independent Hotel Census
INDEPENDENT_CENSUS_PIPELINE_ENABLED=false
```

Requires existing Platform config:

- `AIRTABLE_API_KEY`
- `AIRTABLE_BASE_ID_ALT`

`INDEPENDENT_CENSUS_PIPELINE_ENABLED` gates **future** internal scripts and APIs. Schema **apply** also requires this flag (or `--force`).

**Schema setup (Phase 2A):**

```bash
npm run independent-census:schema:check
npm run independent-census:schema:apply
```

Report: `reports/independent-census-schema-check.json`

Link fields to **`Hotel Census`** are **not** auto-created (Airtable would add inverse columns on that table). Add `Possible Match in Current Census`, `Compares To Census Record`, and `Linked Census Record` manually in Airtable if needed.

**Multi-source framework (Phase 2D):**

| Module | Role |
|--------|------|
| `lib/independent-census/source-registry.js` | Source profiles (OSM, Wikidata, brand directory, government registry, Google Places, submitted, manual upload) |
| `lib/independent-census/source-policy.js` | Conservative product/storage policy helpers; unknown sources → high risk, no product use |
| `lib/independent-census/normalize-candidate.js` | Shared candidate shape, dedupe key, quality score, policy flags |

**OSM dry-run (Phase 2B / 2B+ / 2D):**

```bash
# Broad discovery (all tourism tags including apartment)
npm run independent-census:osm:dry-run -- --country "Dominican Republic" --max-elements 2000 --batch-id osm-dominican-republic-expanded-2026-05-20

# Hotel-focused (recommended before staging): hotel/resort/guest_house/hostel/motel only; no apartments or unnamed unless flags set
npm run independent-census:osm:dry-run -- --country "Dominican Republic" --max-elements 2000 --hotel-focused --batch-id osm-dominican-republic-hotel-focused-2026-05-20
```

| Flag | Default | Effect |
|------|---------|--------|
| `--hotel-focused` | off | Overpass queries hotel-focused tags; post-filter excludes `tourism=apartment` and unnamed |
| `--include-apartments` | off | Keep `tourism=apartment` when used with `--hotel-focused` |
| `--include-unnamed` | off | Keep records without a name |
| `--min-quality` | none | Drop candidates below heuristic quality score (0–100) |
| `--max-elements` | none | Cap Overpass `out center N` |
| `--limit` | none | Cap normalized candidates after filtering |

- No default `--limit`; use `--default-max-elements` for safety cap 10000 when uncapped.
- Reports include filtering stats, quality tiers, source policy summary, and optional comparison vs expanded 2B+ baseline.

Reports: `reports/independent-census-osm-dry-run-{batchId}.json` and `.csv`. No `--apply`; no Airtable writes.

**OSM ↔ current census match (Phase 2C):**

```bash
npm run independent-census:osm:match-current -- --input reports/independent-census-osm-dry-run-{batchId}.json
```

Reports: `reports/independent-census-osm-current-match-{batchId}.json` and `.csv`. Read-only Hotel Census (no STR fields).

**Verified schema design (Phase 2H):** [verified-independent-hotel-census-schema.md](./verified-independent-hotel-census-schema.md)

**Candidate staging apply (Phase 3A):**

```bash
# Dry-run (default)
npm run independent-census:candidates:apply -- \
  --input reports/independent-census-osm-dry-run-osm-dominican-republic-hotel-focused-2026-05-20.json \
  --match-report reports/independent-census-osm-current-match-osm-dominican-republic-hotel-focused-2026-05-20.json \
  --batch-id osm-dominican-republic-hotel-focused-2026-05-20 \
  --min-quality medium \
  --exclude-actions skip_missing_name,possible_duplicate_review

# Apply (requires INDEPENDENT_CENSUS_PIPELINE_ENABLED=true)
npm run independent-census:candidates:apply -- ...same args... --apply
```

- Writes **only** to `Independent Hotel Source Candidates`.
- Match metadata (confidence, reason, read-only census record id) stored in `Raw Payload JSON` — **no** Hotel Census link fields via API.
- Dedupes on `Source Type` + `Source Record ID` + `Import Batch ID`.
- Maps match actions (`likely_existing`, etc.) to staging `Recommended Action` single-select values.
- Reports: `reports/independent-census-candidate-apply-{batchId}.json` / `.csv`

**Brand directory discovery (Phase 2F, report-only):**

This is **not** an aggressive scraping system. Brand/company sites may restrict robots, spiders, or automated extraction. Treat each directory under **source-specific terms**; manual/permissioned CSV/JSON first.

```bash
# Seed URLs / placeholder property links (no crawl)
npm run independent-census:brand-directory:dry-run -- \
  --mode search-list \
  --input fixtures/independent-census/brand-directory-seeds.example.json \
  --batch-id brand-directory-seeds-example-2026-05-20

# Manual export (future)
npm run independent-census:brand-directory:dry-run -- --mode manual-file --input path/to/export.csv --batch-id choice-manual-2026-05-20

# Explicit sitemap URL only (respects robots.txt; hotel-looking URLs → discovery leads)
npm run independent-census:brand-directory:dry-run -- --mode sitemap --brand "Example" --source-url https://example.com/sitemap.xml --batch-id example-sitemap-2026-05-20
```

Flow:

```
brand directory URL / manual CSV
  → discovery lead or candidate (needs_research)
  → match OSM / read-only Hotel Census
  → human review
  → future promotion to Verified Independent Hotel Census (validation rules)
```

- **Do not** scrape photos, reviews, rates, booking availability, or pricing.
- **Do not** use brand directory data in user-facing features until reviewed.
- **Reject `--apply`** on brand-directory script (no Airtable writes).
- Template: `fixtures/independent-census/brand-directory-template.csv`
- Example seeds: `fixtures/independent-census/brand-directory-seeds.example.json`
- Reports: `reports/independent-census-brand-directory-dry-run-{batchId}.json` / `.csv`

**Promotion validation (future Phase 4):** A verified row requires one strong official/permissioned source **or** two independent supporting sources (e.g. OSM + Wikidata). See Phase 2H schema doc.

**Wikidata dry-run (Phase 2E, report-only):**

```bash
npm run independent-census:wikidata:dry-run -- --country "Dominican Republic" --limit 500 --batch-id wikidata-dominican-republic-2026-05-20

# Read-only match vs staging OSM candidates (same market batch)
npm run independent-census:wikidata:match-candidates -- \
  --input reports/independent-census-wikidata-dry-run-wikidata-dominican-republic-2026-05-20.json \
  --source-batch-id osm-dominican-republic-hotel-focused-2026-05-20
```

- SPARQL via [Wikidata Query Service](https://query.wikidata.org/) — lodging types (hotel, hostel, motel, inn, resort, tourist accommodation).
- Normalized via `buildIndependentCandidate()`; license **CC0**; operator/owner/Wikipedia in payload.
- Wikidata-specific missing flags: operator, owner (plus standard name/city/coords/website).
- **No `--apply`** — does not write to Candidates, Evidence, or Verified.
- Match script reads **Independent Hotel Source Candidates** (OSM batch) read-only; compares name, city/country, geo distance, website host.
- Reports: `reports/independent-census-wikidata-dry-run-{batchId}.json` / `.csv`
- Match reports: `reports/independent-census-wikidata-candidate-match-{batchId}.json` / `.csv`

**Validation evidence (Phase 4A):**

Attaches **Wikidata** as `source_snapshot` evidence on matched **OSM** staging candidates (does not create duplicate Wikidata candidate rows).

```bash
# Dry-run
npm run independent-census:evidence:apply -- \
  --match-report reports/independent-census-wikidata-candidate-match-wikidata-dominican-republic-2026-05-20.json \
  --wikidata-report reports/independent-census-wikidata-dry-run-wikidata-dominican-republic-2026-05-20.json \
  --batch-id osm-wikidata-dr-high-confidence-2026-05-20 \
  --min-confidence high

# Apply
npm run independent-census:evidence:apply -- ... --apply
```

- Default `--min-confidence high`; optional `--include-medium`.
- Writes **only** `Independent Hotel Source Evidence` (links `Candidate` → OSM staging row).
- Dedupes on Evidence `Name`: `4A|{batchId}|{QID}|{osmSourceRecordId}`.
- `Captured By` = `Phase 4A independent-census-evidence`.
- **No** Verified promotion, **no** Hotel Census links, **no** new Wikidata candidates.
- Reports: `reports/independent-census-evidence-apply-{batchId}.json` / `.csv`

**Choice brand-directory evidence (Phase 4Q):**

Attaches **Choice property sitemap URLs** as `source_snapshot` evidence on **high/medium** OSM candidate matches from Phase 4P (no property HTML fetched).

```bash
npm run independent-census:evidence:apply -- \
  --evidence-source brand_directory \
  --match-report reports/independent-census-choice-property-match-prioritized-2026-05-20.json \
  --batch-id choice-brand-directory-evidence-2026-05-20 \
  --min-confidence medium

# Apply (requires INDEPENDENT_CENSUS_PIPELINE_ENABLED=true)
npm run independent-census:evidence:apply -- ... --apply
```

- Selects rows with `link_to_existing_candidate_review`, `candidateMatchConfidence` ≥ medium, `review_required` / `approved_for_internal_staging`.
- Dedupes on Evidence `Name`: `4Q|{batchId}|{choicePropertyId}|{matchedCandidateRecordId}`.
- `Captured By` = `Phase 4Q brand-directory-evidence`.
- **No** Verified promotion, **no** Candidate updates.

**Corrected Choice evidence from OSM website reconciliation (Phase 4U):**

Creates **new** evidence rows from Phase 4T direct property ID / URL matches (does not modify Phase 4Q collision rows).

```bash
npm run independent-census:evidence:apply -- \
  --evidence-source choice_property_id_reconciliation \
  --reconciliation-report reports/independent-census-choice-property-id-reconciliation-2026-05-20.json \
  --batch-id choice-property-id-corrected-evidence-2026-05-20 \
  --include-match-types direct_property_id_match,direct_property_url_match \
  --property-ids mx071

# Apply (requires INDEPENDENT_CENSUS_PIPELINE_ENABLED=true)
npm run independent-census:evidence:apply -- ... --apply
```

- Dedupes on Evidence `Name`: `4U|{batchId}|{choicePropertyId}|{candidateRecordId}`.
- `Captured By` = `Phase 4U choice-property-id-corrected-evidence`.
- Optional `--property-ids` filter (e.g. `mx071` for Mazatlán pilot).

**Promotion review for corrected Choice evidence (Phase 4V, report-only):**

Uses **only** Phase 4U evidence batch (`4U|…`); ignores prior 4Q collision rows.

```bash
npm run independent-census:promotion:review -- \
  --evidence-source choice_property_id_reconciliation \
  --evidence-batch-id choice-property-id-corrected-evidence-2026-05-20 \
  --candidate-source-type osm \
  --batch-id choice-property-id-corrected-promotion-review-2026-05-20
```

Reports: `reports/independent-census-promotion-review-choice-property-id-corrected-2026-05-20.json` / `.csv`

**Promotion review (Phase 4B / 4R, report-only):**

Read-only report for human reviewers — **no** Verified writes.

```bash
# OSM + Wikidata (4B)
npm run independent-census:promotion:review -- \
  --evidence-source wikidata \
  --evidence-batch-id osm-wikidata-dr-high-confidence-2026-05-20 \
  --candidate-batch-id osm-dominican-republic-hotel-focused-2026-05-20

# OSM + Choice brand-directory (4R)
npm run independent-census:promotion:review -- \
  --evidence-source brand_directory \
  --evidence-batch-id choice-brand-directory-evidence-2026-05-20 \
  --candidate-source-type osm \
  --batch-id choice-brand-directory-promotion-review-2026-05-20
```

Reports: `reports/independent-census-promotion-review-{reportSlug}.json` / `.csv` (4R slug defaults to `choice-brand-directory-2026-05-20`).

**Choice property ID collision review (Phase 4S, report-only):**

Scores competing Choice property IDs when multiple URLs link to one OSM candidate (Phase 4R collision groups).

```bash
npm run independent-census:brand-directory:collision-review -- \
  --promotion-review reports/independent-census-promotion-review-choice-brand-directory-2026-05-20.json \
  --match-report reports/independent-census-choice-property-match-prioritized-2026-05-20.json \
  --property-url-report reports/independent-census-choice-property-url-extract-cala-2026-05-20.json \
  --batch-id choice-collision-review-2026-05-20
```

Reports: `reports/independent-census-choice-collision-review-2026-05-20.json` / `.csv`

**Choice property ID reconciliation from OSM websites (Phase 4T, report-only):**

Scans OSM staging candidates for `choicehotels.com` / Radisson Americas / WoodSpring URLs in website or payload; extracts property IDs and matches against the Choice sitemap extract (stronger signal than broad URL-only matching).

```bash
npm run independent-census:choice:property-id-reconcile -- \
  --property-url-report reports/independent-census-choice-property-url-extract-cala-2026-05-20.json \
  --candidate-retention-report reports/independent-census-candidate-coverage-dedupe-2026-05-20.json \
  --batch-id choice-property-id-reconcile-2026-05-20
```

Reports: `reports/independent-census-choice-property-id-reconciliation-2026-05-20.json` / `.csv`

- Loads Phase 4A evidence (`Name` prefix `4A|{evidenceBatchId}|`) and linked OSM candidates.
- Two-source rule: OSM candidate + high-confidence Wikidata evidence.
- Outputs eligibility (`eligible_for_review`, `needs_manual_research`, `possible_duplicate`, `insufficient_core_fields`) and recommendation (`promote_after_review`, `review_before_promote`, `do_not_promote_yet`).
- Includes **proposed** Verified v0.1 field mapping in JSON (not written to Airtable).
- Reports: `reports/independent-census-promotion-review-{evidenceBatchId}.json` / `.csv`
- Does **not** read or write `Hotel Census`.

**Verified promotion (Phase 4C, gated):**

First controlled write to **Verified Independent Hotel Census** — human-gated only.

```bash
# Dry-run
npm run independent-census:verified:promote -- \
  --review-report reports/independent-census-promotion-review-osm-wikidata-dr-high-confidence-2026-05-20.json \
  --batch-id osm-wikidata-dr-verified-v01-2026-05-20 \
  --approved-by "Your Name" \
  --max-records 35

# Apply (requires INDEPENDENT_CENSUS_PIPELINE_ENABLED=true)
npm run independent-census:verified:promote -- ... --apply
```

- Promotes only `promote_after_review` + `eligible_for_review` from Phase 4B report.
- Requires `--approved-by` on `--apply`.
- Dedupes on `Verified Dedupe Key`, `Primary Source Candidate` link, and name+country+nearby geo.
- Maps **existing** Verified table fields only (Phase 2A schema).
- `Census Reconciliation Status` = `not_in_census` (no Hotel Census API links).
- Does **not** update Candidates or Evidence rows.
- Reports: `reports/independent-census-verified-promote-{batchId}.json` / `.csv`

**Manual follow-up (Phase 4D, report-only):**

Work queue for remaining `review_before_promote` + `needs_manual_research` rows (no promotion).

```bash
npm run independent-census:promotion:manual-review -- \
  --review-report reports/independent-census-promotion-review-osm-wikidata-dr-high-confidence-2026-05-20.json \
  --batch-id osm-wikidata-dr-manual-followup-2026-05-20
```

- Classifies manual review reason, review priority (high/medium/low), and suggested next action.
- CSV includes blank `humanNotes` and `readyForFuturePromotion` columns for ops.
- Reports: `reports/independent-census-manual-followup-{reportSlug}.json` / `.csv` (`reportSlug` = `--batch-id` with `-manual-followup` stripped; e.g. batch `osm-wikidata-dr-manual-followup-2026-05-20` → `osm-wikidata-dr-2026-05-20`).
- No Airtable writes.
- DR high-confidence batch (2026-05-20): **61** rows, all `missing_city` / `add_city_from_manual_review` (**13** high, **48** medium priority).

**Coverage benchmark (Phase 4E, read-only):**

Compare legacy `Hotel Census` to staging Candidates, Evidence, and Verified — no data copy from census.

```bash
npm run independent-census:coverage:benchmark -- \
  --country "Dominican Republic" \
  --batch-id coverage-dr-2026-05-20
```

- Safe census fields only (name, city, country, lat/lng, website, phone, affiliation, parent company, status, rooms).
- Match logic: name + country + city when present, plus geo distance and website host (no STR Number / Property ID).
- Metrics: counts by country, candidate/verified coverage % vs legacy, overlap/gap estimates, duplicate-risk clusters, missing-field stats, lowest verified-coverage markets.
- DR section: legacy vs OSM candidates vs Wikidata evidence vs verified, with recommended next validation sources.
- Reports: `reports/independent-census-coverage-benchmark-{batchId}.json` / `.csv`
- No Airtable writes.

**Brand Setup CALA inventory (Phase 4F, read-only):**

Parent-company prioritization from **Brand Setup - Brand Basics** (not Hotel Census). Default: **all** brand records; pass `--activeOnly=true` to limit to Active/Live.

```bash
npm run independent-census:brand-setup:cala-inventory
```

- Reports: `reports/brand-setup-cala-parent-company-inventory.json` / `.csv`, `reports/brand-setup-cala-brand-gaps.csv`
- Docs: `docs/parent-company-census-prioritization.md`
- No Airtable writes.

**Brand directory from Brand Setup (Phase 4G, report-only):**

```bash
npm run independent-census:brand-directory:generate-seeds-from-brand-setup -- \
  --parent-company "Choice Hotels International" \
  --normalized-parent-company "choice hotels"

npm run independent-census:brand-directory:dry-run -- \
  --mode search-list \
  --input fixtures/independent-census/brand-directory-seeds-choice-hotels-from-brand-setup.json \
  --batch-id brand-directory-choice-hotels-brand-setup-2026-05-20
```

- Seeds from Brand Setup fields; all statuses unless `--activeOnly=true`
- Docs: `docs/parent-company-census-prioritization.md`

**Property-path analysis (Phase 4H, read-only):**

```bash
npm run independent-census:brand-directory:analyze-property-paths -- \
  --input fixtures/independent-census/brand-directory-seeds-choice-hotels-from-brand-setup.json \
  --batch-id choice-property-paths-2026-05-20
```

- One GET per brand `sourceUrl`; optional `robots.txt` + single sitemap probe (no deep crawl).

**Sitemap child-index review (Phase 4I, report-only):**

```bash
npm run independent-census:brand-directory:review-sitemap -- \
  --sitemap-url "https://www.choicehotels.com/sitemapindex.xml" \
  --brand-seeds fixtures/independent-census/brand-directory-seeds-choice-hotels-from-brand-setup.json \
  --max-child-sitemaps 5 --max-urls 500 --batch-id choice-sitemap-review-2026-05-20
```

**Property URL extract (Phase 4J, report-only):**

1. Choice property-level URLs are feasible from `propertysitemap.xml.gz` (gzip sitemap XML).
2. Extraction is **URL-only** — no property HTML fetch.
3. Does **not** create Independent Hotel Source Candidates (ingest requires source policy sign-off).
4. **Next:** match extracted CALA Choice property URLs to OSM Candidates and Verified Independent Hotel Census (safe name + geo; no STR).

```bash
npm run independent-census:brand-directory:extract-property-urls -- \
  --property-sitemap-url "https://www.choicehotels.com/propertysitemap.xml.gz" \
  --brand-seeds fixtures/independent-census/brand-directory-seeds-choice-hotels-from-brand-setup.json \
  --region-filter CALA --batch-id choice-property-urls-cala-2026-05-20
```

Reports: `reports/independent-census-choice-property-url-extract-cala-2026-05-20.json` / `.csv`

**Property URL match (Phase 4K, report-only):**

1. Phase 4J produced official Choice property-level URL leads.
2. Phase 4K compares those leads to OSM **Candidates** and **Verified** (read-only).
3. No candidate ingest until **source policy sign-off**.
4. Next controlled step: create Choice `brand_directory` candidate or evidence rows if matching is validated.

```bash
npm run independent-census:brand-directory:match-properties -- \
  --property-url-report reports/independent-census-choice-property-url-extract-cala-2026-05-20.json \
  --candidate-batch-id osm-dominican-republic-hotel-focused-2026-05-20 \
  --batch-id choice-property-match-cala-2026-05-20
```

Reports: `reports/independent-census-choice-property-match-cala-2026-05-20.json` / `.csv`

**Property URL candidate apply (Phase 4L, gated):**

1. Phase 4J produced official Choice property-level URL leads.
2. Phase 4K compared leads to OSM Candidates and Verified (read-only).
3. Phase 4L writes **Candidates** only with `--apply` + `--source-policy-approved` + pipeline flag.
4. No Verified promotion; Evidence rows optional later after human review.

```bash
npm run independent-census:brand-directory:apply-property-candidates -- \
  --property-url-report reports/independent-census-choice-property-url-extract-cala-2026-05-20.json \
  --match-report reports/independent-census-choice-property-match-cala-2026-05-20.json \
  --batch-id choice-cala-property-url-candidates-2026-05-20
```

**OSM CALA country-list expansion (Phase 4M):**

```bash
npm run independent-census:osm:country-list -- \
  --countries "Colombia,Mexico,..." \
  --batch-id choice-cala-osm-expansion-2026-05-20
```

Hotel-focused, excludes apartments/unnamed by default, `min-quality` medium. Writes **Candidates** only with `--apply`.

**Choice property re-match (Phase 4N, read-only):**

```bash
npm run independent-census:brand-directory:match-properties -- \
  --property-url-report reports/independent-census-choice-property-url-extract-cala-2026-05-20.json \
  --all-osm-candidates \
  --batch-id choice-property-match-cala-expanded-osm-2026-05-20
```

Uses all OSM batches with `choice-cala-2026-05-20` in Import Batch ID (~48k rows). Optional: `--candidate-batch-ids`, `--country-filter`.

**Candidate coverage / dedupe / retention (Phase 4O, read-only):**

```bash
npm run independent-census:candidates:coverage -- --batch-id candidate-coverage-dedupe-2026-05-20
```

Reports: `reports/independent-census-candidate-coverage-dedupe-2026-05-20.json` / `.csv`

**Prioritized Choice rematch (Phase 4P, read-only):**

```bash
npm run independent-census:brand-directory:match-properties -- \
  --property-url-report reports/independent-census-choice-property-url-extract-cala-2026-05-20.json \
  --candidate-retention-report reports/independent-census-candidate-coverage-dedupe-2026-05-20.json \
  --include-retention keep_high_priority,enrich_next,keep_for_matching \
  --batch-id choice-property-match-prioritized-2026-05-20
```

Compares to Phase 4N in report `comparisonPhase4N`.

---

## Source ladder (why multi-source)

Independent census quality comes from **combining** sources with different strengths — not from treating any single feed as the master.

| Layer | Source type | Role |
|-------|-------------|------|
| Map / discovery | **OpenStreetMap** (ODbL) | Open POI backbone: coordinates, breadth, tourism tags; weak city/website/phone in many markets |
| Open enrichment | **Wikidata** (CC0) | Notable hotels, websites, coordinates, operators when present; incomplete coverage |
| Official brand | **Brand directory** (source-specific terms) | Marriott, Choice, Hilton, Hyatt, IHG, Accor, etc. — official name, brand, URL, opening status; manual CSV/JSON only until terms reviewed |
| Jurisdictional | **Government registry** | License/registration, legal name, room counts where published; fragmented by country |
| Identity lookup | **Google Places** (Maps Platform Terms) | Enrichment and identity lookup — **not** permanent master census; refresh required; no photos/reviews in storage |
| Permissioned truth | **Owner / brand / operator submitted** | Highest long-term confidence when the relevant party submits under platform terms |

### Why one source is not enough

- **OSM** has geographic breadth and open licensing but sparse business attributes (e.g. DR expanded run: ~9% with city, ~8% website).
- **Brand directories** carry official brand facts but terms vary; no autonomous scraping without legal review per chain.
- **Google** has strong identity signals but storage, attribution, and refresh constraints — coordinates and display fields are not assumed permanent.
- **Government registries** are authoritative locally but heterogeneous and slow to integrate per jurisdiction.
- **Submitted data** is strongest when available but depends on platform participation and review.

All normalized rows pass through `buildIndependentCandidate()` and `getSourcePolicy()` before any future staging write.

---

## Data Steward Agent roadmap (Phase 5+)

The independent census should eventually run as a **scheduled, AI-assisted data stewardship workflow** — not a fully autonomous production updater.

### Scheduled source refresh jobs (future)

| Source | Suggested cadence | Notes |
|--------|-------------------|--------|
| OSM (Overpass) | Monthly (priority markets); quarterly (secondary) | Discovery + coordinate refresh; hotel-focused filters by default |
| Wikidata | Monthly / quarterly | Enrichment for Q-id linked properties |
| Brand directory | On manual import + quarterly review | CSV/JSON drops only until per-source terms allow automation |
| Government registry | Per jurisdiction when adapter exists | License/status verification |
| Google Places | On-demand enrichment | Lookup by name/geo; refresh Place-linked fields on schedule |
| Submitted updates | Real-time / weekly queue | Owner/brand/operator corrections |

**On-demand runs:** before demos, investor meetings, and owner/brand outreach in a target market.

### AI-assisted tasks (human-in-the-loop)

- Name normalization and transliteration
- Duplicate detection across sources and vs read-only `Hotel Census`
- Brand / operator inference with evidence citations
- City and submarket suggestion from geo + registry hints
- Source evidence summarization for review queue
- Confidence scoring and change-detection notes
- Review queue prioritization (quality tier, market, gap vs census)

### Guardrails (non-negotiable)

- **No automatic writes** to `Hotel Census`, `Brand Alias Mapping`, or production scoring/UI
- **No STR/CoStar-derived fields** in independent pipeline or match exports
- **No Google-derived permanent master fields** unless Maps Platform terms explicitly allow retention
- **No brand-directory scraping** without source-specific legal review
- **No AI self-approval** — scripts and models may propose; humans approve
- Promotion to **Verified Independent Hotel Census** only after explicit review
- Optional later merge into production `Hotel Census` only after a separate migration approval

---

## Phases (roadmap)

| Phase | Scope | Status |
|-------|--------|--------|
| **1** | `fields.js`, `platform-base.js`, docs, `.env.example` | Done |
| **2A** | `ensure-independent-census-tables.mjs` (schema check/apply) | Done |
| **2B** | OSM Overpass dry-run (nodes) | Done |
| **2B+** | OSM: ways/relations, `out center`, addr/city enrichment | Done |
| **2C** | Read-only OSM ↔ `Hotel Census` match reports | Done |
| **2D** | Multi-source framework + OSM hotel-focused filters + steward roadmap doc | Done |
| **2H** | Verified Independent Hotel Census schema proposal (docs only) | Done |
| **2E** | Wikidata dry-run + read-only match vs staging OSM candidates | Done |
| **2F** | Brand directory discovery framework (manual-file / sitemap / search-list) | Done |
| **2G** | Google Places lookup/enrichment dry-run (no API in dry-run phase) | Planned |
| **3A** | OSM candidates → **Independent Hotel Source Candidates** (`--apply` gated) | Done |
| **4A** | Wikidata evidence on high-confidence OSM matches → **Evidence** (`--apply` gated) | Done |
| **4B** | Human promotion review report (OSM + Wikidata two-source rule) | Done |
| **4C** | Gated promote `promote_after_review` → **Verified Independent Hotel Census** | Done |
| **4D** | Manual follow-up report for `review_before_promote` rows | Done |
| **4E** | Coverage benchmark vs legacy `Hotel Census` (read-only) | Done |
| **4F** | Brand Setup CALA parent-company inventory (read-only) | Done |
| **4G** | Brand-directory seeds from Brand Setup + Choice dry-run | Done |
| **4H** | Choice property-path analysis (sourceUrl / robots / sitemap probe) | Done |
| **4I** | Choice sitemap child-index review (capped child XML) | Done |
| **4J** | Choice property URL extract from propertysitemap.xml.gz | Done |
| **4K** | Match Choice CALA property URLs to OSM candidates + Verified | Done |
| **4L** | Gated apply Choice property URLs → Candidates (`brand_directory`) | Done |
| **4M** | OSM hotel-focused expansion for Choice CALA countries | Done |
| **4N** | Re-match Choice property URLs vs expanded CALA OSM candidates | Done |
| **4O** | Candidate coverage, dedupe, and retention report | Done |
| **4P** | Prioritized Choice rematch (retention-filtered OSM pool) | Done |
| **3** | Additional source importers → Candidates | Planned |
| **4** | Human review → **Verified Independent Hotel Census** | Planned |
| **5** | Scheduled Data Steward Agent (AI-assisted, flag-gated) | Planned |

---

## Blocked / restricted data

Do **not** copy into staging or verified tables:

- `STR Number`, `Property ID`, `Chain ID`, `Market`, `Submarket`, ADR, RevPAR, occupancy, rate fields, `Development Cost` markers, or other STR/CoStar columns listed in the inventory doc
- STR Excel under `data/str-imports/` except as a separate, explicitly approved reference workflow

---

## Comparison with current `Hotel Census`

When matching candidates to the current census (Phase 2+):

1. Normalized **name + city + country** (same pattern as `lib/str-census-import/normalize.mjs`)
2. Optional geo proximity when lat/lng exist
3. **Do not** match on STR Number from open sources

Updates from matching must only set candidate fields:

- `Possible Match in Current Census`
- `Possible Match Confidence`
- `Recommended Action`

Never update `Hotel Census` from this pipeline.

---

## Promotion workflow (future)

1. Ops sets candidate `Review Status` to `approved`.
2. `scripts/promote-candidate-to-verified.mjs` (future) requires `--record-id` and `--approved-by`.
3. Creates one **Verified Independent Hotel Census** row; links **Primary Source Candidate**.
4. Does not create or update **Hotel Census** rows.

---

## Files intentionally unchanged (Phase 2D)

- `Hotel Census` Airtable table and `lib/hotel-census/*` behavior (read-only match only)
- Staging tables: no writes until Phase 3+
- `api/brand-presence.js`, `api/brand-presence-summary.js`, `api/operators-by-brand-region.js`, `api/brand-library.js`
- Brand Explorer, Scout, Radar, and brand presence UI
- `lib/str-census-import/*` (report utils only) and STR apply scripts
