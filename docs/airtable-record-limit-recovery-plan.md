# Airtable record limit recovery plan

## Problem

The Independent Hotel Census staging base has reached Airtable’s record limit. OSM country imports loaded tens of thousands of **Independent Hotel Source Candidates** rows, treating Airtable as a raw discovery data lake. That usage is not sustainable for workflow tables.

## Principles

1. **Airtable is not the raw OSM data lake.** Raw discovery belongs on disk (and later SQLite/Postgres), not in workflow tables.
2. **Airtable stores curated workflow records only:**
   - High-priority candidates (`keep_high_priority`, `enrich_next`, `keep_for_matching`)
   - Evidence-supported candidates
   - Official-source leads (`brand_directory`)
   - Duplicate-review rows until resolved
   - Verified Independent Hotel Census records
3. **Legacy Hotel Census remains read-only** for backwards-match benchmarking. No STR/CoStar fields, no writes from this recovery track unless explicitly approved elsewhere.
4. **No automated archive or delete** without export, human review, and gated `--apply`.

## Classification (report-only)

The cleanup plan script (`npm run independent-census:candidates:cleanup-plan`) classifies every candidate from the Phase 4O coverage report plus optional backwards-match results:

| Classification | Intent |
|----------------|--------|
| `keep_in_airtable` | Website, brand, quality, promotion-ready, or non–low-priority retention |
| `evidence_supported_keep` | Import batch indicates evidence workflow |
| `verified_linked_keep` | Already linked in Verified |
| `official_source_keep` | Brand directory source |
| `high_confidence_match_keep` | High-confidence legacy census backwards match |
| `duplicate_review` | Retention `duplicate_review` or duplicate cluster |
| `export_to_raw_store_then_archive` | OSM `low_priority_hold` with no keep signals — export first |
| `low_priority_archive_later` | OSM low priority but not yet meeting full export criteria |
| `do_not_touch` | Default protected hold |

## Immediate execution sequence

1. **Fix Verified index reliability** — Paginated load, retries, backoff, timeout. Dry-run aborts if index fails unless `--allow-missing-verified-index`. `--apply` always aborts if index fails.
2. **Verify index load** — Fast check (no 46k rematch):
   ```bash
   npm run independent-census:backwards-match:legacy -- \
     --verified-index-check-only \
     --batch-id backwards-global-legacy-match-verified-index-check-2026-05-20
   ```
3. **Generate limited 250-record apply plan** — From global dry-run report (no Airtable writes):
   ```bash
   npm run independent-census:verified:apply-plan
   ```
   Outputs: `reports/independent-census-global-verified-apply-plan-001-2026-05-20.json` / `.csv`
4. **Export archive-later candidates to local raw store** — Before any Airtable archive:
   ```bash
   npm run independent-census:candidates:export-raw-store
   ```
   Outputs: `data/independent-census/raw/airtable-candidate-export-2026-05-20.json` / `.csv`
5. **Review apply plan** — Human sign-off on 250 high-confidence rows (Mexico → Ecuador priority).
6. **Limited Verified apply (future)** — Only after index check passes and plan review; `--apply --approved-by` with `INDEPENDENT_CENSUS_PIPELINE_ENABLED=true`. Cap with `--max-promotions 250`.
7. **Archive candidate records (future)** — Only with explicit archive `--apply` after export file confirmation. Never before export.

## Recovery sequence (reference)

1. **Global backwards-match dry-run** — Identify high-confidence legacy matches and promotion-eligible OSM rows without writing Verified (`independent-census:backwards-match:legacy`).
2. **Cleanup plan report** — Quantify keep vs archive-later counts and estimated record reduction.
3. **Export raw OSM** — Copy rows slated for `export_to_raw_store_then_archive` to `/data/independent-census/raw/` (see raw data store plan).
4. **Manual review** — Confirm no evidence-linked or verified-linked rows appear in export lists.
5. **Gated archive (future)** — Only after export verification; requires explicit `--apply` and protection rules for linked records.

## Commands

```bash
# Backwards-match dry-run (JSON candidates only; legacy census read-only)
npm run independent-census:backwards-match:legacy -- \
  --candidate-retention-report reports/independent-census-candidate-coverage-dedupe-2026-05-20.json \
  --all-countries \
  --include-retention "keep_high_priority,enrich_next,keep_for_matching,low_priority_hold" \
  --min-confidence high \
  --batch-id backwards-global-legacy-match-2026-05-20

# Cleanup / export plan (report only)
npm run independent-census:candidates:cleanup-plan -- \
  --input reports/independent-census-candidate-coverage-dedupe-2026-05-20.json \
  --backwards-match-report reports/independent-census-backwards-match-legacy-backwards-global-legacy-match-2026-05-20.json \
  --batch-id candidate-cleanup-plan-2026-05-20
```

## Safety guardrails (unchanged)

- No writes to legacy Hotel Census, Brand Setup, or Brand Alias Mapping in these phases.
- No Evidence or Candidate writes during backwards-match dry-run.
- Verified writes only with `--apply`, `--approved-by`, and `INDEPENDENT_CENSUS_PIPELINE_ENABLED=true`.
- No STR, CoStar, Google API, or brand property HTML in this pipeline.

## Related

- [Independent census raw data store plan](./independent-census-raw-data-store-plan.md)
