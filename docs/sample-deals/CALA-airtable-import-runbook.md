# CALA sample deals — Airtable import runbook

## Dry-run (no Airtable writes)

```bash
node scripts/dry-run-cala-sample-deal-import.mjs
```

**Output:** `data/cala-sample-import-dry-run/manifest.json` and `data/cala-sample-import-dry-run/<slug>.import.json`

Each bundle includes routed payloads for:

- Deals
- Location & Property (`Deal_ID` link)
- Market - Performance - Deal & Capital Structure
- Strategic Intent - Operational - Key Challenges
- Contact & Uploads
- Target List (status `Considering`, notes include review-set context)

## Seed Airtable

Requires `AIRTABLE_API_KEY` and `AIRTABLE_BASE_ID` in `.env`.

```bash
# Review dry-run JSON first, then:
node scripts/seed-cala-sample-deals.mjs --apply

# Replace prior CALA samples (Deal Status = "Sample — CALA demo"):
node scripts/seed-cala-sample-deals.mjs --apply --clean

# Single sample:
node scripts/seed-cala-sample-deals.mjs --apply --only aeropuerto-cancun-select-service
```

**Results:** `data/cala-sample-import-results.json` — maps `slug` → `dealId` (`rec…`).

### Optional env

| Variable | Purpose |
| --- | --- |
| `CALA_SAMPLE_USER_RECORD_ID` | Link deals to Users table |
| `CALA_SAMPLE_COMPANY_RECORD_ID` | Link Company Profile |
| `CALA_SEED_AIRTABLE_DELAY_MS` | Throttle between writes (default 350) |

## Implementation

- `lib/sample-deal-airtable-import.js` — fixture → table payloads (same routing as Deal Setup PATCH)
- `scripts/dry-run-cala-sample-deal-import.mjs` — write dry-run JSON
- `scripts/seed-cala-sample-deals.mjs` — apply to Airtable

## Schema note (fixed 2026-05-22)

Strategic Intent “Other” text fields use `… Other Text` column names in Airtable (e.g. `Top 3 Deal Breakers Other` → `Top 3 Deal Breakers Other Text`). The import mapper includes these overrides.

## Cleanup

Deals tagged `Deal Status` = `Sample — CALA demo` can be removed with `--clean` before re-seed.
