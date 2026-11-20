# GDI Surfe Usage V1.2 — Airtable persistence proposal

**Status:** `PERSISTENCE_DESIGNED` — awaiting founder/IC schema approval before table create or live sync.  
**Local SoT:** `data/group-demand-intelligence/surfe-usage/` (V1.1 lock + atomic JSON + JSONL retained).

## Reuse audit

Inspected AI Demand Positioning base (`appa2cE7FTRmIbB32`) inventory (`reports/gdi-airtable-complete-data-model-audit-v1`).

| Candidate | Verdict |
|-----------|---------|
| Group Demand Opportunities | Wrong grain (opportunities, not lookup events) |
| Decision Events | Semantic mismatch (validation/action/outcome, not Surfe billing) |
| Research Runs / Target Runs | Research coverage, not contact enrichment accounting |
| **New: GDI Surfe Usage Events** | **Proposed** — minimal usage-event table |

## Proposed table

- **Base:** AI Demand Positioning (`appa2cE7FTRmIbB32`)
- **Name:** `GDI Surfe Usage Events`
- **Identity:** `Lookup Event ID` (idempotent upsert)
- **Env:** `GDI_SURFE_USAGE_AIRTABLE_TABLE_ID` after create
- **Enable:** `GDI_SURFE_USAGE_AIRTABLE_SYNC_ENABLE=1`
- **Apply:** `GDI_SURFE_USAGE_AIRTABLE_DRY_RUN=0` (default remains dry-run)

Field map: see `PROPOSED_FIELD_MAP` in `lib/group-demand-intelligence/gdi-surfe-usage-airtable-sync-v1.js`.

## Explicit non-fields (privacy)

Do **not** store: emails, phones, contact names, raw Surfe payloads, API keys, accepted field values.

## Historical migration

Do **not** blindly sync all 29 existing ledger rows — they classify as `FIXTURE_TEST`. Sync excludes fixtures by default.

## Classification

`PERSISTENCE_DESIGNED` until table exists and dry-run → apply retrieval is proven.
