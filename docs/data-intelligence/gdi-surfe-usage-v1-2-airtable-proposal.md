# GDI Surfe Usage V1.2 — Airtable persistence proposal

**Status:** `SCHEMA_CREATED` via provider-agnostic ledger (2026-11-23) — Surfe sync writers still dry-run / disabled.  
**Local SoT:** `data/group-demand-intelligence/surfe-usage/` (V1.1 lock + atomic JSON + JSONL retained).  
**Canonical table:** see `docs/data-intelligence/gdi-contact-enrichment-airtable-schema-v1.md`.

## Reuse audit

Inspected AI Demand Positioning base (`appa2cE7FTRmIbB32`) inventory (`reports/gdi-airtable-complete-data-model-audit-v1`).

| Candidate | Verdict |
|-----------|---------|
| Group Demand Opportunities | Wrong grain (opportunities, not lookup events) |
| Decision Events | Semantic mismatch (validation/action/outcome, not Surfe billing) |
| Research Runs / Target Runs | Research coverage, not contact enrichment accounting |
| **GDI Contact Enrichment Usage Events** | **Created** — shared Findymail/Surfe/future ledger (`tblvUJlik63pETitY`) |
| GDI Surfe Usage Events | **Do not create** — superseded by provider-agnostic table |

## Created table (provider-agnostic)

- **Base:** AI Demand Positioning (`appa2cE7FTRmIbB32`)
- **Name:** `GDI Contact Enrichment Usage Events`
- **ID:** `tblvUJlik63pETitY`
- **Identity:** `Lookup Event ID` (idempotent upsert)
- **Env:** `GDI_CONTACT_ENRICHMENT_USAGE_AIRTABLE_TABLE_ID` (legacy alias `GDI_SURFE_USAGE_AIRTABLE_TABLE_ID` still accepted)
- **Enable:** `GDI_SURFE_USAGE_AIRTABLE_SYNC_ENABLE=1` (still off by default)
- **Apply:** `GDI_SURFE_USAGE_AIRTABLE_DRY_RUN=0` (default remains dry-run)

Core field map: `MAP_USAGE_EVENT` in `lib/group-demand-intelligence/contact-intelligence/gdi-contact-enrichment-airtable-schema-v1.js`.  
Extended Surfe-only columns in `PROPOSED_FIELD_MAP` remain optional future additions — do not invent a second table.

## Explicit non-fields (privacy)

Do **not** store: emails, phones, contact names, raw Surfe payloads, API keys, accepted field values.

## Historical migration

Do **not** blindly sync all 29 existing ledger rows — they classify as `FIXTURE_TEST`. Sync excludes fixtures by default.

## Classification

`PERSISTENCE_DESIGNED` until table exists and dry-run → apply retrieval is proven.
