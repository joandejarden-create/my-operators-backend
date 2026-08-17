# Research Engine V2 — Contradiction-First Status/Affiliation Checker (V1)

## Goal

Prove Dealality can natively rediscover Webhound Test 6–class freshness/affiliation findings
using contradiction-first research — **without** Webhound credits or Airtable writes.

## Reused infrastructure

- Hilton census status audit pattern (`lib/hotel-census/audit-hilton-census-status.js`)
- IHG directory extract + hoteldetail name/brand parsers (`lib/ihg-brand-directory-extract.js`)
- Marriott URL/MARSHA helpers (`lib/marriott-brand-directory-extract.js`)
- Choice sitemap match loader (`lib/hotel-census/plan-choice-census-sitemap-match.js`)
- Census field constants (`lib/hotel-census/fields.js`)
- Local census snapshot: `reports/census-amenities-blank-rows.csv`
- Local directories: `reports/ihg-cala-directory-extract.json`, `reports/cala-tribute-property-visual-discovery.json`

## New modules

| Module | Role |
|--------|------|
| `lib/research-engine-v2/claim-model.js` | Claims + proposed corrections |
| `lib/research-engine-v2/source-hierarchy.js` | Claim-specific source priority + temporal resolve |
| `lib/research-engine-v2/query-generator.js` | Support + disproof queries (generic) |
| `lib/research-engine-v2/brand-family.js` | Adapter routing |
| `lib/research-engine-v2/adapters/*` | IHG / Marriott / Choice / generic |
| `lib/research-engine-v2/check-hotel-freshness.js` | `checkHotelFreshness()` orchestrator |
| `lib/research-engine-v2/cross-table-checks.js` | Light Census ↔ BE integrity |

## Blind benchmark process

1. Snapshot Dealality values → `01-input-snapshot.json`
2. Run native checker → freeze `02-native-results.*`
3. Only then `--compare-webhound` → `03-webhound-reconciliation.json`

## Non-goals (V1)

Scheduler, Airtable writer, full temporal DB, every hotel group, UI, owner/gov discovery, Webhound integration.
