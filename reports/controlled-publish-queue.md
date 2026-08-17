# Controlled Publish Queue

Generated: 2026-07-06T15:57:34.793Z
Queue: **v2.1** (read-only)

## Executive summary

| Metric | Count |
|--------|------:|
| Total entities | 10 |
| Resolved | 8 |
| Unresolved | 2 |
| Entities with controlled candidates (audit) | 0 |
| Entities ready for controlled publish | 0 |
| Total ready items | 0 |
| Suggested-only items | 0 |
| Needs steward review | 2 |
| Already published / populated | 13 |
| Blocked items | 0 |

## Ready for controlled publish

_None in current filter._

## Suggested only / needs steward review

- **GHL Hoteles (GHL Holding)** — `op.platform.offeredServices` → `Offered Services` (Medium): select_option_validation_required
- **GHL Hoteles (GHL Holding)** — `op.brand.familiesOperated` → `Brand Families Operated` (Medium): select_option_validation_required

## Already published / destination populated

- **GHL Hoteles (GHL Holding)** — `op.snapshot.companyName` → `company_name` · live: GHL Hoteles (GHL Holding) · fact `rec9wfIL6Ym3jlEl6` — future changes require steward review
- **GHL Hoteles (GHL Holding)** — `op.snapshot.companyDescription` → `companyDescription` · live: GHL Hoteles is a Latin American hospitality operator that ma · fact `recNzLKr826ycKR7k` — future changes require steward review
- **GHL Hoteles (GHL Holding)** — `op.markets.regionsSupported` → `specificMarkets` · live: Colombia, Chile, Guatemala, Peru · fact `reccszsLnWjA5fPnp` — future changes require steward review
- **Hotel Equities (CALA)** — `op.snapshot.companyName` → `company_name` · live: Hotel Equities (CALA) · fact `rec4OkNp3HErir1Tm` — future changes require steward review
- **Hotel Equities (CALA)** — `op.snapshot.companyDescription` → `companyDescription` · live: Hotel Equities (CALA) is the Caribbean & Latin America divis · fact `rec5ZV7hxlyZz3eRk` — future changes require steward review
- **Hotel Equities (CALA)** — `op.snapshot.primaryServiceModel` → `primaryServiceModel` · live: Mixed · fact `recDasPN4e1SOJOUa` — future changes require steward review
- **Hotel Equities (CALA)** — `op.markets.regionsSupported` → `specificMarkets` · live: Caribbean; Latin America · fact `recQEsdNe6Z6yYl7R` — future changes require steward review
- **Hotel Equities (CALA)** — `op.snapshot.companyDescription` → `companyDescription` · live: Hotel Equities (CALA) is the Caribbean & Latin America divis · fact `recg9JSrZm9gmFKcN` — future changes require steward review
- **Arbor Lodging (CALA)** — `op.snapshot.companyName` → `company_name` · live: Arbor Lodging (CALA) · fact `recyLCMW5xem5TdzK` — future changes require steward review
- **Kimpton Hotels** — `be.positioning.tagline` → `Brand Tagline` · live: Luxury with a Wink · fact `rec2DboeBcrWqexw2` — future changes require steward review
- **Kimpton Hotels** — `be.identity.parentCompany` → `Parent Company` · live: IHG Hotels & Resorts · fact `rec3C43a3luyOqAdB` — future changes require steward review
- **Kimpton Hotels** — `be.positioning.summary` → `Brand Positioning` · live: Boutique lifestyle hotels with personality—design-forward, g · fact `rec4uii7RPJNocprM` — future changes require steward review
- **Curio Collection by Hilton** — `be.identity.parentCompany` → `Parent Company` · live: Hilton Worldwide · fact `recpaaTI64IIsm7hi` — future changes require steward review

## Blocked / no mapping

- **Aimbridge Hospitality (LATAM/CALA)** (`TBD`): Record ID TBD in tracker
- **Best Western Plus** (`rec5KPgalPPAFl7UZ`): no_approved_facts
- **Hilton Garden Inn** (`TBD`): Record ID TBD in tracker
- **Radisson Blu by Choice** (`recWPEvxBQxVVzSq3`): no_approved_facts
- **Viento Sur Gestión Hotelera** (`recZPHT2zqc8K6itx`): no_approved_facts

## Exact next commands (dry-run only)

_No controlled publish dry-runs recommended._

## Safety

- Read-only queue — no Airtable writes
- v2 allowlist: operator `specificMarkets` only
