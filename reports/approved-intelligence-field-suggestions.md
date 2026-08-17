# Approved Intelligence Field Suggestions

Generated: 2026-07-06T15:47:55.288Z
Suggestions: **v1** (read-only — Mode B)
Entity: operator — **GHL Hoteles (GHL Holding)** (`reciI2tYQBfMoMK9G`)

## Executive summary

| Metric | Count |
|--------|------:|
| Total suggestions | 5 |
| Controlled publish candidates | 0 |
| Suggested-only updates | 5 |
| Excluded mappings | 0 |
| Excluded facts (not approved) | 2 |
| Risk Low / Medium / High | 0 / 4 / 1 |

**Next action:** Review suggested-only updates; no blank-field auto-publish candidates.

## Proposed suggestions

| Field | Destination | Live → Proposed | Risk | Status |
|-------|-------------|------------------|------|--------|
| `op.snapshot.companyName` | company_name | GHL Hoteles (GHL Holding) → GHL Hoteles | **High** | Needs Review |
| `op.platform.offeredServices` | Offered Services | — → Events and celebrations (meetings and gr | **Medium** | Needs Review |
| `op.snapshot.companyDescription` | companyDescription | GHL Hoteles is a Latin America → With presence in 35 hotels, 4 countries, | **Medium** | Needs Review |
| `op.markets.regionsSupported` | specificMarkets | Colombia, Chile, Guatemala, Pe → Colombia, Chile, Guatemala, Peru | **Medium** | Needs Review |
| `op.brand.familiesOperated` | Brand Families Operated | — → Geotel, GHL Collection, GHL Relax, GHL S | **Medium** | Needs Review |

## Suggested-only updates

- **op.snapshot.companyName** → `company_name` (High)
  - Live: GHL Hoteles (GHL Holding)
  - Proposed: GHL Hoteles
- **op.platform.offeredServices** → `Offered Services` (Medium)
  - Live: —
  - Proposed: Events and celebrations (meetings and group events at GHL hotels)
- **op.snapshot.companyDescription** → `companyDescription` (Medium)
  - Live: GHL Hoteles is a Latin American hospitality operator that manages a multi-brand 
  - Proposed: With presence in 35 hotels, 4 countries, 18 destinations, 3.433 rooms and 2.000 
- **op.markets.regionsSupported** → `specificMarkets` (Medium)
  - Live: Colombia, Chile, Guatemala, Peru
  - Proposed: Colombia, Chile, Guatemala, Peru
- **op.brand.familiesOperated** → `Brand Families Operated` (Medium)
  - Live: —
  - Proposed: Geotel, GHL Collection, GHL Relax, GHL Style, Irotama Resort, Latam Hotel Corpor

## Excluded facts (not approved)

- `op.platform.offeredServices` — Pending
- `op.snapshot.primaryServiceModel` — Pending

## Risks

- **op.snapshot.companyName** (High): identity_field_risk — Do not auto-publish; steward must explicitly approve any overwrite.
- **op.platform.offeredServices** (Medium): select_option_validation — Validate select options against Airtable allowed values before publish.
- **op.snapshot.companyDescription** (Medium): destination_populated — Compare proposed vs live value; approve only if proposed is clearly better.
- **op.markets.regionsSupported** (Medium): destination_populated — Compare proposed vs live value; approve only if proposed is clearly better.
- **op.brand.familiesOperated** (Medium): select_option_validation — Validate select options against Airtable allowed values before publish.

## Safety

- Report-only v1 — no platform field writes
- No governance or Company Validated writes
- Future controlled publish writes only **Approved For Publish** suggestions
