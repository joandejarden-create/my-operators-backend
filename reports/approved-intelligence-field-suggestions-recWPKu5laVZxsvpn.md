# Approved Intelligence Field Suggestions

Generated: 2026-07-06T15:06:42.326Z
Suggestions: **v1** (read-only — Mode B)
Entity: operator — **Hotel Equities (CALA)** (`recWPKu5laVZxsvpn`)

## Executive summary

| Metric | Count |
|--------|------:|
| Total suggestions | 5 |
| Controlled publish candidates | 0 |
| Suggested-only updates | 5 |
| Excluded mappings | 0 |
| Excluded facts (not approved) | 4 |
| Risk Low / Medium / High | 0 / 4 / 1 |

**Next action:** Review suggested-only updates; no blank-field auto-publish candidates.

## Proposed suggestions

| Field | Destination | Live → Proposed | Risk | Status |
|-------|-------------|------------------|------|--------|
| `op.snapshot.companyName` | company_name | Hotel Equities (CALA) → Hotel Equities | **High** | Needs Review |
| `op.snapshot.companyDescription` | companyDescription | Hotel Equities (CALA) is the C → Hotel Equities took a step into the Cari | **Medium** | Needs Review |
| `op.snapshot.primaryServiceModel` | primaryServiceModel | Mixed → Hotel Management | **Medium** | Needs Review |
| `op.markets.regionsSupported` | specificMarkets | Caribbean; Latin America → Caribbean, Latin America, United States, | **Medium** | Needs Review |
| `op.snapshot.companyDescription` | companyDescription | Hotel Equities (CALA) is the C → Choose the third party management team a | **Medium** | Needs Review |

## Suggested-only updates

- **op.snapshot.companyName** → `company_name` (High)
  - Live: Hotel Equities (CALA)
  - Proposed: Hotel Equities
- **op.snapshot.companyDescription** → `companyDescription` (Medium)
  - Live: Hotel Equities (CALA) is the Caribbean & Latin America division of Hotel Equitie
  - Proposed: Hotel Equities took a step into the Caribbean and Latin America (CALA) region, r
- **op.snapshot.primaryServiceModel** → `primaryServiceModel` (Medium)
  - Live: Mixed
  - Proposed: Hotel Management
- **op.markets.regionsSupported** → `specificMarkets` (Medium)
  - Live: Caribbean; Latin America
  - Proposed: Caribbean, Latin America, United States, Canada
- **op.snapshot.companyDescription** → `companyDescription` (Medium)
  - Live: Hotel Equities (CALA) is the Caribbean & Latin America division of Hotel Equitie
  - Proposed: Choose the third party management team at Hotel Equities when you want to expand

## Excluded facts (not approved)

- `op.snapshot.companyDescription` — Pending
- `op.snapshot.primaryServiceModel` — Pending
- `op.brand.familiesOperated` — Pending
- `op.snapshot.companyName` — Pending

## Risks

- **op.snapshot.companyName** (High): identity_field_risk — Do not auto-publish; steward must explicitly approve any overwrite.
- **op.snapshot.companyDescription** (Medium): destination_populated — Compare proposed vs live value; approve only if proposed is clearly better.
- **op.snapshot.primaryServiceModel** (Medium): destination_populated — Compare proposed vs live value; approve only if proposed is clearly better.
- **op.markets.regionsSupported** (Medium): destination_populated — Compare proposed vs live value; approve only if proposed is clearly better.
- **op.snapshot.companyDescription** (Medium): destination_populated — Compare proposed vs live value; approve only if proposed is clearly better.

## Safety

- Report-only v1 — no platform field writes
- No governance or Company Validated writes
- Future controlled publish writes only **Approved For Publish** suggestions
