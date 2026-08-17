# Approved Intelligence → Platform Field Publishing Audit

Generated: 2026-07-06T15:47:51.512Z
Audit: **v1** (read-only)
Entity: operator — **GHL Hoteles (GHL Holding)** (`reciI2tYQBfMoMK9G`)

## Governance snapshot

- Validation Status: Company Published
- External Display: Show Trust Label
- Company Validated: no
- Display allowed for field publish: **yes**

## Summary

| Metric | Count |
|--------|------:|
| Approved facts audited | 5 |
| Evidence only | 0 |
| Suggested field update | 5 |
| Controlled publish candidate | 0 |
| Blocked | 0 |
| Pending/rejected (excluded) | 2 |

## Mappings

| Field key | Approved value | Destination | Live value | Mode | Blockers |
|-----------|----------------|-------------|------------|------|----------|
| `op.snapshot.companyName` | GHL Hoteles | Operator Setup - Platform & Markets → `company_name` | GHL Hoteles (GHL Holding) | **suggested_field_update** | identity_field_populated_no_overwrite |
| `op.platform.offeredServices` | Events and celebrations (meetings and group events at GHL ho | Operator Setup - Governance, Delivery & Diligence → `Offered Services` | — | **suggested_field_update** | select_option_validation_required |
| `op.snapshot.companyDescription` | With presence in 35 hotels, 4 countries, 18 destinations, 3. | Operator Setup - Profile & Positioning → `companyDescription` | GHL Hoteles is a Latin American hospital | **suggested_field_update** | destination_field_populated |
| `op.markets.regionsSupported` | Colombia, Chile, Guatemala, Peru | Operator Setup - Platform & Markets → `specificMarkets` | Colombia, Chile, Guatemala, Peru | **suggested_field_update** | destination_field_populated |
| `op.brand.familiesOperated` | Geotel, GHL Collection, GHL Relax, GHL Style, Irotama Resort | Operator Setup - Profile & Positioning → `Brand Families Operated` | — | **suggested_field_update** | select_option_validation_required |

## By publish mode

### suggested_field_update

- **op.snapshot.companyName** (Company Name)
  - Proposed: GHL Hoteles
  - Blockers: identity_field_populated_no_overwrite
- **op.platform.offeredServices** (Offered Services)
  - Proposed: Events and celebrations (meetings and group events at GHL hotels)
  - Blockers: select_option_validation_required
- **op.snapshot.companyDescription** (Company Description)
  - Proposed: With presence in 35 hotels, 4 countries, 18 destinations, 3.433 rooms and 2.000 collaborators, GHL operates hotels acros
  - Blockers: destination_field_populated
- **op.markets.regionsSupported** (Regions Supported)
  - Proposed: Colombia, Chile, Guatemala, Peru
  - Blockers: destination_field_populated
- **op.brand.familiesOperated** (Brand Families Operated)
  - Proposed: Geotel, GHL Collection, GHL Relax, GHL Style, Irotama Resort, Latam Hotel Corporation, GHL
  - Blockers: select_option_validation_required

## Excluded facts (not approved)

- `op.platform.offeredServices` — Pending
- `op.snapshot.primaryServiceModel` — Pending

## Safety

- Read-only audit — no Airtable writes in v1
- No apply mode — controlled publish is design-only
- Never writes: Company Validated
- Never writes: Company Validation Date
- Never writes: Validation Status
- Never writes: Usage Permission
- Never writes: External Display Status
- Never writes: Confidence Level
- Never writes: Last Reviewed Date
- Never writes: Evidence Notes
- Never writes: Company Reviewed
- Never writes: submission_status
- Never writes: readyForInvestorPublication
- Never writes: Company Validated
- Never writes: Company Validation Date
- Never writes: scoring fields (BAS/OAS/OCS)
- Never writes: Deal Readiness outputs
