# Approved Intelligence → Platform Field Publishing Audit

Generated: 2026-07-06T15:06:38.670Z
Audit: **v1** (read-only)
Entity: operator — **Hotel Equities (CALA)** (`recWPKu5laVZxsvpn`)

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
| Pending/rejected (excluded) | 4 |

## Mappings

| Field key | Approved value | Destination | Live value | Mode | Blockers |
|-----------|----------------|-------------|------------|------|----------|
| `op.snapshot.companyName` | Hotel Equities | Operator Setup - Platform & Markets → `company_name` | Hotel Equities (CALA) | **suggested_field_update** | identity_field_populated_no_overwrite |
| `op.snapshot.companyDescription` | Hotel Equities took a step into the Caribbean and Latin Amer | Operator Setup - Profile & Positioning → `companyDescription` | Hotel Equities (CALA) is the Caribbean & | **suggested_field_update** | destination_field_populated |
| `op.snapshot.primaryServiceModel` | Hotel Management | Operator Setup - Profile & Positioning → `primaryServiceModel` | Mixed | **suggested_field_update** | destination_field_populated |
| `op.markets.regionsSupported` | Caribbean, Latin America, United States, Canada | Operator Setup - Platform & Markets → `specificMarkets` | Caribbean; Latin America | **suggested_field_update** | destination_field_populated |
| `op.snapshot.companyDescription` | Choose the third party management team at Hotel Equities whe | Operator Setup - Profile & Positioning → `companyDescription` | Hotel Equities (CALA) is the Caribbean & | **suggested_field_update** | destination_field_populated |

## By publish mode

### suggested_field_update

- **op.snapshot.companyName** (Company Name)
  - Proposed: Hotel Equities
  - Blockers: identity_field_populated_no_overwrite
- **op.snapshot.companyDescription** (Company Description)
  - Proposed: Hotel Equities took a step into the Caribbean and Latin America (CALA) region, recognizing the immense growth potential 
  - Blockers: destination_field_populated
- **op.snapshot.primaryServiceModel** (Primary Service Model)
  - Proposed: Hotel Management
  - Blockers: destination_field_populated
- **op.markets.regionsSupported** (Regions Supported)
  - Proposed: Caribbean, Latin America, United States, Canada
  - Blockers: destination_field_populated
- **op.snapshot.companyDescription** (Company Description)
  - Proposed: Choose the third party management team at Hotel Equities when you want to expand your hotel's company brand like never b
  - Blockers: destination_field_populated

## Excluded facts (not approved)

- `op.snapshot.companyDescription` — Pending
- `op.snapshot.primaryServiceModel` — Pending
- `op.brand.familiesOperated` — Pending
- `op.snapshot.companyName` — Pending

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
