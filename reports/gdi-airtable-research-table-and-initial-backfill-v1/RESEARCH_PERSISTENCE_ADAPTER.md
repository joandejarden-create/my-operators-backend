# Research persistence adapter

## Module

`lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js`

## Behaviors

1. Resolve `(hotelId, opportunityId)` → `compositeKey`
2. Deduplicate via Airtable filter on `compositeKey` (fail on duplicates)
3. Preserve research payload JSON (telemetry-only keys dropped; oversized → fail closed)
4. Create missing / update material fields
5. Preserve `firstDiscoveredAt` / `firstDiscoveredRunId` on update
6. Read-back verification after production write
7. Hotel isolation via `hotelId` filter
8. Never writes GDO or publication packages

## Authorization

Production apply requires:
- `authorizeProductionWrites=true`
- `exactWriteSetAuthorization=yotel-guadalajara-research-inventory-85-v1`
- Exact 85-record allowlist match
