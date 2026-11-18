# Idempotency root cause and fix

## Root causes (stacked)

1. **No field comparison** — `upsertResearchInventoryRecord` always returned `WOULD_UPDATE` when a row existed.
2. **Slim opportunity projection** — `loadLocalOpportunities` previously stripped `segment` / `sources` / full payload, so rebuilds looked like material changes.
3. **Classifier injects in payload** — injecting classification helpers into the opportunity object before `researchPayloadJson` serialization churned the blob.

## Fix

- `diffMaterialResearchFields` / `fingerprintResearchFields` / `canonicalizeJson`
- Full source opportunity rows (identity overlays only)
- Payload serialize drops telemetry only; omits empty `demandThesis`
- Preserve `firstDiscoveredAt` / `firstDiscoveredRunId`
- Return `UNCHANGED` when no material diff

## Verified acceptance

| Scope | Result |
|-------|--------|
| Initial 85 | **85 UNCHANGED** |
| Full 659 after backfill | **659 UNCHANGED** |
