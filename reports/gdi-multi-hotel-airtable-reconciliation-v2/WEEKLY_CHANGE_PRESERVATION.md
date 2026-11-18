# Weekly Change preservation

**Contract:** `gdi-weekly-change-durability-v1` (referenced in manifest `weeklyChangeContract`)

## Rules enforced in planner

Synchronization must **not**:

- Reset `firstDiscoveredAt` / `firstDiscoveredRunId` when an Airtable baseline already exists
- Treat Airtable migration or title edits as a new commercial discovery
- Invent NEW Weekly Change events solely because filesystem narrative differs on DEFERRED or UNCHANGED rows

On **SHARED** rows with only non-material drift, decision is **UNCHANGED** with durable Weekly Change annotations carried forward (`buildDurableWeeklyChangeAnnotation`).

## Idempotent rerun certification

From `MULTI_HOTEL_DRY_RUN_SYNC_MANIFEST.json`:

```json
"idempotencyRecheck": {
  "ok": true,
  "identical": true
}
```

Re-planning the same hotel inputs produces identical decision counts and stable Weekly Change payloads.

## Test coverage

- Assertion 14 in `test-gdi-multi-hotel-airtable-reconciliation-v2.mjs`: shared row with title drift stays UNCHANGED; Weekly Change present
- Assertions 11–12: planner-level idempotent rerun via `certifyWeeklyChangeStabilityAcrossIdenticalReruns`

## Founder implication

Authorized future sync may update eligible commercial fields but must preserve discovery lineage already stored on Airtable GDOs.