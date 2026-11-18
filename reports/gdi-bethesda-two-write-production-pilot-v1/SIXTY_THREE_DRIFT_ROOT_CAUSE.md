# 63 WOULD_UPDATE root cause

## Prior pilot observation

Full FS bag dry-run reported **63 WOULD_UPDATE** / **23 UNCHANGED**.

## Root cause

Bridge dry-runs injected `firstDiscoveredRunId: <currentRunId>` into `researchPayloadJson` when the opportunity lacked that key. Typed Airtable provenance columns are the SoT for first discovery; most live payloads did **not** contain `firstDiscoveredRunId`. Material compare then treated the injected stamp as a content change.

| Signal | Count (prior) |
|--------|--------------:|
| researchPayloadJson | ~61 |
| sourceSummary | ~2 |

Classification: **operational provenance stamp false positive** — not 61 genuinely material research improvements, and not authorization to overwrite Airtable from FS.

## Correction applied

`PAYLOAD_VOLATILE_KEYS` now excludes `firstDiscoveredRunId`, `firstDiscoveredAt`, and `firstSeenRunId` from payload material compare (in addition to `lastResearchedAt` / `runId` / etc.).

## Post-correction bag dry-run

```json
{
  "candidatesBeforeDedupe": 86,
  "candidates": 86,
  "DUPLICATES_RESOLVED": 0,
  "WOULD_CREATE": 0,
  "WOULD_UPDATE": 3,
  "UNCHANGED": 83,
  "CREATED": 0,
  "UPDATED": 0,
  "FAILED": 0,
  "SKIPPED_FORBIDDEN": 0,
  "SKIPPED_BUDGET": 0
}
```

Would-update remaining: **3**

Field hits: {"sourceSummary":3,"lastMaterialChangeAt":1,"researchPayloadJson":1}

**These residual updates remain UNAPPLIED.** Do not replace newer Airtable information with older filesystem snapshots.
