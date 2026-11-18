# Test results

## Pilot acceptance

```json
{
  "all14Accounted": true,
  "all14ExecutorInvoked": true,
  "paidFreshRetrievalBlocked": true,
  "publicNoCostDiscoveryAttempted": true,
  "bethesda86AirtableIntact": true,
  "dryRunNoProductionWrites": true,
  "gdoWrites": 0,
  "publicationWrites": 0,
  "rockCreekWrites": 0,
  "fabricatedResults": false,
  "writeManifestPreparedNotApplied": true
}
```

## Commands run

| Command | Result |
|---------|--------|
| `node scripts/gdi-bethesda-live-14-segment-persistence-pilot-v1.mjs` | PASS (`ok: true`, plannedWrites=2, productionWrites=0) |
| `npm run test:gdi-bethesda-live-14-segment-persistence-pilot-v1` | PASS (14 required artifacts) |
| `npm run test:gdi-full-e2e-research-persistence-bridge-v1` | PASS |
| `npm run test:gdi-bethesda-automated-research-persistence-pilot-v1` | PASS |
| `npm run dealality:ci-precheck` | PASS (gitleaks + workflow concurrency) |

## Distinction

Unit/pilot tests passing prove execution health and write-control integrity. They do **not** claim strong paid discovery yield (Exa blocked) or sales-ready commercial quality (0 sales-ready).
