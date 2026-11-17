# Canonical Execution Contract — P1-C

## Entry command

```bash
npm run gdi:canonical-full-e2e -- --hotel <AirtableHotelId> --mode <mode>
```

Modes: `preflight` | `fixture` | `dry-run` | `live` (live requires paid auth flags).

## Hotel configuration

Resolved via existing `resolveHotelGdiProfileForResearch`. Missing hotel ID fails closed.

## Run context

One `runId` per hotel execution under `execution-observability/{hotelId}/`. Propagates through discovery + deep research.

## Execution modes

| Mode | Paid | Research | Publish |
|------|------|----------|---------|
| preflight | No | No | No |
| fixture | No | Fixture/historical | No |
| dry-run | Blocked | Fixture/historical | No |
| live | Requires authorize flags | Authorized providers | No (separate gate) |

## Segment discovery

`executeFourteenSegmentDiscoveryMatrix` with `executionLifecycle: external` when parent-managed.

## Deep research

`executeOriginalFullE2eResearchChain` + `recordDeepResearchOutcome` into the same ledger.

## Provider governance

Apify default: `APIFY_NOT_ORCHESTRATED`. Exa live only with authorize flags. Fixture ≠ live.

## Persistence

Durable observability JSON + chain outDir summaries. Does not mutate CURRENT packages.

## Certification vs publication

Operational certificate may be COMPLETE for research stages. `publicationAuthorized` is always false here. Publication remains founder-gated via research-to-page.
