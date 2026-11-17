# Test Results — P1-A/B

## Commands

```bash
npm run test:gdi-full-e2e-execution-integrity-p1ab-v1
npm run test:gdi-full-e2e-execution-observability-v1
npm run dealality:ci-precheck
```

## Results

| Suite | Result |
|-------|--------|
| `test:gdi-full-e2e-execution-integrity-p1ab-v1` | **18/18 PASS** |
| `test:gdi-full-e2e-execution-observability-v1` | **17/17 PASS** |

## Fixtures / coverage

- Synthetic hotel IDs under temp `executionObservabilityRoot` (no mutation of YOTEL/Bethesda/Radisson hist ledgers).
- Fixture discovery matrix for all 14 segments.
- Apify mock orchestration (no live Actor).
- Historical reconciliation UNKNOWN preservation.
- CURRENT publication pointer read-only sanity.

## Not run (by design)

- Live Exa discovery
- Live Apify Actors
- Parallel / SerpAPI / Surfe
- Airtable writes
- Publication / Railway deploy
- Browser admin monitor visual QA (API/shell wiring covered by observability suite)

## No-paid-call confirmation

Test summary emits:

```json
{
  "paidExa": false,
  "paidApify": false,
  "paidParallel": false,
  "surfe": false,
  "airtableWrites": false,
  "publicationChanges": false
}
```
