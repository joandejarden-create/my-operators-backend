# Airtable compaction read-path proof

**Contract:** storage stubs must never masquerade as complete customer report content.

## Implementation

`lib/ai-demand-positioning/airtable-published-report.js`:

- `compactPayloadForAirtableOverlay` — stubs oversized nested keys with `{ storageOnly, payloadStoreRef, field, periodId, propertyId }`
- `resolveCompactedAirtablePayload` — rebuilds complete payload from PRIMARY_SOT
- `validateAdpPublishedStoreRef` — rejects traversal, cross-hotel, wrong period, invalid filenames
- `loadPublishedReportFromAirtable` — resolves stubs or returns **null** (filesystem fallback); never returns stubs
- `loadPublishedEvidenceFromAirtable` — same for evidence stubs

Airtable Live reads remain **off** by default (`ADP_AIRTABLE_READ_LIVE` unset).

## Tests (`scripts/test-adp-airtable-payload-compaction-v1.mjs`)

| Case | Result |
|------|--------|
| Complete payload within limit | PASS |
| Oversized compacted + reconstructed from PRIMARY_SOT | PASS |
| Missing reference | PASS (reject) |
| Wrong property | PASS (reject) |
| Wrong period | PASS (reject) |
| Invalid path (`..`) | PASS (reject) |
| Unavailable authoritative store | PASS (reject) |
| Idempotent compaction | PASS |
| No customer-visible stubs after resolve | PASS |

Successful Airtable upsert alone is **not** treated as read-compatibility proof — reconstruction tests are required.
