# Decision & Outcome — filesystem → Airtable migration

**Mode:** apply
**Generated:** 2026-09-17T13:14:14.449Z
**Base:** `appvtnDurnMSjINP6`
**FS root:** `C:\Dev\deal-capture-proxy\data\decision-outcomes`

## Counts

| Metric | Value |
| --- | ---: |
| Hotels scanned | 3 |
| Decisions migrated | 20 |
| Events migrated | 96 |
| Skipped | 0 |
| Conflicts | 0 |
| Duplicates (eventId already exists) | 6 |
| Failures | 0 |

## Zero-invention confirmation

- **Confirmed:** migration preserves `decisionId` / event ids from disk and does **not** invent `organizationId`, revenue, financial values, timestamps, or other absent fields.

## Policy

- Default is dry-run; pass `--apply` to write.
- Decisions upserted by `decisionId` via `saveDecisionRecord`.
- Events appended idempotently by `eventId` via `appendEvent`.
- Gentle rate-limit delay between writes.

