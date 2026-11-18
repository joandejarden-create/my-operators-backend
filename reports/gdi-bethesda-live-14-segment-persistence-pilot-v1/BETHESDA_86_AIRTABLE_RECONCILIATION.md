# Bethesda 86 Airtable reconciliation

| Metric | Value |
|--------|------:|
| Filesystem opportunities | 86 |
| Live Airtable rows | 86 |
| Unique composite keys | 86 |
| Duplicate keys | 0 |
| Missing payloads | 0 |
| Missing in Airtable vs FS | 0 |
| Extra in Airtable vs FS | 0 |
| Rows with `firstDiscoveredAt` | 59 |
| Intact identity set | **true** |

Base `appa2cE7FTRmIbB32` · Table `tblbkDj0vhQLfrssV` · Hotel `recLuxvwwxID7U2B8`

## Identity integrity

- No missing `(hotelId, opportunityId)` identities vs the Bethesda 86 filesystem bag.
- No duplicate composite keys.
- Full `researchPayloadJson` present on all 86 live rows.
- Provenance: 59/86 currently carry `firstDiscoveredAt` (pre-backfill rows may lack the typed column while payload still holds history).

## Full-bag dry-run vs live Airtable (READ compare only)

```json
{
  "candidatesBeforeDedupe": 86,
  "candidates": 86,
  "DUPLICATES_RESOLVED": 0,
  "WOULD_CREATE": 0,
  "WOULD_UPDATE": 63,
  "UNCHANGED": 23,
  "CREATED": 0,
  "UPDATED": 0,
  "FAILED": 0,
  "SKIPPED_FORBIDDEN": 0,
  "SKIPPED_BUDGET": 0
}
```

### Important distinction

The **63 WOULD_UPDATE** count is filesystem bag drift vs live Airtable when the entire 86-row bag is compared. It is **not** authorization to mass-update Airtable.

This pilot’s founder-bounded write plan remains at most:

- **1 CREATE** — Pinnacle Lacrosse Championships 2027
- **1 UPDATE** — PDA TRI December 2026 evidence refresh

See `EXACT_PRODUCTION_PILOT_WRITE_MANIFEST.json`. Production mutations remain **blocked**.
