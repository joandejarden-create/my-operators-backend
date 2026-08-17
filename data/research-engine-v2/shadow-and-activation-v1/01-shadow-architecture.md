# Shadow Monitoring Architecture (read-only)

## Objective

Answer: **What changed since Dealality last verified this record?**

No Airtable writes. No automatic apply.

## Initial cohort

- Hotel Indigo — Mexico
- Kimpton — Mexico

Reusable via brand filter + directory adapter routing (V1.1 `checkHotelFreshness`).

## Flow

1. Snapshot cohort hotels (local census extract)
2. Run contradiction-first freshness (Exact/High gates)
3. Opening corroboration for Medium Pipeline→Open
4. Directory gap + stale-candidate scans
5. Identity enrichment proposals (review only)
6. Deduplicate alerts via local `shadow-state.json` (30-day window)
7. Emit human digest

## Dedup state

Local only: claim fingerprint, first/last detected, evidence URL/date, review state.
**Not** a source of truth.
