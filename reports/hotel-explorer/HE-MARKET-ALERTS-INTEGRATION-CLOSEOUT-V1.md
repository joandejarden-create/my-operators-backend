# Market Alerts ↔ Hotel Explorer — Integration Closeout (v1)

**Date:** 2026-11-19  
**PR:** #463  
**Apply / schema create / deploy:** **NOT AUTHORIZED**

## 1. Canonical hotel relationships

| Mechanism | Status | Multi-hotel | Notes |
|---|---|---|---|
| Tags `hpc:<recId>` | **Production association path** | Yes (multiple tags) | Idempotent planner; apply gated by `MARKET_ALERTS_HOTEL_LINK_APPLY=1` |
| `HPC Hotel Record IDs` (multiline) | **Designed only** | Yes (one row → many ids) | Live probe: field **does not exist** (`Unknown field name`). Do not create without founder + MA owner auth |
| Duplicate article rows | **Forbidden** | — | One MarketAlerts record; associations are tags/field merges only |

Live probe: `HE-MARKET-ALERTS-CLOSEOUT-LIVE-PROBE-V1.json` — **0** existing `hpc:` tags in sample.

## 2. Sources & Evidence

- Same GET `/api/hotel-explorer/hotels/:hotelId/news-articles` returns `evidenceCitations`.
- Citations preserve: Market Alerts id, source URL, publisher, publication date, supported hotel fact, verification status.
- UI merges into Sources & Evidence without copying into HI Evidence tables.
- Module: `lib/hotel-intelligence/market-alerts-evidence-citations-v1.js`.

## 3. Automatic refresh / no write-on-read

- Persisted Tag (or future assoc-field) associations are discovered on the normal GET news path.
- Brand/Operator and Sources mounts are GET-only (`writeOnRead: false`, `X-Dealality-Write: forbidden`).
- No paid research triggered by tab open.

## 4. Bethesda discovery gap

- Live search for property-specific Bethesda Marriott Market Alerts: **0 rows**.
- Brand/Operator Hotel News subsection remains **hidden** (correct empty behavior).
- Do **not** manufacture articles.
- Future MA runs should consider: verified aliases (`Bethesda Marriott Hotel`, `Marriott Bethesda`), renovation announcements, ownership/sponsor transactions, management changes.

## 5. Founder validation matrix

| Case | Evidence |
|---|---|
| 1. Hotel with one verified article | Fixture + live Loews Nashville MA `recwzBkgumjUmvJnz` (GET by hotel name; HPC identity not yet in census) |
| 2. Hotel with multiple articles | Fixture Alpha Boutique (2+ cards) |
| 3. Article → multiple hotels | Fixture multi-hotel + `HPC Hotel Record IDs` / dual Tags design; same article id for hotel A and B |
| 4. No matching articles → hidden | Bethesda live + fixture empty |
| 5. Sources & Evidence citation | `evidenceCitations` + Sources mount; preserves MA id/URL/publisher/date/fact/verification |
| 6. Dedup + GET-only | Dedupe ID collapse; association idempotence; API has no `.create`/`.update` |

## 6. Quality

- Standard §9c updated (v1.12)
- Lesson **HE-QL-023**
- Gates: `npm run test:he-market-alerts-closeout-v1` · product quality gates

## Remaining authorization (STOP)

1. Create `HPC Hotel Record IDs` field (optional) — founder + MA owner.
2. Apply Tag / field associations with `MARKET_ALERTS_HOTEL_LINK_APPLY=1`.
3. HPC identity for Loews Nashville (and similar) before portfolio-scale association apply.
