# GDI Performance V1 — Network Waterfall

## Share initial load (before)

```
HTML ──────────────────────────────────────── (~130ms)
  resolve ────────────────── (~280ms)
    opportunities list ────────────────────────────── (~5150ms)  ← dominant
      render list (client)
```

Duplicate: none on first open (single resolve + single list).  
Required for first render: resolve + opportunities.

## Share View Details (before)

```
click View Details
  [no visual change]
  GET .../opportunities/:id ──────────────────────── (~4900ms)  ← full hotel reload
  drawer open + render
```

## Share initial load (after)

```
HTML ────────────────────────────────────────
  resolve ──────────┐ parallel when hotelId peeked from token
  opportunities ────┘ (first ~5s cold; ~0.3s warm via TTL cache)
  render list
```

## Share View Details (after)

```
click → drawer shell + “Loading details…” (<100ms)
  GET detail ── warm: cache hit ~170ms | cold miss: single-row or full doc
  hydrate drawer
```

## Payload notes

| Response | Shape | Notes |
| --- | --- | --- |
| List | `gdi_opportunity_list_v2` DTO | ~2KB/row; no evidence arrays |
| Detail | full opportunity (~11KB) | evidence/sources included |
| List total | ~62KB / 30 rows | acceptable; not primary bottleneck |

## Serial chains found

| Chain | Fix |
| --- | --- |
| resolve → opportunities | Parallel peek + Promise.all |
| detail → full listOpportunities | Cache + single-record path |
| progression forEach loadDecision | Parallel + TTL cache |
