# Phase 1.1 — Radar Hotel Explorer validation

## Automated gates — PASS

```bash
npm run test:adp-phase1-radar-he-gates
```

| Gate | Result |
|------|--------|
| `test:radar-host-hotel-explorer-wiring-v1` | PASS — ranked-list host loads HE; Bethesda share pack absent |
| `test:radar-hotel-explorer-progressive-enrichment-v1` | PASS — RADAR_BASELINE / ENRICHED / FULL; Bethesda internal eligible; verified precedence; customer strip |
| `assert:hotel-explorer-share-assets` | PASS |
| `test:adp-published-store-durability-v1` | PASS |
| `test:ai-demand-positioning-published-foundation` | PASS |

Progressive-enrichment architecture preserved. No new share packs / customer visibility introduced.

## Browser internal-preview smoke — BLOCKED_ENV

| Check | Result |
|-------|--------|
| Server on `PORT=8091` | PASS (`http://127.0.0.1:8091`) |
| Radar page HTTP 200 | PASS |
| HE assets load | PASS |
| Open Bethesda `recLuxvwwxID7U2B8` via `hotelExplorer` + `internalPreview=1` | **BLOCKED** — brand-presence API 500 (no `.env` / Airtable; census map snapshot build failed) |
| Five Radar sections click-through | **BLOCKED** (HE panel did not open without census) |
| Baseline-only second hotel | **BLOCKED** (map showed 0 hotels) |

**Bethesda remains internal-preview eligible only** (automated gate asserts share pack absent).

Artifacts: `/opt/cursor/artifacts/radar-he-*.png`, `radar-hotel-explorer-smoke-test-report.md`

## Identity / boundaries (from automated tests)

- Census `rec…` identity required; name alone insufficient
- Verified attributes may overlay display; census keeps identity
- Customer/share projection strips internal-only and unverified
