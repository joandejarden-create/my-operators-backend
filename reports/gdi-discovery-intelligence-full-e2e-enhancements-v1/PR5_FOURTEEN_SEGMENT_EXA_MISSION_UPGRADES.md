# PR5 — Fourteen-Segment Exa Mission Upgrades V1

## Objective

Sharpen Exa discovery context for all fourteen GDI segments with hotel-dependent catchment and segment-true missions. No paid provider calls.

## Modules

| Path | Role |
|---|---|
| `lib/.../exa-agent/gdi-fourteen-segment-exa-mission-upgrades-v1.js` | Catchment resolver + 14 segment missions + upgraded context builder |
| `lib/.../exa-agent/exa-live-discovery-provider-bridge-v1.js` | Wires upgraded context into `hotelContextBlock` / readiness / live provider |
| `lib/.../workflow/full-e2e-fourteen-segment-discovery-execution-v1.js` | Passes `researchTrack` into Exa live provider |
| `scripts/test-gdi-fourteen-segment-exa-mission-upgrades-v1.mjs` | Deterministic regression |

## What changed

1. **Hotel-specific catchment** — Bethesda/DMV, Geneva, London, and generic fallbacks. Bethesda negative filters (National Mall / distant US) are **not** applied to international hotels.
2. **Segment-true missions** — each of 14 segments has distinct targets, early signals, exclusions, and contact types.
3. **Local-track attachment** — optional `researchTrack` appends track context into the same Exa context block.
4. **Legacy overlays preserved** — planner / catering / preferred-lodging overlays + LATAM language note remain additive.

## Explicit non-claims

- Does **not** prove live Exa yield improvement (requires founder-authorized paid validation).
- Does **not** change CURRENT publication packages or customer drawers.
- Does **not** perform Airtable writes.

## Tests

```bash
npm run test:gdi-fourteen-segment-exa-mission-upgrades-v1
```

## Authorization gates remaining

- Paid Exa live re-validation of Bethesda (and international fixture) mission quality.
- Airtable activation (separate plan).
