# GDI One-Click Publication Architecture V1

## Canonical path

Local GDI opportunity list (display-eligible)
→ `POST /api/group-demand-intelligence/hotels/:hotelId/publish`
→ `publishHotelInventoryOneClick`
→ `publishHotelInventoryBatch` (single hotel lock)
→ immutable versioned snapshots + `currentPublication` pointer
→ verify (manifest parity) or roll pointer back

## Authoritative customer content

Published snapshots include full approved fields and `publishedPresentationLocked: true`.

Share and authenticated customer reads:

- merge published registry snapshots
- **skip** `applyLiveCommercialQuality` when locked
- render via shared presentation components

## What stays out of band

- Presentation-only CSS/layout: app deploy only
- Research-excluded inventory: retained internally, never a second list
- Other hotels: untouched (hotel-scoped lock + no cross-hotel writes)
- Share tokens: never rotated by publish

## Production Spice repair (founder GO)

1. Merge PR #136 **or** pin Railway to this revision.
2. Deploy app (registry + one-click + locked presentation).
3. Run one-click apply for Spice with the seven IDs (dry-run first).
4. Confirm share API returns seven full-content opportunities (1 Sales Ready / 6 AD; no Aventa).
5. Rollback = restore prior `currentPublication` pointer (versioned snaps remain).

## Scripts

- `npm run test:gdi-one-click-hotel-publish-v1`
- `npm run gdi:one-click-publish-spice-dry-run`
