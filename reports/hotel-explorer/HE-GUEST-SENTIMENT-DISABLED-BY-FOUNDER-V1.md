# HE Guest Sentiment — DISABLED_BY_FOUNDER

**Date:** 2026-11-27  
**Flag:** `HE_GUEST_SENTIMENT_ENABLED=false` (default)  
**Certification:** `DISABLED_BY_FOUNDER`  
**FULL_HE_E2E_VALIDATED:** `NO` (not auto-promoted)

## Customer surface

- Guest Sentiment hidden from Hotel Explorer / Opportunity Radar / preview / golden-demo tab navigation.
- Active customer tabs: **11** (registry still holds 12 including preserved `sentiment`).
- Deep links (`sentiment`, `brand`, `guest-sentiment`) fall back to a safe tab / unavailable panel.
- API `GET /api/hotel-explorer/hotels/:hotelId/guest-sentiment` returns empty, rights-safe `DISABLED_BY_FOUNDER` payload (no platform ratings).

## Preserved

- Seven-platform UI code, staged fixtures, rights model, HI Hotel Reputation Observations schema, dry-run scripts, quality lessons.

## Reactivation (founder / admin)

1. Set `HE_GUEST_SENTIMENT_ENABLED=true`.
2. Set client boot `window.__DEALALITY_HE_GUEST_SENTIMENT_ENABLED__ = true`.
3. Publication-rights gates in `guest-sentiment-model-v1` remain authoritative — the flag never overrides them.
4. No paid scrape / Apify / scheduled collection is authorized by the flag alone.

## Tests

- `npm run test:he-guest-sentiment-disabled-by-founder-v1`
- `npm run test:radar-canonical-12-tab-explorer-parity-v1`
- `npm run test:he-guest-sentiment-brand-operator-v1`
- `npm run test:he-product-quality-gates-v1`
