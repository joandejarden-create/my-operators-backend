# HE Guest Reputation Visual & Insight Quality (HE-QL-029 / V17)

**Date:** 2026-11-19  
**PR:** [#463](https://github.com/joandejarden-create/my-operators-backend/pull/463)  
**Lesson:** HE-QL-029 · Quality Standard v1.18 §9d  
**Surface:** `http://127.0.0.1:8091/hotel-explorer-preview.html` · Phase 3 Proposed

## Summary

Guest Reputation & Experience now matches Dealality Chain Scale / My Deals accents, shows a consistent **seven-platform** overview (populated + dimmed unavailable), and replaces dense insight boxes with a compact executive assessment. Methodology sits in a collapsed secondary section.

## Changes

1. **Canonical colors** — reputation bars, links, and platform selectors use `--accent--primary-1` (`#6c72ff`) with gold hover; removed `#38bdf8` from `hiw-rep-*`.
2. **Seven-platform overview** — `REPUTATION_OVERVIEW_PLATFORMS_V1` (Tripadvisor, Booking.com, Expedia, Google Reviews, Hotels.com, Agoda, Trivago). Available cards keep native rating / scale / count / date / link. Unavailable cards stay visible as **Not connected** / **Data unavailable** — never invent zeros.
3. **Compact executive insights** — Relative Strengths · Areas to Investigate · Recommended Management Follow-Up · Trend & Data Confidence.
4. **Methodology** — collapsed **Sources, Methodology & Limitations** below insights.
5. **Analytical distinctions preserved** — Bethesda Value for money 7.7/10 surfaces as relative investigate, not complaint proof; no fabricated trends.

## Validation

| Hotel | Coverage | Expected |
| --- | --- | --- |
| Bethesda Marriott `recLuxvwwxID7U2B8` | 3 populated + 4 dimmed | Seven cards; Booking categories; Value as investigate |
| Sparse Coverage Hotel (fixture build) | 1 populated + 6 dimmed | Overview length 7; no averages |
| KGPV `recUNycnMwOVFX0hc` (no packet) | 0 populated + 7 dimmed | Founder preview shows unavailable cards only |

## Gates

```bash
npm run test:he-guest-sentiment-brand-operator-v1
npm run test:he-product-quality-gates-v1
npm run test:he-loading-ux-toast-v1
```

## Screenshots

`reports/hotel-explorer/phase4-quality/phase4-founder-preview-screenshots/`

- `quality-v17-rep-seven-platforms.png` — Bethesda 3 populated + 4 Not connected
- `quality-v17-rep-insights.png` — compact executive four-card assessment
- `quality-v17-rep-methodology.png` — Sources, Methodology & Limitations expanded
- `quality-v17-rep-sparse.json` — sparse coverage model evidence (1 available / 6 unavailable)
- `quality-v17-rep-empty.png` — KGPV seven dimmed cards (Data unavailable / Not connected)

## Persistence / paid APIs

- No Airtable writes
- No paid review-provider calls (`X-Dealality-Paid-Providers: forbidden`)
- Classification remains **PROTOTYPE** for staged public-listing metadata
