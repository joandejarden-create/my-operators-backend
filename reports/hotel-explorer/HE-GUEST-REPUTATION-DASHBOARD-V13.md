# HE Guest Reputation & Experience Dashboard — Founder Preview v13

**Date:** 2026-11-19  
**PR:** #463  
**Lesson:** HE-QL-025 · Quality Standard v1.14 §9d  
**Airtable / paid / deploy:** none

## Verdict

Guest Sentiment inside Brand / Operator is now a reusable **Guest Reputation & Experience** dashboard: platform scorecards, Category Performance bars, structured executive insights, restrained trends status, and collapsed Sources & Methodology. Independent wave loading is preserved.

## Validation

| Hotel | Result |
|---|---|
| Bethesda Marriott (`recLuxvwwxID7U2B8`) | PASS — 3 scorecards (4.1/5, 8.0/10, 8.6/10); Booking Category Performance bars; 4 insight cards; trends restrained; methodology collapsed |
| Sparse coverage (unit fixture) | PASS — single Tripadvisor platform + categories; no averages |
| KGPV (`recUNycnMwOVFX0hc`) | PASS — sentiment host hidden (no authorized packet) |

## Evidence

`reports/hotel-explorer/phase4-quality/phase4-founder-preview-screenshots/`

- `quality-v13-rep-scorecards.png`
- `quality-v13-rep-category.png`
- `quality-v13-rep-insights.png`
- `quality-v13-rep-methodology.png`
- `quality-v13-rep-empty-kgpv.png`

## Gates

```bash
npm run test:he-guest-sentiment-brand-operator-v1
npm run test:he-product-quality-gates-v1
```

## Persistence

`PROTOTYPE` UI/presentation over staged public-listing metadata — no Airtable apply.
