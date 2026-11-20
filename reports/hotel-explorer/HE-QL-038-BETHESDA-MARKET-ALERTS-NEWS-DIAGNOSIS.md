# HE-QL-038 — Bethesda Hotel News & Articles diagnosis

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Date:** 2026-11-19  
**API:** `GET /api/hotel-explorer/hotels/recLuxvwwxID7U2B8/news-articles` → `visible: false`, `articles: []`

## Verdict

Property-specific Market Alerts coverage for Bethesda Marriott is **genuinely absent** from the live Market Alerts table — not a Founder Preview render bug and not an over-aggressive filter hiding valid rows.

## Evidence

| Probe | Result |
|-------|--------|
| Title/summary/Hotel·Project search `bethesda marriott` | 0 rows |
| Tag search `hpc:recLuxvwwxID7U2B8` | 0 rows |
| Title search `mullen` / `pooks hill` | 0 rows |
| Title/summary search `bethesda` | 1 row — Choice Hotels Q1 results — classified **UNRELATED** |
| Read model + UI | Correctly **hides** Hotel News subsection when empty |

## Implications

1. Keep the empty subsection **hidden** (already implemented).
2. Do **not** invent articles from Marriott corporate news.
3. Future population path: research candidates → dedupe against Market Alerts URL → associate `hpc:recLuxvwwxID7U2B8` → authorized apply → HE auto-shows.

## Research candidates (not applied this PR)

Known Bethesda ownership / financing / transaction references from HE ownership intelligence should be checked against Market Alerts before any CREATE. No production Airtable writes in HE-QL-038.

## Classification

`NEWS_ABSENT_IN_MARKET_ALERTS` — read path healthy; persistence backlog for property-specific articles.
