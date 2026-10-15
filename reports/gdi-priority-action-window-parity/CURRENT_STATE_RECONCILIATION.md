# Current-state reconciliation (five hotels)

**Snapshot:** `five-hotel-facing-snapshot.json` + live Airtable read via `listOpportunitiesForHotel` + `filterCustomerFacingOpportunities`  
**As of:** 2026-10-15 (simulation run)

## Verdict in one line

**Priority / Exploratory** (publication section) and **Priority High/Medium/Watch** (fit band) are **intentionally different fields**, but San Diego/Mayflower **Watch/Watch counters are not intentional research decisions** — they are **import default stamps** on empty `priority` / `bookingWindowStatus`.

## Facing filter counts (raw → API → UI)

| Hotel | Raw Airtable | Customer-facing | Priority filter (H/M/W) | Action Window (Pursue/Contact/Watch/TooEarly) | Packaging (Core/Exploratory/missing) |
|-------|--------------|-----------------|-------------------------|-----------------------------------------------|--------------------------------------|
| Bethesda Marriott | 54 | 32 | 4 / 15 / 13 | 8 / 9 / 11 / 2 (+2 missing bw) | 9 / 4 / 19 |
| Waterstone | 29 | 9 | 1 / 6 / 2 | 0 / 8 / 1 / 0 | 0 / 0 / 9 |
| Renaissance TS | 29 | 8 | 0 / 4 / 4 | 0 / 8 / 0 / 0 | 0 / 0 / 8 |
| Mayflower | 37 | 11 | 0 / 0 / **11** | 0 / 0 / **11** / 0 | 3 / 8 / 0 |
| Wyndham SD Bayside | 46 | 21 | 0 / 0 / **21** | 0 / 0 / **21** / 0 | **17** / **4** / 0 |

San Diego screenshot (High 0 / Medium 0 / Watch 21 · Pursue 0 / Contact 0 / Watch 21) **matches live facing data**.

Publication “19 Priority + 2 Exploratory” refers to **`prospectReviewSection`**, not the Priority filter. Live facing shows **17 Core + 4 Exploratory** (includes 2 PE referral cards in Exploratory). Drift vs the written publication tally is packaging/register arithmetic, not the Watch stamp bug.

## Field drivers (trace)

| UI control | Field | Writers |
|------------|-------|---------|
| Priority High/Medium/Watch | `opportunity.priority` | Legacy `classifyPriority` (`scoring.js`); reviewed import previously defaulted `WATCHLIST`; UI counts missing as Watch |
| Action Window | `opportunity.bookingWindowStatus` | Qualification / scoring; reviewed import previously defaulted `WATCH` |
| Priority vs Exploratory (reports) | `prospectReviewSection` | Reviewed CSV `section` column via `reviewed-prospect-import-v1.js` |
| Customer Contacted / outcomes | `actionStatus` / validation / outcomes | Customer feedback APIs — separate |

List/detail DTOs pass through `priority` and `bookingWindowStatus` unchanged (`opportunity-list-dto.js`). Facet counts in `dealality-gdi-ui.js` use those fields only.

## San Diego / Mayflower pathology

All 21 SD facing rows:

- `prospectReviewSection` set (reviewed package)
- `hotelRelevanceDisposition = READY_FOR_CARD`
- Credible thesis present
- Usable contact route (email or functional events URL)
- `hotelFitScore = null`, `evidenceConfidence = null`
- `priority = WATCHLIST`, `bookingWindowStatus = WATCH` with **no research reason**

Mayflower 11/11 same pattern.

**Root cause:** `reviewed-prospect-import-v1.js` stamped:

```js
if (!next.priority) next.priority = WATCHLIST;
if (!next.bookingWindowStatus) next.bookingWindowStatus = WATCH;
```

so counters were non-zero, but **not evidence-assigned**.

Bethesda reviewed rows often retained prior scored Medium/Qualify/Contact from the legacy path (`if (!next.priority)` did not overwrite).

## Visibility workarounds

`customer-publication-invariants-v1.js` treats `WATCHLIST` / `FUTURE_WATCH` as **watch-lane for Ready/Watch publication counts**. That affects Ready vs Valid Watch reporting, **not** the browse Priority/Action facet assignment itself. No evidence that visibility flags wrote `priority=WATCHLIST` onto SD/Mayflower reviewed rows — the import default did.

## Cross-hotel same-account note

AHIMA Advocacy appears on Bethesda (Medium / Watch, fit 68, grade A) and Mayflower (Watch/Watch, fit null, reviewed stamp). Different hotels may legitimately differ on fit; Mayflower’s Watch is **not** a fit decision — it is the default stamp.

## Separated reviewed vs legacy (facing)

| Hotel | Reviewed-package facing | Legacy/research/PE facing |
|-------|-------------------------|---------------------------|
| Bethesda | Mixed scores (not all Watch) | High/Medium from scoring + 1 PE High |
| Waterstone / Renaissance | Mostly legacy scored Medium + Qualify | — |
| Mayflower | 11/11 Watch defaults | 0 other facing |
| San Diego | 21/21 Watch defaults | 0 other facing (PE cards included in reviewed Exploratory) |
