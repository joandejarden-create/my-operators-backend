# Future-cycle Watch gating (MODE B)

**Classification:** `REUSABLE_PRODUCT_LOGIC`  
**Code:** `lib/group-demand-intelligence/future-cycle-evidence.js`, `qualification-precision.js`, `scoring.js`  
**Regressions:** `scripts/test-gdi-future-cycle-watch-gating.mjs` → `npm run test:gdi-future-cycle-watch`  
**Date:** 2026-09-16

## Purpose

Prior-year hosts and recurring annual events with an unpublished next cycle must land **WATCHLIST / FUTURE_CYCLE**, not hard **DISQUALIFIED**, when market relevance remains plausible. One-time / terminal-negative cases still DQ.

## `FUTURE_CYCLE_EVIDENCE_STATE` enum

| State | Meaning |
|---|---|
| `CURRENT_FUTURE_CYCLE_CONFIRMED` | Explicit FUTURE_CYCLE type + recurring / unpublished-next / prior-host / cycle-closed signals |
| `RECURRING_CYCLE_STRONG` | Recurring language + prior host, unpublished next, demand recurring, or current cycle closed |
| `PRIOR_YEAR_HOST_KNOWN` | Prior-host / reactivation + supporting cycle signals |
| `FUTURE_CYCLE_EXPECTED` | Demand recurring, booking watch/too-early, unpublished next, or current cycle closed |
| `FUTURE_CYCLE_UNCONFIRMED` | Recurring language or explicit type alone |
| `NO_FUTURE_CYCLE_EVIDENCE` | No preserve signals |

Terminal-negative language (`discontinued`, `moved permanently`, `one-time`, etc.) forces `preserveAsWatch=false`.

## Watchlist preservation rule

Preserve as watch when **not** terminal-negative and evidence state is strong enough that the thesis is “monitor next cycle,” not “open current sourcing.”

`shouldPreservePlacedVenueAsFutureCycleWatch` also preserves placed / no-overflow venues when reactivation (`PRIOR_HOTEL_OPPORTUNITY`, `PRIOR_MARRIOTT_OPPORTUNITY`) or booking window is `TOO_EARLY` / `WATCH`.

## Changes in core classifiers

### `deriveOpportunityType`

- Placed / no-overflow venues: if `preserve` → `FUTURE_CYCLE` (not `CLOSED_DISQUALIFIED`).
- Overflow / housing labels still yield `OVERFLOW_HOUSING` unless future-cycle preserve wins (next-cycle thesis).
- `CURRENT_CYCLE_CLOSED` / `TOO_EARLY` → `FUTURE_CYCLE`.
- Open venue statuses → `PRIMARY_PURSUIT` / `REACTIVATION` as before.

### `computeOpportunityQualification`

- Fully placed or primary-selected-no-overflow + type `FUTURE_CYCLE` / `REACTIVATION` → qualification **MODERATE**, gate passed, notes = watch-only (not open sourcing).
- Same venue statuses without preserve → **CLOSED** (DQ path).
- Territory / local-limited / `CLOSED_DISQUALIFIED` still close or weaken as before.

### `classifyPriority`

- `FUTURE_CYCLE` (and placed venues typed as such) → **WATCHLIST**, not High/Medium pursuit.
- Explicit `CLOSED_DISQUALIFIED` / closed qualification without future-cycle exception → **DISQUALIFIED**.
- Hotel Fit does **not** override a failed qualification gate into High.

## When DQ still applies

- One-time / no-recurrence / permanently moved or discontinued.
- Outside realistic territory.
- Booking window `LIKELY_TOO_LATE` with no future-cycle preserve path.
- Placed venue with **no** future-cycle / reactivation evidence.
- Explicit `CLOSED_DISQUALIFIED` opportunity type.

## Renaissance examples (post-rebuild expectation)

After qualification rebuild with this patch:

| Opportunity | Expected |
|---|---|
| NAMT Fall Conference + Festival | `FUTURE_CYCLE` + `WATCHLIST` |
| Spring Road Conference | `FUTURE_CYCLE` + `WATCHLIST` |

Pre-patch freeze may still show `CLOSED_DISQUALIFIED` / `DISQUALIFIED` for these rows. Do not treat frozen first-run JSON as the post-patch classification truth.

## Regressions covered

- Prior-host + recurring + unpublished next → Watchlist (not Closed).
- One-time / no recurrence stays DQ.
- Permanently moved out of market → terminal negative, no preserve.
- Venue TBD open path still qualifies High/Medium when gates pass.
- Historical host does **not** imply open sourcing.
- Strong evidence states map to recurring / prior-host / confirmed.
- Native vs Webhound normalization share schema.
- Parallel gate requires ≥2 triggers; Webhound-unavailable path never requires Webhound.

## Evidence note

Blind Native / Parallel discovery lanes were **not** re-run for this doc. Classification behavior above is product-logic complete; discovery-quality comparison remains `INSUFFICIENT_EVIDENCE` until those lanes execute.
