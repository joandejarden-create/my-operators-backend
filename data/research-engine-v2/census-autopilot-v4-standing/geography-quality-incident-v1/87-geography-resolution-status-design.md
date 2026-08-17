# Geography Resolution Status — Design (no production schema change this task)

## Problem

Market text field cannot encode both value and resolution state.

## Proposed fields (future, not applied now)

| Field | Type | Values |
| --- | --- | --- |
| Market | text | Canonical Dealality Market or blank |
| Market Resolution Status | single select | CONFIRMED / UNRESOLVED / REVIEW / NOT_APPLICABLE |
| Submarket | text | Canonical corridor or blank |
| Submarket Resolution Status | single select | MATCHED / NOT_APPLICABLE / UNRESOLVED |

## Rules

- Do not put `UNRESOLVED` / `Unknown` strings into Market text as fake markets.
- NOT_APPLICABLE on Market only for explicit taxonomy (rare); usually Market is CONFIRMED or UNRESOLVED.
- Submarket NOT_APPLICABLE is common when Market is terminal / no corridor structure.

## Interim (pre-schema)

Claim store / governance JSON: `market_resolution_status`, `submarket_resolution_status`.
