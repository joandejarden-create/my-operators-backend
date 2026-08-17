# Unknown Placeholder Policy

## Recommendation

**REMOVE literal `"Unknown"` from factual City field.**

| Layer | Store |
| --- | --- |
| Factual Airtable City | blank / null |
| Research status | `UNRESOLVED` / `CITY_UNKNOWN` in claim/queue metadata |
| UI display | "Unknown" only as presentation label |

## Why

Literal `Unknown` behaves like a real City in filters, Market mapping, analytics, and Brand Explorer joins.

## Other fields

Audit found placeholders should follow the same pattern: factual blank + research status — do **not** store `Unknown` / `N/A` / `TBD` as factual values.

## This task

Dry-run may propose `UNKNOWN_PLACEHOLDER_CLEAR` (City: `"Unknown"` → null) **only when no confirmed City fill** is available, as an optional hygiene mutation. Prefer SAFE_CITY_BLANK_FILL when a verified City exists.
