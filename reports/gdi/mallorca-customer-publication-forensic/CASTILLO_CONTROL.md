# Castillo Control

| Metric | Value |
|--------|-------|
| hotelId | `rec82D9zpB8fede1I` |
| Canonical Ready | 0 |
| Canonical Valid Watch | 0 |
| Customer API Ready | 0 |
| Customer API Watch | 0 |
| Empty customer state correct | **YES** |

## HOLD_WATCH vs VALID_FUTURE_WATCH

- **HOLD_WATCH** — internal research/promote hold; not customer-facing
- **VALID_FUTURE_WATCH** — only when `isValidFutureWatch.ok` (+ publication rules)

Do NOT publish WAITING_FOR_PUBLICATION, NO_CURRENT_HOTEL_PATH, SIGNAL_ONLY, or Complete Plausible that fails Watch gate.
