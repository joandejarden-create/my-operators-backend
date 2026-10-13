# QUALIFY status audit — W Rome

## Meaning
`bookingWindowStatus: QUALIFY_NOW` renders as customer pill **QUALIFY**.

## Finding
On the frozen customer-visible cohort (6), QUALIFY appeared on cards that were still foundational research — not Ready sales actions.

## Separation
| Layer | Meaning | W Rome after |
|---|---|---|
| READINESS STATE | Ready / Future Watch / Research | Demoted to FUTURE_WATCH |
| SALES ACTION STATE | Contact / Watch / Qualify | `WATCH` persisted |

QUALIFY/READY contradictions in starting cohort: **6**  
QUALIFY on Ready after: **0** (must be 0)
