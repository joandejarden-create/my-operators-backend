# Executive Summary — Metric-to-Source Mapping (Phase 1)

| Metric | Source | Derivation | Attribution | Availability |
|---|---|---|---|---|
| Total qualified opportunities | CURRENT package opportunities | `length` | Hotel | AVAILABLE_DERIVED when CURRENT |
| New opportunities | `weekly-delta.js` | `NEW` / `isNewThisWeek` | Hotel | AVAILABLE_DERIVED |
| Materially changed | weekly-delta | `UPDATED` + `REACTIVATED` | Hotel | AVAILABLE_DERIVED |
| Actionable opportunities | `gdi-contact-actionability-v1` | SALES_READY + REACHABLE | Hotel | AVAILABLE_DERIVED |
| Need-period aligned | opp `needDateMatch` / timing status | boolean/status match | Hotel | PARTIAL_COVERAGE |
| Priority segments | segment fields on opps | top-5 counts | Hotel | AVAILABLE_DERIVED |
| Demand signals evaluated | — | — | Shared market | TELEMETRY_REQUIRED |
| Unique opportunities discovered | CURRENT count | inventory size (not raw discovery) | Mixed | PARTIAL_COVERAGE |
| Deeply researched | dossier/evidence markers | heuristic | Hotel | PARTIAL_COVERAGE |
| Evidence sources | evidence/seed URLs | deduped HTTP URLs | Mixed | PARTIAL_COVERAGE |
| Screened out | — | — | Hotel | TELEMETRY_REQUIRED |
| WHO/HOW performed | whoHow / named contact | count | Hotel | AVAILABLE_DERIVED |
| Direct buyers / influencers / entry points | whoHow presentation | person slots | Hotel | AVAILABLE_DERIVED |
| Person-specific outreach routes | contact actionability / channel fields | SALES_READY/REACHABLE (+ channel) | Hotel | AVAILABLE_DERIVED |
| Need-period matches | same as snapshot | count | Hotel | PARTIAL_COVERAGE |
| Newly discovered / materially changed | weekly-delta | counts | Hotel | AVAILABLE_DERIVED |
| Intelligence reused | — | — | Reused | TELEMETRY_REQUIRED |
| Pursued / useful / RFP / won / lost / room nights | pursuits + feedback stores | read-only | Hotel | AVAILABLE_EXISTING or UNAVAILABLE |
| Investment recovery metrics | — | Phase 1 placeholder | Hotel input | HOTEL_INPUT_REQUIRED |

**Dedup:** opportunity identity = `id` / `opportunityId`. Evidence URLs strip query strings. Shared-market runs must not be multiplied across hotels.
