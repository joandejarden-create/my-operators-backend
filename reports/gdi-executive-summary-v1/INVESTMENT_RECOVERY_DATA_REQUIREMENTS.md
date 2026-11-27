# Investment Recovery — Future Data Requirements (Phase 1)

**Status:** Contract + requirements only — **no customer financial KPIs in Phase 1**  
**Prohibited now:** Hardcoded ADR · fabricated revenue · unsupported booking attribution · synthetic ROI  

Phase 1 UI shows that investment recovery is **not yet available** and lists required inputs. Metrics remain `HOTEL_INPUT_REQUIRED` / non-customer-visible until authorized.

---

## Metric keys (established)

| Key | Meaning | Required inputs before customer display |
|---|---|---|
| `identifiedRevenuePotential` | Evidence-backed room-night × authorized rate view | Hotel-approved ADR or contribution rate; need-period room-night estimate with provenance |
| `activePipelineValue` | Sum of pursued opportunities with credible timing | Pursuit status + hotel-confirmed pipeline rules |
| `bookedRevenue` | Contracted / booked attributed to Dealality intelligence | Hotel booking system attribution or signed confirmation |
| `actualizedRevenue` | Stayed / realized revenue | PMS / hotel finance actuals with period match |
| `hotelReportedContribution` | Hotel-reported commercial contribution | Explicit hotel-reported figure + as-of date |
| `dealalityInvestment` | Dealality commercial investment against hotel | Finance-authorized investment ledger (internal; optional customer disclosure) |
| `revenueToInvestmentMultiple` | Ratio of attributed revenue to investment | Both numerator and denominator authorized |
| `contributionBasedInvestmentRecovery` | Recovery vs contribution | Hotel contribution + investment both authorized |

---

## Binding rules

1. **No hardcoded ADR** — values 165 / 195 / 235 must never appear as hotel facts.  
2. **No synthetic scenarios as customer facts** — scenario modeling, if ever added, must be labeled non-factual and founder-gated.  
3. **Missing ≠ zero** — unavailable metrics omit or show unavailable state.  
4. **Schema / persistence** — if durable storage is required for hotel financial inputs, return a **documented proposal** (Airtable vs hotel-config store) rather than silent migration.  
5. **Customer visibility** — financial KPIs require CURRENT publication context **and** hotel-authorized inputs.

---

## Minimum durable records (proposal — not implemented)

| Record | Fields (illustrative) | Owner |
|---|---|---|
| Hotel commercial inputs | `hotelId`, `adrAuthorized`, `contributionRate`, `asOf`, `source`, `authorizedBy` | Founder / hotel commercial |
| Booking attribution events | `opportunityId`, `hotelId`, `roomNights`, `revenue`, `status` (pipeline/booked/actualized), `evidenceRef` | Hotel + Dealality CS |
| Investment ledger entries | `hotelId`, `amountUsd`, `period`, `category` | Dealality finance |

Do **not** invent Airtable fields in Phase 1. Open a schema proposal when founder authorizes financial persistence.

---

## Phase 1 payload behavior

`buildInvestmentRecoveryPlaceholder()` returns all keys with `value: null`, `availability: HOTEL_INPUT_REQUIRED`, `customerVisible: false`.
