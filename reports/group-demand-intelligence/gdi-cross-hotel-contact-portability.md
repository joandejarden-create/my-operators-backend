# GDI Cross-Hotel Contact Portability

**Verdict: PORTABLE**

## Principle

> Every GDI pilot correction must be implemented as a reusable rule, scoring change, research method, validation gate, or regression test unless demonstrably hotel-specific.

> Hotel-specific facts belong in configuration/data, not core logic.

## Contact-stack modules (hotel-agnostic)

| Module | Role |
|---|---|
| `lib/group-demand-intelligence/contact-coverage.js` | Funnel, eligibility, cohort builder, grade simulation |
| `lib/group-demand-intelligence/contact-candidate/*` | WHO ontology, scoring, discovery |
| `lib/hotel-intelligence/contact-intelligence/surfe-identity-acceptance.js` | Identity + field ownership gates |
| `lib/hotel-intelligence/contact-intelligence/provider-submission-gate.js` | Paid submit preflight |
| `lib/surfe/client.js` | Provider adapter only |

## Hotel-specific data (correct under Operating Law)

| Asset | Why OK |
|---|---|
| `data/group-demand-intelligence/hotels/<hotelId>/` | Opportunity packs |
| `data/group-demand-intelligence/evals/*cohort*` | Frozen eval cohorts |
| `official-person-discoveries-v1.js` | Pilot evidence seeds (data) |
| `commercial-qa-overrides-v1.js` | Bethesda commercial QA overrides |

## Coupling audit (touched contact modules)

| Check | Result |
|---|---|
| `contact-coverage.js` hardcodes `recLuxvwwxID7U2B8` | No |
| `contact-candidate/scoring.js` requires DMV keywords | No — uses `geographyHints` |
| Synthetic Miami/Tampa/Austin fixture coverage | Pass |
| Same eligibility rules without Bethesda event names | Pass |

## Known non-contact Bethesda coupling (out of scope for this pass)

These remain pilot-era hotel/demand modules — **not** in the contact reachability path:

- `qualification-precision.js` / `scoring.js` copy mentioning Bethesda
- `dmv-expansion-pass.js` opportunity seeds
- `hotel-profile.js` `PILOT_HOTEL_ID`

Contact stack verdict remains **PORTABLE**. Broader GDI demand scoring portability is a separate layer.

## Regression

```bash
npm run test:gdi-contact-coverage-portability
npm run test:surfe-identity-acceptance
```
