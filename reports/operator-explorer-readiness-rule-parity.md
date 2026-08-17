# Operator Explorer — Readiness Rule Parity

## Implementations

| | Dry-run | Airtable Phase 1 |
| - | ------- | ---------------- |
| File | `scripts/build-operator-explorer-calibration-01.mjs` `buildProfile` | `scripts/operator-explorer-phase-1-apply.mjs` `generateAirtablePayloads` |

## Gate comparison

| Gate | Dry Run | Airtable Phase1 | Same? | Effect |
| ---- | ------- | --------------- | ----- | ------ |
| Identity (name/OM/MA) | From entities.json | From Master fields | Yes (data) | Not used as publish gate |
| Website | In overview | In overview | Yes | Not a publish gate |
| Operating Model | Present | Present | Yes | Not a publish gate |
| Management Availability | Present | Present | Yes | Not a publish gate |
| Geography | Distinct **countries** from Market Presence (≥1 to avoid thin; ≥2 for strong) | Raw **mp row count** ≥2 for Useful | **No** | False Thin when 1 country with depth |
| Assignments Useful | asg ≥ **2** | asg ≥ **5** | **No** | Primary cause of 19→5 |
| Assignments Strong | asg ≥ **5** | asg ≥ **8** | **No** | GHL/Playa lose Strong |
| Brand Relationships Useful | Track2 needs ≥1 **BMC**; else brands count for Strong | Track2 needs br count ≥1; Track1 any | Partial | Driftwood (0 BR) fails Phase1 if asg were ≥5; dry OK on Track1 |
| Brands Strong | distinct brand **names** ≥2 | br **row count** ≥2 | Partial | Usually aligned |
| Structures | Not gated | Not gated | Yes | — |
| Evidence / Last Verified | Not gated | Not gated | Yes | — |
| Publication Status | Not gated | Not gated | Yes | — |
| Record Purpose | **Not checked** | **Not checked** | Yes | **Does not explain 19→5** |
| Lifecycle (submission_status) | **Not checked** | **Not checked** | Yes | **Does not explain 19→5** |
| Minimum assignment depth | 2 / 5 | 5 / 8 | **No** | Material |
| Source count | Not gated | Not gated | Yes | — |
| Profile section completeness | Implicit via asg/countries/brands | Implicit via asg/mp/br counts | **No** | — |
| Named vs aggregate | Local included aggregates in counts | Aggregates held out of Airtable | Intentional data | Atlantica/Sonesta/enterprise |

## Verdict

The readiness drop is **primarily a readiness-rule inconsistency**, not Record Purpose and not mass persistence loss.

Applying **dry-run rules to Airtable named assignments** recovers nearly all publishable classifications (see recalculated counts).
