# PIPELINE TRACE — Radisson Blu Toronto Downtown

## Canonical ADP path (certified hotels)

| Stage | Module / function |
|------|-------------------|
| Property research / attributes | `fixtures/ai-demand-positioning/*-property-profile.json` + attribute classifications stamped into published `realityGap` |
| Scenario generation | `buildScenarioUniverse` (`prompt-universe/scenario-registry.js`) + period-baked `scenarioUniverse` / `scenarioIds` |
| Provider execution | Full ADP E2E orchestrator → runtime period observations |
| Observation capture | `data-model.createObservation` / period `observations[]` |
| Entity / mention extraction | `canonical-subject-presence-v1` via `enrichObservationsWithRank` |
| Rank extraction | `enrichObservationsWithRank` (`executive-metrics-foundation.js`) |
| Competitor extraction | observation `competitorsMentioned` + customer entity resolution |
| Territory aggregation | `scenarioUniverse.territoryAssignments` + intent territory labels |
| Provider aggregation | provider-scoped filter on comparable observations |
| Consideration | Independent Unprompted frozen `eligibleScenarioIds` × comparable mentioned |
| Scenario presence | ≥1 provider mention / Core scenario count |
| Rank metrics | rankEligible mentioned denominator |
| Reality Gap | `RG_WEIGHTED_PARTIAL_V2` (`reality-gap-methodology-v2.js`) |
| Competitive displacement | `ADP_CANONICAL_DISPLACEMENT_SUPPORT_SET_V1` / `lostDemand.displacement` |
| Citation intelligence | `computeOwnedExternalSourceMix` + `computeSourceMetrics` |
| Report snapshot | durable published-current report JSON |
| PDF | Full ADP E2E PDF artifact store + AVA PDF |
| Evidence drawers | `published-read-service.resolveEvidencePeriodForProperty` → `missing_evidence_v1` / `positive_evidence_v1` |

## Toronto vs controls

| CANONICAL FUNCTION | TORONTO USED SAME? | CONTROL USED SAME? | DIFFERENCE |
|---|---|---|---|
| enrichObservationsWithRank | YES | YES | NONE |
| Independent Unprompted eligibleScenarioIds | YES | YES | Toronto stamp has 55 eligible of 63 |
| RG_WEIGHTED_PARTIAL_V2 | YES | YES | NONE |
| loadLatestCustomerPeriod (Loyalty skip) | YES | YES | Period-family fix is global |
| resolveEvidencePeriodForProperty | YES | YES | Prefers published Core periodId |
| BPP Loyalty lens | YES | YES | Separate LOYALTY_BPP period |

**Toronto-specific calculation shortcuts:** NO (fact-conflict register is evidence governance only; does not alter metric formulas).
