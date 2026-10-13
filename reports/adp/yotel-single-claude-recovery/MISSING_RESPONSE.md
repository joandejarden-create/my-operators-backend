# Missing Response

| Field | Value |
|-------|-------|
| periodId | adp_period_adp_yotel_geneva_lake_20261005144555_b21d18 |
| observationId | obs_4d8d697a2caf |
| scenarioId | gen_adp_yotel_geneva_lake_18 |
| territory / intent | group_meeting |
| exact prompt | Compare boutique meeting hotels in Lake Geneva / La Côte for a 40-person company event |
| provider | claude |
| provider model/config | claude-sonnet-4-6; maxTokens=2000; timeoutMs=60000; costPerCall=0.03 |
| original request timestamp | 2026-10-05T15:14:40.953Z |
| timeout/error | claude timeout after 60000ms |
| retry history | none (first recovery) |
| expected response grain | property × scenario × provider × period (PROVIDER_RESPONSE / OBSERVATION_GRAIN) |
| unresolved Claude timeouts | 1 |
| other non-comparable Claude rows | 1 |

Exact reconstruction: scenario query from `buildScenarioUniverse` (same `adp_generic_profile_scenarios_v2` source as baseline). Observation did not store prompt text; query is recovered from governed scenarioId — **EXACT_REPLAYABLE** under shared scenario registry.
