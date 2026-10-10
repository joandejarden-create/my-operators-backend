# Scenario Universe Contract

Module: `adp-scenario-universe-contract-v1.js`

Every period persists: scenarioUniverseId, scenarioIds[], territory assignments, prompt/capability/rank/provider eligibility, capability exclusion reasons (TRUE_CAPABILITY_EXCLUSION | DATA_GAP_EXCLUSION | MODEL_RULE_EXCLUSION | UNKNOWN).

Comparison never uses scenario count alone — requires exact or governed-equivalent scenario IDs.

Change control via `diffScenarioUniverses` — no silent drift.
