# ADP AI Consideration Index + Presence Index Audit V2

Research-only. Customer ACI remains **BLOCKED**. Live ADP UI and Presence Index formula are unchanged.

- Audit runner: `npm run adp:aci-presence-index-audit-v2`
- Tests: `npm run test:adp-aci-presence-index-audit-v2`
- Artifact: `reports/ai-demand-positioning/aci-presence-index-audit-v2.json`

## Live Presence Index (do not treat as fair share)

Source: `lib/ai-demand-positioning/customer/owner-payload.js` → `computeIntentPresenceIndex`

`min(round(subjectScenarioPresence / participatingDeclaredCompAvg × 100), 200)` when ≥3 declared comps appear and their average rate ≥ 30%.

100 means frequency parity vs **participating declared comps**, not equal fair share of a CORE universe.

## Recommended ACI V1 (research, unpublished)

- Actual: fractional share among subject + CORE hotels appearing in the observation (exclude obs with neither).
- Expected: `1 / (1 + CORE_COUNT)` per territory (SECONDARY = 0 in denominator).
- No overall property ACI yet.
- Do not show Presence Index and ACI together as co-equal customer heroes.
