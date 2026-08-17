# Brand AI Visibility — Publication Gate

Provider calls finishing is **not** sufficient for client visibility.

## Code anchors

- `lib/ai-visibility/validation/publication-gate.js` — `isBatchClientPublishable`, `filterSummariesForClientPublication`
- `lib/ai-visibility/controlled-release-monitoring.js` — `client_publication_check`
- Federated read store — prior wave1 + provider-baselines remain readable until roots swapped

## Required conditions

| Condition | Rule |
|-----------|------|
| RUN_COMPLETE_OR_EXPLICIT_PARTIAL | `COMPLETED` or accepted `PARTIAL` / `STOPPED_COST_CAP` with UI partial chrome |
| ARITHMETIC_PASS | Present+Missing=Monitored; rates reconcile |
| FILTER_ISOLATION_PASS | Language / geography / subject / provider isolation |
| EVIDENCE_READABLE | At least spot-checked Presence + Missing + citation |
| NO_P0 | Wrong fact / auth / leakage |
| NO_P1 | Broken monitoring integrity / wrong scope |
| CLIENT_CONTEXT_VALID | Entitlements + portfolio match intended client |

## Partial acceptance

Allowed only when:

1. Completed providers are explicitly listed (`N of 4`).
2. Missing providers show **Not Monitored** (never 0%).
3. Partial banner visible on Exec + Detail.
4. Founder signs off on PARTIAL publication.

## Signal publication

Presence-led surfaces only. Recommendation Share / Questions Won / First Recommendation remain blocked (`publication-gate` / Brand V1 contract).
