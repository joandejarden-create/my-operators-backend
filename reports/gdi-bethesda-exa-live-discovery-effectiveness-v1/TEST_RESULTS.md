# TEST RESULTS — Bethesda Live Exa Discovery Effectiveness V1

## Safety gates (PR #348) — before spend

| Check | Result |
|---|---|
| `node scripts/test-gdi-full-e2e-canonical-entry-p1c-v1.mjs` | **20/20 PASS** |
| Live requires budget ceiling + `authorize-exa-agent-live` | PASS |
| Live readiness blocked without `GDI_EXA_AGENT_LIVE=1` | PASS |
| CLI loads dotenv + Exa live flag | PASS |
| `npm run dealality:ci-precheck` | PASS |
| GitHub CI (gitleaks + GDI recovery) | PASS |
| Merged | PR #348 → `main` @ `88b7e6b07` |

## Phase A preflight (post-merge)

| Check | Result |
|---|---|
| Canonical preflight + assess-live-readiness | LIVE_READY |
| Exa credential present via dotenv | true |
| Medium quote bounded | $0.10 |
| Hard ceiling | $15 |

## Phase B live provider execution

| Check | Result |
|---|---|
| Live command exit code | **0** |
| Discovery fresh cells | 14/14 |
| Paid ledger spend | $3.20 ≤ $15 |
| Provider run IDs recorded | yes (discovery + deep) |
| Certificate | FULL_E2E_EXECUTION_COMPLETE |
| budgetsRespected / outputsIngested | true / true (evidence-based) |
| Publication / Airtable | none / 0 |
| Second paid campaign | **not run** |

## Regression / preservation

| Check | Result |
|---|---|
| Published 67 fingerprint | unchanged `441773211494…` |
| Unrelated AVA / wt dirty / stashes | preserved (not committed) |
| Apify orchestration | still not default |

## Reporting package tests

Deterministic analysis artifact: `analysis-core.json` generated from live ledger + chain + baseline package (no additional paid calls).
