# LIVE EXECUTION CERTIFICATION — Bethesda Exa V1

## Exact command

```bash
GDI_EXA_AGENT_LIVE=1 npm run gdi:canonical-full-e2e -- --hotel recLuxvwwxID7U2B8 --mode live --authorize-paid-discovery --authorize-paid-research --authorize-exa-agent-live --budget-ceiling-usd 15 --max-candidates 20 --exa-effort medium --out reports/gdi-bethesda-exa-live-discovery-effectiveness-v1/live-run --observability-root reports/gdi-bethesda-exa-live-discovery-effectiveness-v1/observability
```

Safety prerequisite merged first: PR #348 (`feat/gdi-bethesda-exa-live-safety-gates-v1`) — dotenv CLI load, `--authorize-exa-agent-live`, hard ceiling required, `assessCanonicalLiveReadiness`, evidence-based `budgetsRespected` / `outputsIngested`.

## Authorization controls

| Control | Value |
|---|---|
| Provider | Exa AI only |
| `authorizePaidDiscovery` | true |
| `authorizePaidResearch` | true |
| `authorizeExaAgentLive` | true |
| `GDI_EXA_AGENT_LIVE` | 1 |
| `budgetCeilingUsd` | 15.00 |
| `exaEffort` | medium (bounded quote $0.10/call) |
| `maxCandidates` | 20 |
| Apify | disabled / not orchestrated |
| Publication | not authorized |
| Airtable | 0 writes |

## Run identity

- **Run ID:** `gdi_e2e_14seg_mwladic1_ff9898`
- **Hotel:** Bethesda Marriott `recLuxvwwxID7U2B8`
- **Observability:** `reports/gdi-bethesda-exa-live-discovery-effectiveness-v1/observability/...`
- **Ledger:** `live-run/paid-budget-ledger.json`

## Provider requests (summary)

| Stage | Settled ops | Spend USD |
|---|---|---|
| DISCOVERY | 14 | 1.40 |
| DEEP_RESEARCH | 18 | 1.80 |
| **Total** | **32** | **3.20** |
| Blocked ops | 0 | — |

All discovery cells: `mode=FRESH_RETRIEVAL` with Exa `agent_run_*` IDs. Deep ops: `EXA_DEEP_SEED_PLUS_ORS`.

## Discovery / deep-research outcomes

- Discovery extracted candidates: **165** across 14 cells
- Processed through original chain: **20**
- Live deep flags on processed: 18
- Certificate research completeness: `DEEP_RESEARCH_COMPLETED`

## Actual cost

- **Spent:** $3.20
- **Ceiling:** $15.00
- **Remaining:** $11.80
- **budgetsRespected:** true (spent ≤ ceiling)

## Final certificate

- **Verdict:** `FULL_E2E_EXECUTION_COMPLETE`
- **outputsIngested:** true (persisted chain trace + discovery state + observability run)
- Execution monitor run summary: segmentsCompletedOf14=14, providerCalls=53, totalSpendUsd=4.6

## Remaining execution gaps

1. Only 20 of 165 extracted candidates received paid deep research (intentional budget/maxCandidates control).
2. No CONFIRMED_DEMAND / RELIABLE_INFERRED_DEMAND promotions in this campaign — watchlist only.
3. Preferred lodging deep SKIPPED after duplicate detection.
4. Authorization label in ledger still defaults to `GDI_AIMBRIDGE_FULL_E2E_LIVE_V1` string (cosmetic; spend isolated to Bethesda outDir).
5. Customer publication not in scope — research artifacts only.
