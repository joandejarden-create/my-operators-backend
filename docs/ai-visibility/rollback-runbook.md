# Brand AI Visibility — Rollback Runbook

## Snapshot readiness

| Item | Mechanism |
|------|-----------|
| Monitoring batches | Immutable batch folders under `data/ai-visibility/runtime/wave1-showcase` and `provider-baselines/{gemini,perplexity,claude}` |
| Federated read | `createBrandAiVisibilityReadStore` resolves federated measured baseline roots |
| Discoverability | Timestamped `phase3c2_*.json` + `phase3c2_latest.json` pointer |
| Pre-change capture | Record rootDirs, batchIds, `phase3c2_latest` mtime **before** any new write |

**SNAPSHOT_READY:** YES (file-store append + pointer pattern)  
**ROLLBACK_READY:** YES (point reads at prior roots / restore prior `phase3c2_latest` / withhold new batch from publication filter)

## Rollback mechanism

1. **Do not delete** prior batch directories.
2. If new batch fails validation → leave `publishable: false` / exclude via publication gate → clients keep prior publishable summaries.
3. If federated pointer / env `AI_VISIBILITY_STORE_ROOT` was changed → restore previous root resolution.
4. If Discoverability latest pointer overwritten incorrectly → copy prior `phase3c2_*.json` back to `phase3c2_latest.json`.
5. Confirm Executive/Detail still load Autograph CALA EN Presence from prior baseline.

Failed validation must **not** destroy the previous valid client view.
