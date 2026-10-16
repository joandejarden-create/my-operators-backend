# Artifact → evaluator mapping (v1.1)

| Evidence | Source path (under runDir) | Evaluator field |
|----------|----------------------------|-----------------|
| Segment coverage | `paid_multisegment/segment-coverage.json`, scorecard, or `full-e2e-evidence.json` | `segmentCoverage` |
| Contact queue/results | `buyer_contact_enrichment/*` | `contactDepth` |
| Publication | `stages.apply_publish`, `fullE2e.publication` | `publication` |
| Local verify | `full_e2e/local-delivery-verify.json` (machine) | `localVerification` |
| Production verify | `full_e2e/production-delivery-verify.json` | `productionVerification` |
| Budget | `GDI_PARALLEL_BUDGET_LEDGER_PATH` via `reconcileAuthoritativeBudget` | `budget` |
| Stage outcomes | `stage-completion-ledger.json` | `stageResults` / derive |
| Fingerprints | run artifact hash + build SHA | `currentFingerprints` |

Handwritten `full-e2e-evidence.json` is optional overlay. Machine failures always win over operator `passed`.
