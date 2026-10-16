# Resume / invalidation rules (v1.1)

## Skip on resume (no duplicate spend/write)

- Completed paid discovery / paid-MS / Surfe stages  
- Completed apply_publish / apply_pe when fingerprints unchanged and status completed  

## Always re-evaluate when

| Stage | Re-run if |
|-------|-----------|
| `local_delivery_verify` | verificationStatus ∉ {passed, verified} OR dataRevision/buildSha/artifactFingerprint mismatch |
| `production_delivery_verify` | same |
| `reconcile_run` | same / ledger path changed |
| `evaluate_completion` | any upstream fingerprint change or prior overall ≠ COMPLETE |

## Do not

- Rerun paid research because only presentation CSS changed (fingerprint scoped to research/contact/publish artifact trees).  
- Erase original execution history or paid-call attempt records during dry-run status.  
- Mutate another hotel’s journal from status CLI.
