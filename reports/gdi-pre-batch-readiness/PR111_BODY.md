## Summary

Targeted correction for GDI paid multi-segment accounting and budget-control enforcement (executable code + reconciliation evidence — not documentation-only).

- Preserve four MATCHED LMC Parallel transactions (`paid_ms:lmc_planners_social_may_v1`, `paid_ms:lmc_corporate_assoc_may_v1`, `paid_ms:lmc_planners_social_sd_v1`, `paid_ms:lmc_corporate_assoc_sd_v1`) with provider run IDs; label report-derived timestamps.
- Correct Surfe dispositions: skip-only continuation reports do not prove earlier enrichment never ran. LMC four MATCHED (already incurred $1.00); fifth UNRESOLVED; scaling Stephanie/Yessica and Wisedocs/Shipyard original Surfe EXECUTED_UNMATCHED.
- Reallocate $1.25 accounting hold: release $1.00 overlap with already-incurred LMC Surfe; retain hold for genuinely additional unresolved exposure. Available remains $4.50 of the $11 authorization. Actual billed UNKNOWN.
- Close historical $2 pilot to additional spending using FINAL-REPORT three-run exhaustion (not the incomplete after-probe snapshot).
- Enforce accounting hold on both Parallel and Surfe reservation paths; fail closed on missing auth ledger and invalid/negative hold; add focused regressions.
- Correct Renaissance GDI status to PUBLISHED_VERIFIED (owning exec; not restarted here). Renaissance ADP: research complete / production share blocked / delivery incomplete. Tampa: no inferred paid cap.

## Balances (Mayflower/SD $11)

| Item | USD |
|------|----:|
| Authorized | 11.00 |
| Estimated incurred | 5.25 |
| Reservations | 0.00 |
| Accounting hold | 1.25 |
| Available | 4.50 |
| Actual billed | UNKNOWN |

## Test plan

- [x] `node scripts/test-gdi-paid-ms-ledger-fail-closed-v1.mjs` (Parallel + Surfe hold, invalid hold, persistence, idempotent recon)
- [x] `node scripts/test-gdi-dry-run-paid-isolation-v1.mjs`
- [ ] Merge via normal PR process; deploy from clean main; verify budget-control version without paid provider calls
