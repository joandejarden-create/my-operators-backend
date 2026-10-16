# Pre-batch readiness closeout (score / dry-run / budget / deploy)

Generated: 2026-10-16T12:10:00Z  
Workroot: `C:\Dev\dealality\my-operators-backend`  
PR #108: merged @ `248a3098` (score-precedence, dry-run isolation, budget fail-closed) — deployed SUCCESS.  
PR #109: prior docs closeout — **transaction matching corrected** (this follow-up).  
PR #110: Renaissance ADP refresh — complete @ `2c58fef4`.

## A. Can a withheld score return after import?

**No.** Shared `resolveStructuredHotelFitNumericScore` requires `numericScoreEmitted === true` or `scored === true`. `scoringVersion` alone is insufficient. Explicit withheld / insufficient-evidence clears stale flats. Valid assessed, provisional, and legitimate zero are preserved. Regression: `SCORE_PRECEDENCE_REGRESSION.json` PASS. Do not reopen without regression evidence.

## B. Can dry-run trigger paid requests or production writes?

**No.** `dryRun:true` forces `authorizeSurfeEffective=false` and `persistCanonical=false` even when `authorizeSurfe:true` / `authorizePublish:true` or resume-restored auth is present. Spies throw on forbidden Surfe/canonical writes. Regression: `DRY_RUN_PROVIDER_WRITE_ISOLATION.json` PASS. Contact-depth public evaluation remains enabled. Do not reopen without regression evidence.

## C. Why was the budget ledger empty?

**Silent empty bootstrap on missing path.** `readParallelBudgetLedger` created a fresh `$11 / $0 spent / 0 entries` file when the authorization ledger was absent under a worktree/data-root. That wiped/lost prior accounting relative to operational reports. Historical `$2` pilot ledger is separate and was not the cause. Fail-closed safeguard (`PAID_MS_LEDGER_MISSING`) shipped in PR #108.

## D. Supported estimated spending, reservations, hold, and remaining balance?

| Item | USD |
|------|----:|
| Authorized ($11 Mayflower/SD envelope) | 11.00 |
| Historical pilot (separate) | 2.00 |
| Supported estimated incurred | **5.25** |
| Outstanding reservations | **0.00** |
| Historical unresolved accounting hold (Surfe) | **1.25** |
| Of estimated spent lacking provider request ID | **1.00** (4 LMC Surfe) |
| Safely available | **4.50** |
| Actual billed | **UNKNOWN** (null; not inferred as zero) |

### $2.65 dispute — corrected (not closed by equal-cost PE/scaling)

| Component | USD | Disposition |
|-----------|----:|-------------|
| Four LMC Parallel (`lmc_planners_social_*` / `lmc_corporate_assoc_*`) | 1.40 | **MATCHED** — restored to ledger; hold converted |
| Surfe LMC contact-depth envelope | 1.25 | **Hold retained** — 4 MATCHED est. (null request IDs) + 5th UNRESOLVED; AMWA not a substitute |
| `pe_sd` / `scaling_*_v2` @ $0.35 | — | **Separate prior spends** — not LMC identity |

Table: `TRANSACTION_RECONCILIATION.md` / `.csv` / `.json`. Authoritative path: `data/group-demand-intelligence/paid-multisegment-budget/authorization-ledger-v1.json`.

## E. Are the fixes deployed and verified locally and in production?

| Layer | Evidence | Result |
|-------|----------|--------|
| Git merge | PR #108 → `248a3098`; PR #110 ADP → `2c58fef4` | Verified |
| Deployment record | Production SUCCESS for those merges | Verified |
| Local/API | `LOCAL_PRODUCTION_VERIFICATION.json` (score/dry-run era) | Verified for PR #108 scope |
| Authenticated browser (AI Admin interactive) | Legitimate admin Memberstack session | **NOT RUN** |

This accounting correction is **documentation + ledger-control**; no application deploy required unless merging the `accountingHoldUsd` code path.

## F. Renaissance New York Times Square

| Product | Cap | Status |
|---------|----:|--------|
| GDI | **$12** | **AUTHORIZED_NOT_STARTED** — owning plan `reports/gdi-six-hotel-e2e/RENAISSANCE_ACTIVATION_PLAN.md` |
| ADP | **$15** | **COMPLETE** — `reports/adp-six-hotel-e2e/renaissance/FOUNDER_SUMMARY.md` |

Independent authorizations. Mayflower/SD $11 **not** transferred. Other five hotels remain queued (proposed GDI $30 / ADP $57 **not** approved). Waterstone distribution HOLD intact. No further founder “go” required for these already-authorized Renaissance scopes — authorization ≠ execution for GDI.

## Remaining blockers by capability

| Capability | Status |
|------------|--------|
| Score import precedence | Closed (PR #108 deployed) |
| Dry-run paid/write isolation | Closed (PR #108 deployed) |
| Budget ledger transaction matching | Corrected; Surfe hold $1.25; available **$4.50** |
| Paid six-hotel batch (other five) | Not approved |
| Authenticated AI Admin interactive | NOT RUN |
| Weekly scheduling / unattended publish | Off |
| Renaissance GDI execution | AUTHORIZED_NOT_STARTED (separate owning activation) |
| Renaissance ADP | COMPLETE |
