# Transaction-level reconciliation — LMC Parallel / Surfe hold correction

**Generated:** 2026-10-16T13:05:00Z  
**Authoritative ledger:** `data/group-demand-intelligence/paid-multisegment-budget/authorization-ledger-v1.json`  
**Pilot ledger (separate, closed):** `data/group-demand-intelligence/parallel-budget/ledger.json` ($2 historical — exhausted)  
**Backup before this correction:** `reports/gdi-pre-batch-readiness/budget-ledger-audit/20261016-150125-hold-surfe-correction/`  
**No new paid requests.** No invented charges/credits. No PE/scaling relabel as LMC.

## Corrected balances (authoritative)

| Item | USD |
|-------------------------------------|----:|
| Authorization (Mayflower/SD paid-MS) | **11.00** |
| Supported estimated incurred | **5.25** |
| Outstanding provider reservations | **0.00** |
| Unresolved accounting hold | **1.25** |
| Safely available (= 11 − 5.25 − 0 − 1.25) | **4.50** |
| Actual billed | **UNKNOWN** |

Hold composition after overlap release (not a second deduction of already-incurred LMC Surfe):

| Claimed execution | Already in incurred | Additional exposure | Hold allocated | Disposition |
|-------------------|--------------------:|--------------------:|---------------:|-------------|
| Four LMC Surfe (Danielle, Roger, Jodi, André) | **1.00** | **0.00** | **0.00** (released) | **MATCHED** — request IDs null; missing ID ≠ extra hold |
| Fifth LMC Surfe credit (FOUNDER 5×$0.25) | 0.00 | 0.25 | 0.25 | **UNRESOLVED** |
| Scaling Stephanie + Yessica Surfe | 0.00 | 0.50 | 0.50 | **EXECUTED_UNMATCHED** |
| Wisedocs Manal + Shipyard Traci original Surfe | 0.00 | 0.50 | 0.50 | **EXECUTED_UNMATCHED** |

Machine table: `TRANSACTION_RECONCILIATION.csv` / `.json`

---

## Four claimed LMC Parallel tasks ($1.40) — MATCHED (preserved)

| # | Job key | Hotel | Provider run ID | Est. | Disposition |
|---|---------|-------|-----------------|----:|-------------|
| 1 | `paid_ms:lmc_planners_social_may_v1` | Mayflower | `trun_567f3335d0c140ad84cb51c43c0b4baa` | 0.35 | **MATCHED** |
| 2 | `paid_ms:lmc_corporate_assoc_may_v1` | Mayflower | `trun_567f3335d0c140ad8222efec4611b85a` | 0.35 | **MATCHED** |
| 3 | `paid_ms:lmc_planners_social_sd_v1` | San Diego | `trun_567f3335d0c140ad8b6191e9c2922d9f` | 0.35 | **MATCHED** |
| 4 | `paid_ms:lmc_corporate_assoc_sd_v1` | San Diego | `trun_567f3335d0c140ad8e3066a4795127f5` | 0.35 | **MATCHED** |

**Timestamp provenance:** ledger `spentAt` **2026-10-15T21:35:00.000Z** is a **report timestamp** from `reports/gdi-lmc-planner-expansion/FOUNDER_REPORT.md` — **not** a verified provider submission time. Execution-history stage stamp **2026-10-15T21:44:42.649Z** is separately labeled. Actual billed: **UNKNOWN**.

`pe_sd_gaslamp_yacht` and `scaling_discovery_*_v2` remain their own prior spent rows — **not** LMC expansion.

---

## Surfe disputed attempts (corrected dispositions)

### A. LMC / planner contact-depth (~$1.25 claimed this pass)

Evidence: `reports/gdi-lmc-planner-contact-depth/FOUNDER_REPORT.md`, `account-contact-depth-register.json`.  
Queue-only / `surfe.status=skipped` re-runs are a **different pass** and do not prove the earlier enrichment never ran.

| Person | Employer / opp | Channel | Timestamp provenance | Request ID | Ledger | Disposition |
|--------|----------------|---------|----------------------|------------|--------|-------------|
| Danielle Kambic | CSI DMC / `gdi_lmc_5b4d51be7726617c` | person email | FOUNDER ~2026-10-15T22:00Z (report) | null | `rsv_recon_surfe_lmc_danielle_kambic` ($0.25 incurred) | **MATCHED** |
| Roger J. Whyte II | Stratus / `gdi_lmc_00cd663a9f282697` | person email | report | null | incurred $0.25 | **MATCHED** |
| Jodi Moraru | Evoke / `gdi_lmc_d670fde177ad418b` | person email | report | null | incurred $0.25 | **MATCHED** |
| André Wells | Events by Andre Wells / `gdi_lmc_860cde704de88d7c` | email attempt → identity conflict | report | null | incurred $0.25 (cost not zeroed) | **MATCHED** |
| FOUNDER “5th” $0.25 | — | — | — | none | hold $0.25 | **UNRESOLVED** |

### B. Scaling-contact Surfe (~$0.50) — not NOT_EXECUTED

Evidence: `reports/gdi-buyer-contact-enrichment/ANSWERS_A-G_SCALING_CONTACTS.md` (gdi-release-main + mirrored claims). CONTINUATION skip reports are a **different** pass.

| Person | Employer / purpose | Channel | Est. | Request ID | In $11 Surfe rows? | Disposition |
|--------|--------------------|---------|----:|------------|--------------------|-------------|
| Stephanie Pursglove | ESET — Office Manager person email | Surfe → identity_conflict | 0.25 | not retained in this worktree | **No** | **EXECUTED_UNMATCHED** |
| Yessica Trujillo | SOCi — People Ops | Surfe email accepted then quarantined (`kts-law.com`) | 0.25 | not retained | **No** | **EXECUTED_UNMATCHED** |

Hold allocates **$0.50**. Do not invent ledger rows or claim precise billed cost.

### C. Wisedocs / Shipyard original person-email Surfe (~$0.50) — not NOT_EXECUTED from skip reports

| Person | Employer / opp | Evidence | Est. | Request ID | In $11 Surfe rows? | Disposition |
|--------|----------------|----------|----:|------------|--------------------|-------------|
| Manal Abu-Shawar | Wisedocs / `gdi_sc_0fe98055e8df9f75` | Provider-returned `manal@wisedocs.ai` (Surfe) — `wisedocs-domain-corroboration.md`; scaling answers: prior named enrichment complete | ~0.25 | not in this ledger | **No** (Parallel `contact_wisedocs_may` $0.15 is separate) | **EXECUTED_UNMATCHED** |
| Traci Hetherington | The Shipyard / `gdi_sc_188e25b6b3b56637` | Scaling/completion docs: prior Surfe person email retained; continuation intentionally skipped repeat | ~0.25 | not in this ledger | **No** (Parallel `contact_shipyard_sd` $0.15 separate) | **EXECUTED_UNMATCHED** |

A later skip because usable channel exists does **not** establish that the earlier paid attempt never ran. Populated email alone does not prove billed cost; missing request ID does not prove zero. Hold allocates **$0.50**.

### D. Prior AMWA Surfe — MATCHED (reference)

`bcb0a16b-063e-4654-8569-f87e707c9bf6` / `rsv_recon_surfe_amwa` $0.25 — already in incurred; **not** the LMC pass.

---

## Historical $2 pilot — closed to spending

Source: `reports/gdi-operational-baseline/FINAL-REPORT.md` (final), not the intermediate after-probe snapshot alone.

| Run | Role | Est. |
|-----|------|----:|
| `trun_567f3335d0c140ad8e87120211c6f61f` | Contaminated research | 1.00 |
| `trun_567f3335d0c140ad9269500c06dc2b38` | Connectivity probe | 0.50 |
| `trun_567f3335d0c140ad8fc1b7a929f468b3` | Corrected identity-lock | 0.50 (provider actual 0.50 on this run) |

| Cap | Supported estimated incurred | Remaining | Closed? |
|----:|-----------------------------:|----------:|---------|
| 2.00 | **2.00** | **0.00** | **Yes** |

Actual billed total still **UNKNOWN** beyond the third-run provider payload. Separate from Mayflower/SD $11 and all new-hotel budgets. Do not reopen $0.50 capacity.

---

## Budget-control enforcement

`remainingParallelBudgetUsd` / Surfe `reserveSurfeSpend` → same authorization ledger:

**available = authorized − estimated incurred − active reservations − unresolved accounting hold**

- Missing authorization ledger → fail closed (`PAID_MS_LEDGER_MISSING`)
- Invalid / non-finite / negative hold → fail closed (`ACCOUNTING_HOLD_INVALID`)
- Missing hold field → 0 only for valid legacy ledger
- Hold persists through reserve/commit/release
- `releaseAccountingHold` / `increaseAccountingHold` / `convertAccountingHoldToIncurred` are idempotent; converting hold to incurred does not double-deduct a released overlap
- Dry-run never submits paid Surfe or production writes (`test-gdi-dry-run-paid-isolation-v1.mjs`)

Focused regressions: Parallel hold block, **Surfe** hold block, invalid hold, persistence, idempotent recon, dry-run isolation.

---

## Hotel activation statuses (independent)

| Scope | Cap | Actual status | Notes |
|-------|----:|---------------|-------|
| Renaissance GDI `recG66DQJKP2c0UNh` | **$12** | **PUBLISHED_VERIFIED / completed** | Owner `cursor-agent-renaissance-gdi-exec`; run `gdi_workflow_recG66DQ_2026-10-16T10-26-54-849Z_80516d`; Parallel `trun_567f3335d0c140adbc64e005916c92ac` ~$0.21; production list 15. Evidence: `gdi-renaissance-exec` `EXECUTION_OWNERSHIP.json` / `FOUNDER_DELIVERY_REPORT.md`. This accounting task did **not** launch or interrupt that run. |
| Renaissance ADP (same hotel) | **$15** | Research **complete**; production share **blocked** (`invalid_share_signature`); Airtable Live upsert **blocked** (payload size); customer delivery **incomplete** | PR #110 @ `2c58fef4` deployed research/publish path. Do not label entire delivery COMPLETE. |
| Tampa Airport Marriott | — | Config **NOT_READY**; **no explicit paid ADP/GDI cap authorized** in this tree | Do not infer cap from six-hotel proposals. Paid discovery not authorized. |
| Other hotel scopes | — | Independent | Waterstone distribution **HOLD** intact. |

Mayflower/SD $11 funds not transferred. This task is not a Renaissance activation gate.
