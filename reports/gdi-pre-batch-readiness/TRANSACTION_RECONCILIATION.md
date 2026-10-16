# Transaction-level reconciliation — LMC Parallel / Surfe correction

**Generated:** 2026-10-16T12:10:00Z  
**Authoritative ledger:** `data/group-demand-intelligence/paid-multisegment-budget/authorization-ledger-v1.json`  
**Pilot ledger (separate):** `data/group-demand-intelligence/parallel-budget/ledger.json` ($2 historical — preserved, not merged)  
**Backup before this correction:** `reports/gdi-pre-batch-readiness/budget-ledger-audit/20261016-120717-tx-match-correction/`  
**No new paid requests.** No invented charges/credits. No PE/scaling relabel as LMC.

## Corrected gap math

| Narrative (prior operational claim) | USD |
|-------------------------------------|----:|
| Spent before LMC expansion | 3.85 |
| Four LMC Parallel × $0.35 | +1.40 |
| LMC contact-depth Surfe ≈ | +1.25 |
| Claimed total spent / remaining | **6.50 / 4.50** |

| Authoritative after this correction | USD |
|-------------------------------------|----:|
| Supported estimated incurred (incl. four MATCHED LMC Parallel) | **5.25** |
| Outstanding provider reservations | **0.00** |
| Historical unresolved accounting hold (Surfe envelope) | **1.25** |
| Safely available (= 11 − 5.25 − 1.25) | **4.50** |
| Actual billed | **UNKNOWN** |

Original disputed **$2.65** hold: **$1.40 converted** into incurred when LMC Parallel tasks were MATCHED and restored; **$1.25 retained** as accounting hold (not incurred, not a reservation).

Machine table: `TRANSACTION_RECONCILIATION.csv` / `.json`

---

## Prior incorrect claims (PR #109) — corrected

| Claim | Why wrong | Correction |
|-------|-----------|------------|
| Four LMC Parallel = `pe_sd_gaslamp_yacht` + `scaling_discovery_may_v2` + `scaling_discovery_sd_v2` (+ one NOT_EXECUTED) | Equal **$0.35** cost ≠ transaction identity; different job keys / purposes / run IDs | Match the four **LMC** discovery runs below |
| Fourth Parallel **NOT_EXECUTED** | Missing folder / absent run in wrong matching set ≠ nonexecution | All four LMC runs evidenced → **MATCHED**; do not use NOT_EXECUTED without stronger nonexecution proof |
| Surfe $1.25 **DUPLICATE** of AMWA + 4 LMC already in $3.85 | Substituted prior AMWA into a later LMC pass; FOUNDER contact-depth claimed **this pass** 5 × $0.25 | Reconcile Surfe separately (below); keep AMWA as prior paid-MS only |
| Gap closed → safely available **$7.15** | Left omitted LMC Parallel out of ledger and treated hold as closed | Restore LMC Parallel; retain Surfe hold; available **$4.50** |

`pe_sd_gaslamp_yacht` and `scaling_discovery_*_v2` remain **their own** prior spent rows. They are **not** LMC expansion executions.

---

## Four claimed LMC Parallel tasks ($1.40) — MATCHED

Evidence: `reports/gdi-lmc-planner-expansion/FOUNDER_REPORT.md`, `data/group-demand-intelligence/execution-history/gdi_three_hotel_integrity_2026-10-15T21-44-42-649Z.json`, script `scripts/gdi-lmc-planner-expansion-execute-v1.mjs` (task defs).  
Authorization: existing Mayflower/SD **$11** paid-MS envelope (no new allowance). Surfe on this expansion pass: **$0** (no named person yet).

| # | Job key | Hotel | Purpose | Provider run ID | Est. | Ledger reservation | Disposition |
|---|---------|-------|---------|-----------------|----:|--------------------|-------------|
| 1 | `paid_ms:lmc_planners_social_may_v1` | Mayflower `rec0eTEnCqlsGJJt9` | LMC planners + social catering discovery | `trun_567f3335d0c140ad84cb51c43c0b4baa` | 0.35 | `rsv_recon_paid_ms_lmc_planners_social_may_v1` | **MATCHED** (restored) |
| 2 | `paid_ms:lmc_corporate_assoc_may_v1` | Mayflower `rec0eTEnCqlsGJJt9` | LMC corporate / room rental / association / nonprofit | `trun_567f3335d0c140ad8222efec4611b85a` | 0.35 | `rsv_recon_paid_ms_lmc_corporate_assoc_may_v1` | **MATCHED** (restored) |
| 3 | `paid_ms:lmc_planners_social_sd_v1` | San Diego `recnPB3Q02NO8Ma1o` | LMC planners + social catering discovery | `trun_567f3335d0c140ad8b6191e9c2922d9f` | 0.35 | `rsv_recon_paid_ms_lmc_planners_social_sd_v1` | **MATCHED** (restored) |
| 4 | `paid_ms:lmc_corporate_assoc_sd_v1` | San Diego `recnPB3Q02NO8Ma1o` | LMC corporate / association / nonprofit | `trun_567f3335d0c140ad8e3066a4795127f5` | 0.35 | `rsv_recon_paid_ms_lmc_corporate_assoc_sd_v1` | **MATCHED** (restored) |

Submission timestamp (report): **2026-10-15T21:35:00.000Z** (expansion FOUNDER). Execution-history stage recorded **2026-10-15T21:44:42.649Z**. Actual billed: **UNKNOWN**.

### 13 fresh LMC/planner accounts — provenance

Expansion FOUNDER: **13 unique** new LMC/planner prospects from these four Parallel runs (16 raw → dedupe → 13; **12** published facing, **1** internal WHF). Source runs = the four `trun_` IDs above — not PE/scaling jobs.

---

## Surfe — separate from Parallel; do not substitute AMWA

### Later LMC / planner contact-depth pass (disputed ~$1.25)

Evidence: `reports/gdi-lmc-planner-contact-depth/FOUNDER_REPORT.md`, `account-contact-depth-register.json`.  
FOUNDER claimed **5 × $0.25 ≈ $1.25 this pass**. Register evidences **four** Surfe outcomes (no provider request IDs retained). Later queue-only re-runs with `surfe.status=skipped` are **not** proof the earlier pass never ran.

| # | Person / opportunity | Employer / channel | Request ID | Est. | Ledger | Disposition |
|---|----------------------|--------------------|------------|----:|--------|-------------|
| 1 | Danielle Kambic / `gdi_lmc_5b4d51be7726617c` | CSI DMC — person email via Surfe | **null** | 0.25 | `rsv_recon_surfe_lmc_danielle_kambic` | **MATCHED** (est.; request ID **UNRESOLVED**) |
| 2 | Roger J. Whyte II / `gdi_lmc_00cd663a9f282697` | Stratus Firm — person email via Surfe | **null** | 0.25 | `rsv_recon_surfe_lmc_roger_whyte` | **MATCHED** (est.; request ID **UNRESOLVED**) |
| 3 | Jodi Moraru / `gdi_lmc_d670fde177ad418b` | Evoke — person email via Surfe | **null** | 0.25 | `rsv_recon_surfe_lmc_jodi_moraru` | **MATCHED** (est.; request ID **UNRESOLVED**) |
| 4 | André Wells / `gdi_lmc_860cde704de88d7c` | Events by Andre Wells — identity conflict | **null** | 0.25 | `rsv_recon_surfe_lmc_andre_wells_conflict` | **MATCHED** (invoked; cost not zeroed; request ID **UNRESOLVED**) |
| 5 | FOUNDER “5th” $0.25 this pass | — | none located | 0.25 | — | **UNRESOLVED** (insufficient to add charge or declare NOT_EXECUTED) |

**Accounting hold $1.25** covers the unresolved Surfe enrichment envelope (request IDs + fifth-credit identity). Hold ≠ incurred ≠ reservation.

### Prior / other Surfe (reference only — already handled; do not re-add)

| Item | Evidence | In $11 ledger? | Note |
|------|----------|----------------|------|
| Prior AMWA named-contact Surfe $0.25 | `bcb0a16b-063e-4654-8569-f87e707c9bf6` / `rsv_recon_surfe_amwa` | **Yes** | Prior paid-MS — **not** the later LMC pass |
| Wisedocs / Shipyard Surfe | CONTINUATION / TASK_BY_SEGMENT: Surfe **skipped** (usable org email) | Parallel contact rows only (`contact_wisedocs_may` / `contact_shipyard_sd` @ $0.15 each) | Surfe **$0** — not a missing Surfe charge |
| Scaling-contact Surfe | CONTINUATION_SUMMARY: skipped | No Surfe rows | Surfe **$0** this continuation |

---

## Balances (authoritative)

| Item | USD | Role |
|------|----:|------|
| Authorized ($11 Mayflower/SD) | **11.00** | Cap |
| Supported estimated incurred | **5.25** | Incurred (estimate) |
| Actual billed | **UNKNOWN** | Invoice |
| Outstanding provider reservations | **0.00** | Reservation |
| Historical unresolved accounting hold | **1.25** | Hold (Surfe envelope) |
| Safely available | **4.50** | Available pending Surfe reconciliation |

Controls: fail-closed missing authorization ledger (`PAID_MS_LEDGER_MISSING`); `accountingHoldUsd` reduces available via `remainingParallelBudgetUsd` (not counted as spent or reserved). Historical $2 pilot path unchanged / restored separately.

**Workroot note:** Authoritative corrected copy lives under `my-operators-backend`. If shell `GDI_DATA_ROOT` points at another worktree (observed: `gdi-release-main`), that on-disk authorization ledger must be kept in sync — a missing/stale folder in one tree is not proof of nonexecution, and unequal trees reintroduce the empty-bootstrap class of error.

---

## Hotel activation (independent authorizations)

| Scope | Cap | Status | Ownership |
|-------|----:|--------|-----------|
| Renaissance GDI (`recG66DQJKP2c0UNh`) | **$12** | **AUTHORIZED_NOT_STARTED** | `reports/gdi-six-hotel-e2e/RENAISSANCE_ACTIVATION_PLAN.md` |
| Renaissance ADP (same hotel) | **$15** | **COMPLETE** (PR #110 @ `2c58fef4`, deploy SUCCESS) | `reports/adp-six-hotel-e2e/renaissance/` |
| Other five hotels | proposed GDI $30 / ADP $57 | **Not approved** | Queued only |
| Waterstone distribution | — | **HOLD** intact | — |

Mayflower/SD $11 funds **not** transferred. Authorization ≠ execution for GDI Renaissance. This accounting task did **not** launch GDI/ADP runs or paid requests.
