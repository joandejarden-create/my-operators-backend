# Transaction-level reconciliation — $2.65 gap ($1.40 Parallel + ~$1.25 Surfe)

**Generated:** 2026-10-16T10:15:00Z  
**Authoritative ledger:** `data/group-demand-intelligence/paid-multisegment-budget/authorization-ledger-v1.json`  
**Pilot ledger (separate):** `data/group-demand-intelligence/parallel-budget/ledger.json` ($2 — preserved, not merged)  
**Backup before this closeout:** `reports/gdi-pre-batch-readiness/budget-ledger-audit/20261016-101429-gap265/`  
**No new paid requests.** No invented charges/credits.

## Gap math

| Narrative | USD |
|-----------|----:|
| Prior claim: spent before later passes | 3.85 |
| Prior claim: four Parallel × $0.35 | +1.40 |
| Prior claim: LMC/planner Surfe ≈ | +1.25 |
| Prior claim: total spent / remaining | **6.50 / 4.50** |
| Reconstructed ledger estimated spent / remaining | **3.85 / 7.15** |
| **Exact difference** | **2.65** |

### Why $2.65 (transaction identity, not narrative)

| Component | USD | Disposition |
|-----------|----:|-------------|
| Surfe envelope already inside reconstructed $3.85 (AMWA + 4 LMC = 5 × $0.25) | 1.25 | **DUPLICATE** if stacked again on top of $3.85 |
| Three Parallel $0.35 tasks already inside reconstructed $3.85 (pe_sd + scaling_may_v2 + scaling_sd_v2) | 1.05 | **DUPLICATE** if stacked again |
| Fourth Parallel $0.35 with no run ID / artifact | 0.35 | **NOT_EXECUTED** (narrative-only) |
| **Sum** | **2.65** | Explains prior $6.50 vs ledger $3.85 |

Do **not** force remaining to $4.50. Do **not** add another $2.65 to the ledger.

Machine table: `TRANSACTION_RECONCILIATION.csv` / `.json`

---

## Four Parallel tasks at $0.35 (claimed $1.40)

| # | Job / purpose | Hotel | Run ID | Auth / reservation | Status | Est. | Actual billed | In ledger? | Dedupe key | Disposition |
|---|---------------|-------|--------|--------------------|--------|----:|---------------|------------|------------|-------------|
| 1 | `paid_ms:pe_sd_gaslamp_yacht` PE Gaslamp/yacht discovery | `recnPB3Q02NO8Ma1o` | `trun_567f3335d0c140ad9bc68e2c107a280e` | `rsv_recon_paid_ms_pe_sd_gaslamp_yacht` | spent | 0.35 | UNKNOWN | **Yes** | `providerRunId` | **INCLUDED** |
| 2 | `paid_ms:scaling_discovery_may_v2` scaling continuation | `rec0eTEnCqlsGJJt9` | `trun_567f3335d0c140adb82a54365648fad5` | `rsv_recon_paid_ms_scaling_discovery_may_v2` | spent | 0.35 | UNKNOWN | **Yes** | `providerRunId` | **INCLUDED** |
| 3 | `paid_ms:scaling_discovery_sd_v2` scaling continuation | `recnPB3Q02NO8Ma1o` | `trun_567f3335d0c140ad949f162eb795ee58` | `rsv_recon_paid_ms_scaling_discovery_sd_v2` | spent | 0.35 | UNKNOWN | **Yes** | `providerRunId` | **INCLUDED** |
| 4 | Claimed fourth Parallel @ $0.35 | — | **none found** | — | — | 0.35 | n/a | No | — | **NOT_EXECUTED** |

Evidence: `reports/gdi-paid-multisegment-validation/TASK_BY_SEGMENT_RECONCILIATION.md`, `CONTINUATION_SUMMARY.json`, `CONTINUATION_A_H.md` (continuation new Parallel = **$0.70** = two v2 tasks only).  
`reports/gdi-lmc-planner-expansion/` path **does not exist** in this workroot; LMC expansion folder present is `gdi-lmc-subsegment-expansion` (plan-only, no Parallel spend).

No omitted Parallel $0.35 row to restore. No duplicate ledger row to delete (single entry each).

---

## Surfe ≈ $1.25 (claimed “subsequent LMC/planner enrichment”)

| # | Purpose | Hotel / opportunity | Request / run ID | Auth / reservation | Status | Est. | Actual billed | In ledger? | Dedupe key | Disposition |
|---|---------|---------------------|------------------|--------------------|--------|----:|---------------|------------|------------|-------------|
| 1 | Prior AMWA named-contact Surfe | Mayflower | `bcb0a16b-063e-4654-8569-f87e707c9bf6` | `rsv_recon_surfe_amwa` | spent | 0.25 | UNKNOWN | **Yes** | `providerRunId` | **INCLUDED** (prior paid-MS, not “fresh LMC-only”) |
| 2 | LMC Danielle Kambic email | `gdi_lmc_5b4d51be7726617c` | **null** (not retained in later queue-only artifact) | `rsv_recon_surfe_lmc_danielle_kambic` | spent (est.) | 0.25 | UNKNOWN | **Yes** | `jobKey` | **INCLUDED** — request ID **UNRESOLVED** |
| 3 | LMC Roger Whyte email | `gdi_lmc_00cd663a9f282697` | null | `rsv_recon_surfe_lmc_roger_whyte` | spent (est.) | 0.25 | UNKNOWN | **Yes** | `jobKey` | **INCLUDED** — request ID **UNRESOLVED** |
| 4 | LMC Jodi Moraru email | `gdi_lmc_d670fde177ad418b` | null | `rsv_recon_surfe_lmc_jodi_moraru` | spent (est.) | 0.25 | UNKNOWN | **Yes** | `jobKey` | **INCLUDED** — request ID **UNRESOLVED** |
| 5 | LMC André Wells (identity conflict) | `gdi_lmc_860cde704de88d7c` | null | `rsv_recon_surfe_lmc_andre_wells_conflict` | spent (est.) | 0.25 | UNKNOWN | **Yes** | `jobKey` | **INCLUDED** — invoked; no email accepted; cost not zeroed |

Evidence: `reports/gdi-lmc-planner-contact-depth/FOUNDER_REPORT.md` (Danielle/Roger/Jodi/André Surfe outcomes).  
Later `results/*.json` / `buyer-contact-enrichment-pass.json` are **queue-only re-runs** with `surfe.status=skipped` (channel already present / not re-authorized) — **not** proof that Surfe never ran; do not treat as zero spend.

FOUNDER_REPORT “5 × $0.25 this pass” overstated LMC-only count: **4** LMC Surfe evidenced + **1** prior AMWA = **$1.25 total Surfe already in ledger**. No fifth LMC Surfe to add (**NOT_EXECUTED** as extra).

---

## Full reconstructed estimated spent ($3.85)

Paid-MS Parallel/core through continuation (`TASK_BY_SEGMENT` + v2): **$2.85** (includes the three $0.35 tasks + AMWA Surfe $0.25).  
Plus four LMC Surfe estimates: **$1.00**.  
**Total $3.85.**

---

## Balances (authoritative)

| Item | USD |
|------|----:|
| Authorized ($11 Mayflower/SD envelope) | **11.00** |
| Supported estimated incurred | **3.85** |
| Outstanding reservations | **0.00** |
| Unresolved spending exposure *beyond* ledger | **0.00** (fourth Parallel not executed; no orphan charge found) |
| Of estimated spent, lacking provider request ID | **1.00** (4 LMC Surfe — already deducted; actual billed still UNKNOWN) |
| Safely available for new spend | **7.15** |
| Actual billed (invoices) | **UNKNOWN** |

Controls: fail-closed missing authorization ledger (`PAID_MS_LEDGER_MISSING`); callers resolve `GDI_PAID_MS_BUDGET_LEDGER_PATH` / `GDI_DATA_ROOT` → same `paid-multisegment-budget/authorization-ledger-v1.json`. Historical $2 pilot path unchanged.

---

## Hotel activation (unchanged)

Renaissance / six-hotel batch **not started**. Mayflower/SD funds **not** transferred to six-hotel or ADP proposals.  
Renaissance package available: `reports/gdi-six-hotel-e2e/RENAISSANCE_ACTIVATION_PLAN.md` + `BASELINE_RECONCILIATION.md` (prep only).
