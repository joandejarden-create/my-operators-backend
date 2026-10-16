# PR #111 targeted correction — closeout (A–I)

**Generated:** 2026-10-16T13:40:00Z  
**Workroot:** `C:\Dev\dealality\my-operators-backend`  
**No new paid Parallel/Surfe requests. No duplicate hotel runs.**

---

## A. Which four LMC transactions remain restored?

| Job key | Provider run ID | Est. USD |
|---------|-----------------|--------:|
| `paid_ms:lmc_planners_social_may_v1` | `trun_567f3335d0c140ad84cb51c43c0b4baa` | 0.35 |
| `paid_ms:lmc_corporate_assoc_may_v1` | `trun_567f3335d0c140ad8222efec4611b85a` | 0.35 |
| `paid_ms:lmc_planners_social_sd_v1` | `trun_567f3335d0c140ad8b6191e9c2922d9f` | 0.35 |
| `paid_ms:lmc_corporate_assoc_sd_v1` | `trun_567f3335d0c140ad8e3066a4795127f5` | 0.35 |

All **MATCHED**. Ledger `spentAt` **2026-10-15T21:35:00.000Z** is a **report timestamp** (FOUNDER expansion), not a verified provider submission time.

---

## B. Which disputed Surfe attempts are matched, unresolved, or demonstrably not executed?

| Attempt | Disposition | Notes |
|---------|-------------|-------|
| LMC Danielle / Roger / Jodi / André (4 × $0.25) | **MATCHED** | Already in incurred ($1.00); request IDs null |
| LMC FOUNDER 5th $0.25 | **UNRESOLVED** | Not located in register |
| Scaling Stephanie + Yessica (~$0.50) | **EXECUTED_UNMATCHED** | ANSWERS_A-G_SCALING_CONTACTS; not in $11 Surfe rows; continuation skip ≠ nonexecution |
| Wisedocs Manal + Shipyard Traci original (~$0.50) | **EXECUTED_UNMATCHED** | Provider email evidence; skip reports are a later pass |
| AMWA prior | **MATCHED** | Already in ledger |
| NOT_EXECUTED | **Not used** for the above disputed claims | Only where evidence supports nonexecution of that specific attempt |

---

## C. What is already incurred versus genuinely additional held exposure?

| Claimed | Already in incurred | Additional exposure | Hold allocated |
|---------|--------------------:|--------------------:|---------------:|
| Four LMC Surfe | **1.00** | 0.00 | **0.00** (released overlap) |
| Fifth LMC Surfe | 0.00 | 0.25 | 0.25 |
| Scaling Stephanie+Yessica | 0.00 | 0.50 | 0.50 |
| Wisedocs/Shipyard original Surfe | 0.00 | 0.50 | 0.50 |

Hold total remains **$1.25** after reallocating away from already-incurred LMC Surfe.

---

## D. Authorized / incurred / reserved / held / available / actual billed

| Item | USD |
|------|----:|
| Authorized (Mayflower/SD) | **11.00** |
| Estimated incurred | **5.25** |
| Reservations | **0.00** |
| Accounting hold | **1.25** |
| Available | **4.50** |
| Actual billed | **UNKNOWN** |

---

## E. Is the historical $2 pilot still closed to spending?

**Yes.** Final three-run reconciliation (`FINAL-REPORT.md`): $1.00 + $0.50 + $0.50 = **$2.00** estimated; remaining **$0**; `closedToAdditionalSpend: true`. Separate from the $11 envelope. Actual billed total still UNKNOWN beyond third-run provider payload.

---

## F. Do both Parallel and Surfe enforce the hold?

**Yes.** `remainingParallelBudgetUsd` subtracts hold; Surfe `reserveSurfeSpend` uses the same authorization ledger. Focused tests cover Parallel hold block, **Surfe** hold block, invalid/negative hold, persistence, idempotent release/increase/convert, and dry-run isolation.

---

## G. Renaissance GDI / Renaissance ADP / Tampa statuses

| Scope | Status |
|-------|--------|
| Renaissance GDI `$12` | **PUBLISHED_VERIFIED / completed** — owner `cursor-agent-renaissance-gdi-exec`; run `gdi_workflow_recG66DQ_2026-10-16T10-26-54-849Z_80516d`. This task did not launch or interrupt. |
| Renaissance ADP `$15` | Research **complete**; production share **issued** (PR #112 proof); customer delivery substantially complete pending post-deploy PDF click verify. Do not label entire delivery COMPLETE solely from research. |
| Tampa | Config **NOT_READY**; **no explicit paid cap authorized**. Do not infer from six-hotel proposals. |
| Waterstone distribution | **HOLD** intact |

---

## H. Which PR/commit was merged and deployed?

| Item | Value |
|------|-------|
| Accounting PR | **#111** squash merge `fa745b1227c5d489fbea953ac17098950becb9b6` |
| Follow-up deploy lean ignores | **#114**, **#115**, **#116** → `origin/main` **`58506d93e87271afa87e567e768654aef41b1ee6`** |
| Railway deployment observed | **`6bea37ae-4829-4374-ad78-81dd0f227f80` SUCCESS** (2026-10-16 ~13:35Z) after main merges |
| Local `deploy:production` full-tree CLI upload | Blocked by Cloudflare **413** (~380–450MB); lean ignore PRs merged; lean pack ~166MB prepared. Production service recovered SUCCESS without dirty/hotfix bypass. |

Budget-control symbols verified on `origin/main` (`releaseAccountingHold`, `ACCOUNTING_HOLD_INVALID`); available formula check locally: 11 − 5.25 − 1.25 = **4.50**.

---

## I. Were any new paid requests or duplicate hotel runs initiated?

**No.**

---

Evidence: `TRANSACTION_RECONCILIATION.md` / `.json` / `.csv`; ledger backup `budget-ledger-audit/20261016-150125-hold-surfe-correction/`.
