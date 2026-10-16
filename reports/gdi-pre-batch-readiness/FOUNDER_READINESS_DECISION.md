# GDI pre-batch readiness decision

**Decision date:** 2026-10-16 (updated 13:05Z — Surfe hold overlap + status correction)  
**Workroot:** `C:\Dev\dealality\my-operators-backend`  
**Code baseline:** branch `fix/gdi-tx-match-hold-renaissance-auth-sync` (PR #111 corrections)

Six-hotel batch (other five): **queued only — not approved**. Do not infer Tampa paid caps from proposals.  
Renaissance GDI: **PUBLISHED_VERIFIED / completed** ($12) — owner `cursor-agent-renaissance-gdi-exec`; this accounting task did not launch or interrupt that run.  
Renaissance ADP: research **complete**; production share **blocked**; customer delivery **incomplete** ($15).  
Tampa: config **NOT_READY**; **no explicit paid cap authorized**.  
Waterstone distribution: **HOLD** intact.

---

## A. Is the process ready for one additional hotel E2E?

Renaissance GDI $12 E2E already **ran and published** under its owning execution task. Further hotels remain queued only. PR #108 score/dry-run/ledger safeguards remain the budget-control baseline; this PR adds hold fail-closed + Surfe hold regressions.

Authenticated AI Admin interaction remains **NOT RUN** — admin-operations gap, not a research/publication blocker.

---

## B. What was newly fixed versus already completed?

### Already completed (accepted; not reapplied)

| Track | Evidence |
|-------|----------|
| A. Qualification/publication repair | PRs #101–#103 / b571d945 lineage |
| B. AI Admin More-menu portal | PR #104 @ `8148eed9` |
| C. Contact-depth | PR #99 closed; superseded by #106 + #107 |
| D. Score precedence / dry-run isolation / ledger fail-closed | PR #108 @ `248a3098` **merged + deployed** |
| E. Renaissance ADP research refresh | PR #110 @ `2c58fef4` — research complete; share/persistence gaps remain |
| F. Renaissance GDI $12 execution | Owning task in `gdi-renaissance-exec` — **PUBLISHED_VERIFIED** |

### This correction (accounting + budget-control code; no paid calls)

1. Preserved four MATCHED LMC Parallel transactions with report-timestamp provenance labels.  
2. Corrected Surfe dispositions: LMC four **MATCHED**; fifth **UNRESOLVED**; scaling + Wisedocs/Shipyard **EXECUTED_UNMATCHED** (not NOT_EXECUTED from skip reports).  
3. Released $1.00 hold overlapping already-incurred LMC Surfe; reallocated hold to genuinely additional exposure (still **$1.25** total).  
4. Closed historical $2 pilot to spending (final three-run reconciliation).  
5. Enforced hold on Parallel **and** Surfe with focused regressions.  
6. Corrected Renaissance GDI/ADP/Tampa status language.

---

## C. What was actually verified locally and in production?

| Layer | Status |
|-------|--------|
| Focused budget-control + dry-run tests | PASS (this branch) |
| This accounting task paid calls | **None** |
| Duplicate hotel runs | **None** |

---

## F. Budget (authoritative Mayflower/SD $11)

| Item | USD |
|------|----:|
| Authorized | 11.00 |
| Supported estimated incurred | **5.25** |
| Outstanding reservations | **0.00** |
| Unresolved accounting hold | **1.25** |
| Safely available | **4.50** |
| Actual billed | **UNKNOWN** |
| Historical pilot (separate, closed) | 2.00 / remaining 0.00 |

Details: `TRANSACTION_RECONCILIATION.md`.
