# GDI pre-batch readiness decision

**Decision date:** 2026-10-16 (updated 10:20Z — $2.65 gap closeout)  
**Workroot:** `C:\Dev\dealality\my-operators-backend`  
**Code baseline:** `origin/main` @ `248a3098` (PR #108 merged + deployed SUCCESS)  
**Docs closeout:** PR #109 (budget gap + verification wording)

Six-hotel batch: **not started** (queued only). Renaissance: **not activated**.

---

## A. Is the process ready for one additional hotel E2E?

**Yes — conditionally**, after explicit founder activation of Renaissance-only (separate paid authorization). PR #108 score/dry-run/ledger safeguards are deployed; do not reopen without regression evidence.

Authenticated AI Admin interaction remains **NOT RUN** — admin-operations gap, not a research/publication blocker for one hotel E2E.

---

## B. What was newly fixed versus already completed?

### Already completed (accepted; not reapplied)

| Track | Evidence |
|-------|----------|
| A. Qualification/publication repair | PRs #101–#103 / b571d945 lineage |
| B. AI Admin More-menu portal | PR #104 @ `8148eed9` |
| C. Contact-depth | PR #99 closed; superseded by #106 + #107 |
| D. Score precedence / dry-run isolation / ledger fail-closed | PR #108 @ `248a3098` **merged + deployed** |

### This closeout (docs + accounting metadata only)

1. Closed exact **$2.65** narrative gap vs reconstructed ledger ($3.85 / $7.15).  
2. Transaction table: `TRANSACTION_RECONCILIATION.md`.  
3. Replaced stale “Filled after merge/deploy” with layered verification evidence.  
4. **No** new spend entries; **no** invented charges/credits; **no** paid calls.

---

## C. What was actually verified locally and in production?

Distinguish layers:

| Layer | Status |
|-------|--------|
| Git merge (PR #108) | Verified — `248a3098` |
| Deployment record | Verified — SUCCESS `2026-10-16T07-29-42-437Z-production.json` |
| Local/API | Verified — `LOCAL_PRODUCTION_VERIFICATION.json` (nav aligned; inventory read-only baseline) |
| Authenticated browser AI Admin interactive | **NOT RUN** |

Focused regressions for PR #108 (score / dry-run / fail-closed): PASS in closeout artifacts. Inventory facing baselines observational only (Mayflower 38 / SD 51).

---

## D. Are contact depth, qualification, fit, complete drawers, and persistence standard stages?

**Yes.** Shared stages on the coordinator / weekly downstream path remain the standard path for active hotels. No bespoke hotel packages required for those stages on Mayflower / SD.

---

## E. Does authenticated AI Admin interaction pass?

**NOT RUN.** No legitimate Memberstack admin session was used in this closeout. Deploy smoke and nav-alignment are **not** interactive menu verification.

---

## F. Budget (authoritative)

| Item | USD |
|------|----:|
| Authorized ($11 Mayflower/SD) | 11.00 |
| Supported estimated incurred | **3.85** |
| Outstanding reservations | **0.00** |
| Unresolved exposure beyond ledger | **0.00** |
| Safely available | **7.15** |
| Actual billed | **UNKNOWN** |
| Historical pilot (separate) | 2.00 |

$2.65 gap = duplicate stacking of Surfe $1.25 + three Parallel $0.35 already in $3.85 + one Parallel $0.35 **NOT_EXECUTED**. Details: `BUDGET_RECONCILIATION.json`, `TRANSACTION_RECONCILIATION.md`.

Do **not** transfer Mayflower/SD funds into the proposed six-hotel budget.

---

## G. Which hotel should run first (when activated)?

**Renaissance New York Times Square Hotel** — `recG66DQJKP2c0UNh`

Prep available (do not duplicate): `reports/gdi-six-hotel-e2e/RENAISSANCE_ACTIVATION_PLAN.md`, `BASELINE_RECONCILIATION.md`.

---

## H. Remaining issues by capability

| Issue | Class |
|-------|--------|
| Authenticated AI Admin interaction | **Admin-operations** — NOT RUN |
| Renaissance / six-hotel paid E2E | **Not started** — needs explicit activation + separate paid auth |
| Four queued hotels missing configs | **Research blocker for those hotels only** |
| Actual Surfe/Parallel invoices | **UNKNOWN** — estimates remain estimates |
| Mayflower/SD → six-hotel fund transfer | **Prohibited** |

**Do not label the entire system blocked.** Research/publication path for one additional hotel E2E is ready pending explicit activation under a separate budget.

---

## Inventory reconciliation (observational)

| Hotel | Facing | Notes |
|-------|--------|-------|
| Mayflower `rec0eTEnCqlsGJJt9` | 38 | Observational baseline; not rewritten |
| Wyndham SD Bayside `recnPB3Q02NO8Ma1o` | 51 | Observational baseline; not rewritten |

Nine restored + thirteen LMC/planner facing preserved. Opportunities, contacts, feedback, publication metadata, and historical $2 pilot ledger preserved.
