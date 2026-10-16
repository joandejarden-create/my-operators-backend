# Six-hotel GDI batch — status summary

**Workroot:** `C:\Dev\dealality\my-operators-backend`  
**Main:** `2c58fef4`  
**Mayflower & San Diego:** untouched as active work · $11 ledger corrected (estimated incurred **$5.25** · hold **$1.25** · available **$4.50**)

## Authorization / execution snapshot

| Scope | Cap | Status |
|-------|----:|--------|
| Renaissance GDI | **$12** | **AUTHORIZED_NOT_STARTED** — `RENAISSANCE_ACTIVATION_PLAN.md` |
| Renaissance ADP | **$15** | **COMPLETE** — `reports/adp-six-hotel-e2e/renaissance/` |
| Other five (proposed GDI $30 / ADP $57) | — | **Not approved** · queued |
| Waterstone distribution | — | **HOLD** intact |

Authorization ≠ execution. Do not re-ask founder “go” for the already-authorized Renaissance GDI $12 or ADP $15 scopes. Do not combine GDI/ADP ledgers or use Mayflower/SD funds.

## Artifacts

| Artifact | Path |
|----------|------|
| Identity resolution | `HOTEL_IDENTITY_RESOLUTION.md` + register JSON |
| Baseline by ID | `BASELINE_RECONCILIATION.md` / `.csv` / `.json` |
| Staged budget proposal | `PAID_RESEARCH_BUDGET_PROPOSAL.md` (Renaissance $12 authorized; remaining $30 not approved) |
| Renaissance GDI package | `RENAISSANCE_ACTIVATION_PLAN.md` |
| Activation gate | `ACTIVATION_GATE.md` |
| Mayflower/SD tx reconciliation | `reports/gdi-pre-batch-readiness/TRANSACTION_RECONCILIATION.md` |

## Answers A–G (prep corrections retained)

### A. Are the four hotels absent, matched, ambiguous, or lookup-blocked?

All four **VERIFIED_ABSENT** (Airtable access OK; search complete; false positives excluded): WASFP, WASPY, CLTCC, TPAAP.

### B. Do all Renaissance/Waterstone records reconcile?

**Yes.** 29/29 unique IDs each; Airtable ↔ local intersection complete.

### C. Why do historical and current facing counts differ?

Historical ~**8** = legacy/ready **surface** filter. Current **2** (Ren) / **3** (Waterstone) = **HOTEL_FACING** eligibility.

### D. What is the Renaissance GDI cap?

**$12.00** — **authorized**. Not drawn from Mayflower/SD $11. Separate from ADP $15.

### E. What remains for the other five?

Proposed **$30.00** GDI / **$57** ADP — **not approved**.

### F. Are readiness fixes deployed?

**Yes** (PR #108 lineage). Authenticated AI Admin interactive remains **NOT_RUN**.

### G. Exact status to execute Renaissance GDI?

Already **authorized**. Owning execution follows `RENAISSANCE_ACTIVATION_PLAN.md`. This accounting task does **not** own live GDI execution and must not duplicate paid runs.

## What this accounting sync did not do

No new paid requests · no duplicate Renaissance runs · no Mayflower/SD opportunity overwrites · no Waterstone HOLD release · no approval of the other five.
