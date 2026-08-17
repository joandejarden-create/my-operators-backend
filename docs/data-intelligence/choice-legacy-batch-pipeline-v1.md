# Choice Legacy Batch Pipeline v1

**Date:** 2026-07-07  
**Status:** Implemented — dry-run default  
**Scope:** End-to-end Choice legacy governance upgrade for named batches (`mini-batch-1`, `mini-batch-2`).

> **Authority:** [choice-legacy-batch-config.js](../../lib/partner-intelligence/choice-legacy-batch-config.js), individual stage v1 docs under `docs/data-intelligence/choice-legacy-batch-*`

---

## Purpose

Mini-Batch 1 and Mini-Batch 2 proved the full pipeline, but required **7+ separate npm commands** per batch run. This orchestrator coordinates the existing batch modules into **one command** with safety gates preserved.

---

## Commands

```bash
# Dry-run (default) — plans all stages, no Airtable writes
npm run choice-legacy-batch-pipeline -- --batch mini-batch-2 --dry-run

# Apply — runs only pending safe stages in order
npm run choice-legacy-batch-pipeline -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-pipeline

# Per-brand fallback
npm run choice-legacy-batch-pipeline -- --batch mini-batch-2 --dry-run --brand radisson-choice
```

Reports: `reports/choice-legacy-batch-pipeline.{md,json}`

---

## Pipeline stages (in order)

| Stage | Module reused | Apply behavior |
|-------|---------------|----------------|
| A. Source package / local PDF | `choice-legacy-brand-source-package-batch` | Register matched local PDFs only; not auto-approved |
| B. URL capture | `choice-legacy-batch-url-capture` | Consumer + press only; skip duplicates |
| C. Source stewardship | `choice-legacy-batch-source-stewardship` | Approve Choice-controlled sources |
| D. Extraction | `choice-legacy-batch-extract` | Pending facts only |
| E. Fact stewardship | `choice-legacy-batch-fact-stewardship` | Approve clean facts; hold weak ones |
| F. Governance publish | `choice-legacy-batch-governance-publish` | Company Published posture only |
| G. Verification | `active-brand-governance-upgrade` logic | Read-only; Platform Ready equivalence |

Completed stages are **skipped**. Platform Ready brands are **no-op**.

---

## Stage detection

Per brand, the orchestrator reports one of:

- Source Package Needed
- URL Capture Needed
- Source Stewardship Needed
- Extraction Needed
- Fact Stewardship Needed
- Governance Publish Needed
- Verification Needed
- Platform Ready
- Blocked

Split-out / block only on real issues: missing package, duplicate conflict, RHG contamination, weak extraction, no approved facts, governance downgrade, Source-Informed misclassification, Company Validated conflict.

---

## Does not do

- Rebuild Brand Explorer content
- Overwrite Brand Setup content fields
- Auto-approve unsafe facts
- Set Company Validated or Company Validation Date
- Weaken RHG/global safeguards
- Change UI, scoring, BAS, OAS, OCS, Deal Readiness, or schema

---

## Manual step reduction

**Before:** 7 apply commands + 2 verification commands = **9 npm runs** per batch.

**After:** 1 dry-run + 1 apply + optional verification = **2–3 npm runs** per batch.

Individual stage commands remain available as fallbacks.

---

## Regression

After pipeline changes:

```bash
npm run choice-legacy-batch-pipeline -- --batch mini-batch-1 --dry-run
npm run choice-legacy-batch-pipeline -- --batch mini-batch-2 --dry-run
npm run active-brand-governance-upgrade -- --dry-run
npm run audit-partner-intelligence-publish-readiness
npm run test:partner-intelligence-stewardship-package
npm run test:partner-intelligence-publish-readiness
npm run test:partner-intelligence-profile-governance-publish
npm run test:intelligence-production-queue
```
