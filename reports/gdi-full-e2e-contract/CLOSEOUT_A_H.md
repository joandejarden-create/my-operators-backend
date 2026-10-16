# Full GDI E2E fail-closed correction — Founder A–H

**Contract version:** `gdi-full-e2e-contract-v1.1`  
**Branch:** `fix/gdi-full-e2e-fail-closed`  
**Prior PR:** #118 @ `9a576c8e`  
**Date:** 2026-10-16

---

## A. Does Full E2E execute the required stages, or which remain manual?

**Coordinator-executed (machine):** hotel resolve, reuse, discovery/paid-MS (when authorized), fit, contacts/buyer enrichment, synthesize, eligibility, cards, manifest, PE/corporate apply, publish (when authorized), **local API verify**, **production API verify**, **authoritative budget reconcile**, completion evaluate.

**Still manual / operator-evidence where access requires it:** authenticated Memberstack browser drawer walkthroughs, share-token mint confirmation (no token in git), PDF visual clipping QA when a PDF route runs, human quality review of sales narratives. Manual review must record reviewer, timestamp, scope, evidenceRef and **cannot** override failed machine checks.

---

## B. Can omitted segments or missing required deliverables still produce COMPLETE?

**No.** Fail-closed evaluator rejects:

- Empty coverage  
- Omitted / unknown / duplicate segment IDs  
- Unresolved applicability  
- gap/unsupported/missing/broken deliverables (even with `gapRecorded: true`)  
- Label `"researched"` or `reuseSatisfiesScope` boolean alone  
- Missing contact summary with a nonempty queue  
- Stages NOT_STARTED / BLOCKED / FAILED  
- Stale or wrong-hotel/run verification  

Proved by `scripts/test-gdi-full-e2e-contract-v1.mjs`.

---

## C. Does contact depth reconcile to the actual candidate queue?

**Yes when artifacts exist.** `buildCompletionInputFromRun` derives queue + dispositions from `buyer_contact_enrichment` pass/queue JSON. Every required queue ID needs a valid disposition. Zero-default without a verified empty queue does **not** complete. Explicit `queueVerifiedEmpty` supports no-candidate runs.

---

## D. Are local/production checks executed and bound to current artifacts?

**Yes for API surfaces.** Stages call `runLocalDeliveryVerification` / `runProductionDeliveryVerification`, bind hotelId/runId/dataRevision/buildSha/artifactFingerprint, and store machine results. Resume re-runs verify when `verificationStatus` is not_run/failed/stale or fingerprints change. Operator “passed” cannot upgrade a failed machine result.

Authenticated browser checks remain separately recorded when required.

---

## E. Does resume correctly revisit unfinished/stale checks?

**Yes.** Resume skips completed paid/publish stages (no duplicate spend) but re-runs `local_delivery_verify`, `production_delivery_verify`, `reconcile_run`, and `evaluate_completion` when verification needs rerun per fingerprints / not_run status.

---

## F. What exact command runs authorized Full E2E?

```bash
# Preflight
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel <id> --mode full-e2e --dry-run

# Research without publication
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel <id> --mode full-e2e \
  --run-discovery --authorize-paid-discovery --budget-usd <N> \
  --authorization-ref <auth_id> --target-env local

# Full research + publication
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel <id> --mode full-e2e \
  --run-discovery --authorize-paid-discovery --authorize-surfe --authorize-publish \
  --budget-usd <N> --authorization-ref <auth_id> --target-env production

# Resume
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel <id> --mode full-e2e --resume-run <runId>

# Read-only status (does not write into the source run journal)
node scripts/gdi-full-e2e-status-v1.mjs --hotel <id> --run-dir <path> \
  --out-dir reports/gdi-full-e2e-contract
```

`--dry-run` overrides every paid/write flag. `--authorization-ref` is validated against the hotel/product ledger and does **not** mint allowance. Research auth ≠ publish auth.

---

## G. What does the current Renaissance evidence actually support?

Read-only evaluate of  
`gdi_workflow_recG66DQ_2026-10-16T10-26-54-849Z_80516d`  
under contract v1.1:

| Result | Value |
|--------|-------|
| overallStatus | **FAILED** / not COMPLETE (honest) |
| canClaimComplete | **false** |
| Axes | Expected 11; omitted `professional_planners_dmc`; plan-derived rows still `plan_only` vs continuation paid work not mapped into the original run ledger segmentCoverage |
| Deliverables | missing in ledger overlay |
| Stages | Several NOT_STARTED on the original ledger (Full E2E stages added after that run) |

**Support:** Multisegment **PARTIAL publication** from the separate continuation execution is real (see Renaissance exec ownership). The **original coordinator ledger alone** does **not** satisfy Full E2E COMPLETE under the corrected contract — correctly.

Journal was **not** mutated (status writes only under `reports/gdi-full-e2e-contract/`).

---

## H. Which SHA is deployed and what remains blocked?

| Item | Status |
|------|--------|
| Code fix PR | [#119](https://github.com/joandejarden-create/my-operators-backend/pull/119) merged @ `e6d9a609` |
| Lean ignore PR | [#120](https://github.com/joandejarden-create/my-operators-backend/pull/120) merged @ `acc380bf` |
| Deployed SHA | **`acc380bf`** (lean-stage upload; Railway deploy `8cd94aaf` SUCCESS; postdeploy smoke PASS) |
| Remains blocked | Full E2E COMPLETE for Renaissance until segmentCoverage artifacts, deliverable verification, and Full E2E stage ledger rows are produced by a governed Full E2E run (no new paid spend authorized by this task) |
| Railway 413 | Corrected: PDF-only GDI reports un-ignore + marketing CDN texture excludes + **lean-stage `railway up --path-as-root`** (direct CLI pack still ~340MB / 413). No dirty/hotfix deploy. |

---

## Remaining implementation gaps (honest)

1. Headless authenticated drawer walkthrough not automated.  
2. Automatic mapping of continuation scorecards into the **original** run’s segmentCoverage (cross-run join) is not done — use evidence overlay or a new Full E2E run.  
3. Share mint still uses existing share paths; tokens never committed.  
4. This task authorizes **no** new paid research and does not activate additional hotels.
