# Full GDI E2E closeout — answers A–H

## A. What now executes when I request Full GDI E2E?

`scripts/gdi-research-to-page-workflow-v1.mjs --mode full-e2e` runs the existing coordinator stages in `FULL_E2E_COORDINATOR_STAGES` (validate → reuse → discovery → paid-MS → fit → people → contacts → **buyer_contact_enrichment** → hotel relevance → eligibility → cards → manifest → preview → corporate/PE/publish when authorized → **local_delivery_verify** → **production_delivery_verify** → **reconcile_run** → **evaluate_completion**). Completion status is **computed** by `evaluateFullE2eCompletion` and written to `full_e2e/full-e2e-completion.json`. Weekly refresh is **not** included in Full E2E stage order.

## B. What prevents another association-only run from being called complete?

The evaluator returns `PARTIAL` (never `COMPLETE`) when applicable segments remain `plan_only` / incomplete, or when `publication.associationOnly` coexists with incomplete applicable research. Regression: `COMPLETION_EVALUATION_ASSOC_ONLY_PARTIAL.json` + test case 1.

## C. How is research depth measured without forcing card quotas?

Evidence-based `DEPTH_INADEQUACY_RULES` and per-segment `coverageStatus` / `reuseSatisfiesScope` / `depthFailures` / `researchedZeroYield`. Briefs and query plans are not execution. Zero qualified prospects can pass; thin/irrelevant provider results cannot.

## D. Are contact depth, Airtable persistence, reports, links, local, and production required?

Yes for overall `COMPLETE`: contact-depth stage on the path; qualified persistence with read-back; required deliverables (or recorded gaps); **both** local and production verification passed against current artifacts. Airtable write alone or local-only proof cannot `COMPLETE`.

## E. What happens when budget or access blocks a stage?

Completed work is preserved; overall status becomes `PARTIAL` or `BLOCKED` with explicit reasons. Forbidden `NOT_APPLICABLE` rationales (e.g. budget exhausted) fail closed. Auth-blocked browser checks must be `blocked`/`not_run`.

## F. How does this differ from weekly refresh?

Weekly refresh is a distinct incremental mode (`gdi-weekly-refresh-coordinator-v1.mjs` / `--stage weekly_refresh`) for due checks, material changes, contact gaps, and coverage backlog. It uses the same quality/persistence rules but **must not** imply full initial market coverage `COMPLETE`.

## G. Which parts remain manual or unimplemented?

Evidence overlay for segment coverage and delivery proofs; operator-recorded local/production verification; share mint via existing paths; PDF layout confirmation where generated; fuller automatic scorecard→evaluator bridging. See `KNOWN_IMPLEMENTATION_GAPS.md`. No new paid batch or hotel activation in this task.

## H. What exact command should we use for the next hotel?

```bash
node scripts/gdi-research-to-page-workflow-v1.mjs \
  --hotel <canonicalHotelId> \
  --mode full-e2e \
  --authorization-ref "<authorization ledger / ticket id>" \
  --budget-usd <N> \
  --authorize-paid-discovery \
  --out-dir reports/gdi-full-e2e-contract
```

Preflight first:

```bash
node scripts/gdi-research-to-page-workflow-v1.mjs \
  --hotel <canonicalHotelId> \
  --mode full-e2e \
  --dry-run \
  --authorization-ref "<authorization>" \
  --out-dir reports/gdi-full-e2e-contract
```

Do not start this against an active Renaissance/Tampa/ADP owner lock; use `gdi-full-e2e-status-v1.mjs --run-dir …` for read-only evaluation of existing artifacts.
