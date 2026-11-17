# GDI Full E2E Execution Integrity P1-A/B — Founder Report

**Contract:** `gdi-full-e2e-execution-integrity-p1ab-v1`  
**Audit source:** PR #345 / FND-001, FND-002, FND-003  
**Paid research:** NONE  
**Publication changes:** NONE

## Root causes corrected

1. **Incomplete segment ledger (FND-002):** Canonical `executeFourteenSegmentDiscoveryMatrix` did not create durable execution-observability runs or emit per-segment terminal states into the PR #336 ledger.
2. **Apify overclaim (FND-003):** Provider inventory labeled Apify `WIRED_DEFAULT_MOCK_FAIL_CLOSED` while the fourteen-segment path never imported the Apify bridge — configured ≠ orchestrated.
3. **Silent configured-vs-executed confusion:** Optional Apify skips could have looked like successful execution without JOB_SUBMITTED evidence.

## Scope implemented

| Item | Change |
|------|--------|
| P1-A | Bridge `gdi-fourteen-segment-execution-ledger-bridge-v1.js` + matrix instrumentation (default on) |
| P1-B | Inventory corrected to `CONFIGURED_NOT_ORCHESTRATED_IN_FULL_E2E`; honest `APIFY_NOT_ORCHESTRATED` skips on Apify-primary segments; optional mock orchestration behind `enableApifyOrchestration` |
| Tests | `npm run test:gdi-full-e2e-execution-integrity-p1ab-v1` (18/18) |
| Safety | No live Exa/Apify/Parallel/Surfe; no Airtable writes; no CURRENT package mutation |

## Is canonical Full E2E now instrumented?

**Yes — for the fourteen-segment discovery matrix path** (`executeFourteenSegmentDiscoveryMatrix`), which is what `original-full-e2e-research-chain-v1` invokes for Stage 2 discovery.

Each hotel run now:

1. Creates a durable run under `data/group-demand-intelligence/execution-observability/{hotelId}/` (or a test root).
2. Marks all planned segments STARTED → terminal (COMPLETED / SKIPPED / FAILED / BLOCKED_*).
3. Records provider evidence (fixture / historical cache / live) distinctly.
4. Records Apify honesty for Apify-preferred segments.
5. Finalizes an execution certificate via existing `certifyExecutionRun`.

Campaign-specific scripts that bypass the matrix remain **uninstrumented** (follow-up P1-C).

## Segment state behavior

- Handler entry alone ≠ EXECUTED.
- Fixture / historical / live paths produce COMPLETED with accurate provider IDs and `cacheHit` where appropriate.
- Zero candidates may still COMPLETE when the configured strategy finished.
- No provider/cache/fixture evidence → SKIPPED (`NO_PROVIDER_OR_CACHE_INVOCATION`).
- Historical UNKNOWN hist_* ledgers remain UNKNOWN (not rewritten).

## Apify state behavior

| Mode | Behavior |
|------|----------|
| Default Full E2E | `APIFY_CONFIGURED` → `APIFY_SKIPPED` / provider SKIPPED with `APIFY_NOT_ORCHESTRATED` |
| `apifyDisabled` | `APIFY_DISABLED` |
| `enableApifyOrchestration` + mock | Existing bridge runs mock-only; ledger shows CACHE_HIT / mock — not live JOB without auth |
| Live | Requires `enableApifyOrchestration` + `authorizeApifyLive` + existing env gate; **not enabled by this PR** |

## Remaining gaps

- P1-C: single entry wrapper so all hotel campaigns use this path.
- Live Apify commercial value still unproven (no paid run authorized).
- Deep-research stage ledger density beyond discovery Stage A remains partial (existing stage timeline reused; not expanded here).
- FND-012 confidentiality API audit still open.

## Test results

- `test:gdi-full-e2e-execution-integrity-p1ab-v1` → **18/18 PASS**
- `test:gdi-full-e2e-execution-observability-v1` → **17/17 PASS** (regression)

## Unverified live behavior

Live Exa/Apify execution evidence on a real hotel campaign was **not** exercised (by design — no paid authorization). Deterministic fixture certification proves ledger completeness and Apify honesty only.
