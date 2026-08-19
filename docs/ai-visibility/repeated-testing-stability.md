# Repeated testing and stability

> **Status:** Stage B controlled validation approved at **$30** hard cap. Isolated store only. **Do not deploy.** Scheduler OFF. DataForSEO 0. Full 133-prompt run 0.

## Why this exists

A single AI response is an observation. Repeated observations help distinguish one-off results from recurring or unstable patterns. Repeated testing does **not** prove model probability, causality, future behavior, or universal recommendation behavior.

## Methodology (client-safe)

AI responses can vary between runs. Dealality uses repeated observations on priority owner-decision prompts to distinguish recurring patterns from one-off results. We report the number and consistency of observed responses rather than assigning artificial confidence scores.

Forbidden: “high confidence,” “95% reliable,” “statistically significant,” “model certainty,” numeric stability/reliability scores.

## Grain

Preferred: **prompt × brand × provider × language × geography**, plus **repeat type** (`EXACT_REPEAT` vs `CONTROLLED_VARIANT`). Do not pool unlike observations merely to increase N. All Providers alignment is a separate contextual layer.

## States

**Recurrence:** `INSUFFICIENT_OBSERVATIONS` · `ONE_OFF` · `EARLY_REPEATED_EVIDENCE` · `RECURRENT` · `INFREQUENT`

**Stability** (eligible at N ≥ 3): `CONSISTENTLY_PRESENT` · `CONSISTENTLY_ABSENT` · `MIXED` · `CHANGING`

**Cross-provider:** `ALIGNED_PRESENT` · `ALIGNED_ABSENT` · `PARTIAL_ALIGNMENT` · `HIGH_VARIABILITY` · `INSUFFICIENT_PROVIDER_COVERAGE`

N = 3 is eligible for a descriptive label. It is not “high confidence.”

**Time windows:** `SAME_RUN_REPETITION` (< 1 day) · `SHORT_TERM` (< 14 days) · `LONGITUDINAL` (≥ 14 days). Three tests in ten minutes are not equivalent to three tests over six weeks.

Stability (agreement among observations) is not trend (change over time).

## Sampling (no numeric score)

| Priority | Examples |
|----------|----------|
| CRITICAL | Scenario `commercialPriority=CRITICAL`; executive-finding prompts; prior MIXED/CHANGING/HIGH_VARIABILITY; client-selected |
| HIGH | Important observed demand + commercial relevance; Autograph / Design Hotels / Westin context; Mexico Spanish observed franchise |
| STANDARD | Stable mapped scenario; lower-importance PAA-only observed theme |
| EXPLORATORY | DERIVED unless it solves a test-quality problem vs the observed parent |

Observed demand does **not** automatically make a prompt the most frequently tested.

## Validation

16-prompt cohort. Cost model uses per-provider baseline actuals (84 calls each). Conservative OpenAI uses Wave-1 HIGH ($1.33). Stage B founder-approved execution hard cap is **$30** (supersedes the prior $75 design cap). Isolated store: `data/ai-visibility/runtime/stability-stage-b`. Do not federate into live Brand AI reads until separately approved.

## Authoritative stability input (binding)

For all repeated-testing / stability context — including **Narrative & Source Intelligence v1** — use **only**:

| Field | Value |
|-------|-------|
| Report | `reports/ai-visibility/repeated-testing-stage-b-report-final-wave.json` |
| Approved wave | `aiv_stability_stage_b_20260817_f0a829` |
| Stage B evidence count | 31 exact repeats |

**Do not** aggregate or infer recurrence from `aiv_stability_stage_b_20260817_22c195`. That earlier wave may remain in the isolated store for audit, but it is **non-authoritative** for stability grains, executive support metadata, or downstream narrative/source evidence.

The combined `repeated-testing-stage-b-report.json` (all waves in session) is superseded and must not be used for downstream reads.

Code constants: `STAGE_B_AUTHORITATIVE_WAVE_ID`, `STAGE_B_NON_AUTHORITATIVE_WAVE_IDS`, `STAGE_B_AUTHORITATIVE_REPORT_REL_PATH` in `lib/ai-visibility/stability-policy.js`.

## Code

| Module | Role |
|--------|------|
| `lib/ai-visibility/stability-aggregation.js` | Grain, states, aggregator |
| `lib/ai-visibility/stability-policy.js` | Sampling, cohort, cost |
| `lib/ai-visibility/stability-historical-audit.js` | Read-only store audit |
| `lib/ai-visibility/stability-client.js` | Client copy |
| `lib/ai-visibility/stability-stage-b-orchestrator.js` | Isolated Stage B runner ($30 cap, 31 exact repeats) |

```bash
npm run test:ai-visibility-stability
npm run ai-visibility:stability-historical-audit
npm run ai-visibility:stability-stage-b-preflight
# founder-approved paid run only:
npm run ai-visibility:stability-stage-b-execute
```

P0C raw gap existence and Truth fact values are unchanged. Stability metadata may attach later for presentation only.
