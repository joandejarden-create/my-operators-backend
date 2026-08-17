# Brand AI Visibility — Monitoring Runbook (MANUAL_GOVERNED)

**Scheduler remains OFF.** Do not claim automated / biweekly / continuous monitoring.

## Recommended first NEW monitoring cohort (bounded)

| Field | Value |
|-------|--------|
| PORTFOLIO | Marriott (operationally cleaner) |
| HERO_BRAND | Autograph Collection (`recEJCTDj1zrsjPM6`) |
| GEOGRAPHY | CALA |
| LANGUAGE | en |
| PROVIDERS | openai → gemini → perplexity → claude (or Claude last per recurring order) |
| PROMPT_COHORT | `peers_uu_collection_lifestyle_owner_decision_v2` |
| EXPECTED_PROMPT_COUNT | **12** (CALA EN slot) |
| EXPECTED_PROVIDER_CALLS | 12 per provider |
| EXPECTED_TOTAL_CALLS | **48** (12 × 4) |
| EXPECTED_COST | ~**$15** (scaled from baseline actuals; Claude-heavy) |
| COST_CAP | **$40** hard stop for this bounded cohort (founder may raise) |

Do **not** start a full 84-call Wave-1 multi-slot wave for first pilot day unless cost + staffing approved.

## Actual system commands (existing only)

### Credential / live-env preflight (no secrets printed)

```bash
npm run ai-visibility:phase3b2-live-env
npm run ai-visibility:phase3a11-live-env
npm run ai-visibility:phase3b4-live-env
npm run ai-visibility:phase3b5-live-env
```

### Provider baseline / wave execution (LIVE — only when founder-approved)

```bash
# OpenAI wave-1 style execute (full plan — expensive; prefer bounded ops review first)
npm run ai-visibility:phase3a11-wave1-execute

# Provider baseline complete / inventory / execute paths
npm run ai-visibility:phase3b4-complete-baseline
npm run ai-visibility:phase3b5-inventory
npm run ai-visibility:phase3b5-execute
```

### Discoverability (not AI provider calls)

```bash
npm run ai-visibility:phase3c2-preflight
npm run ai-visibility:phase3c2-baseline -- --brand-ids=<recIds>
```

### Dry-run only (safe)

```bash
npm run ai-visibility:phase3a10-dry-run
npm run ai-visibility:phase3b6-dry-run
```

## Operator sequence

1. **Initiate** — confirm cohort table above; capture pre-run snapshot IDs.
2. **Confirm batch / wave ID** — from orchestrator preflight output (never invent IDs).
3. **Provider preflight** — credentials + model; if Claude not ready → document PARTIAL; do not swap.
4. **Set cost cap** — bounded $40 (or approved value); refuse run if cap missing.
5. **Execute provider wave** — one provider at a time; prefer order perplexity → gemini → openai → claude for cost isolation when using recurring config.
6. **Monitor completion** — checkpoint files under provider store `checkpoints/`.
7. **Retry eligible failures** — per `PROVIDER_RETRY_POLICIES` (max 2 attempts); no silent provider swap.
8. **Stop on hard cost cap** — status `STOPPED_COST_CAP` / `partial_cost_cap`; leave incomplete providers Not Monitored.
9. **Resolve partial state** — UI must show `N of 4` + partial banner; metrics from completed providers only.
10. **Finalize batch** — write batch summary + evidence; no Airtable execution writes for response corpus.
11. **Validation** — [post-run-validation-checklist.md](./post-run-validation-checklist.md).
12. **Publication gate** — [publication-gate.md](./publication-gate.md) before client visibility.

## Failure policy (runtime)

| Event | Behavior |
|-------|----------|
| One provider fails | Mark provider FAILED / Not Monitored; continue others; never substitute |
| Timeout | Retry per policy; then fail that call/provider path |
| Partial responses | Keep successful observations; recompute Present/Missing from recovered set |
| Cost cap | Stop new calls; `STOPPED_COST_CAP`; partial publish only if gate allows |
| Citations absent | Valid provider variation; Citation Coverage may be <100%; not a loader failure |
| Evidence write fails | Do not publish; escalate P0/P1; retain prior client view |
| Partial batch | Client banner: “Partial monitoring results — N of 4 providers completed.” |

## Lifecycle mapping

| Conceptual | Actual (`PERIOD_STATUS` / batch) |
|------------|----------------------------------|
| DRAFT / PREFLIGHT_READY | Preflight scripts + `PLANNED` |
| RUNNING | `RUNNING` |
| PARTIAL | `PARTIAL` |
| COMPLETED | `COMPLETED` |
| FAILED | `FAILED` |
| ABORTED_COST_CAP | `STOPPED_COST_CAP` |
| VALIDATION_REQUIRED | Post-run validation + validation manifest |
| APPROVED_FOR_PUBLICATION | `isBatchClientPublishable` / founder approval |
| PUBLISHED | Client read store points at validated federated baseline |
