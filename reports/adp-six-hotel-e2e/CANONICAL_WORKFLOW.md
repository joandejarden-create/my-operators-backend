# Canonical ADP workflow (six-hotel batch)

**Workroot:** `C:\Dev\dealality\my-operators-backend`  
**Code baseline:** `origin/main` `248a3098`  
**Mode:** Preparation only — no research, paid calls, or customer publication launched.

ADP and GDI share hotel identity (HPC `rec…`) but keep research, scoring, budgets, ledgers, and publication tables **distinct**. Do not write ADP outputs into GDI opportunity tables.

## Pipeline (existing modules — no parallel ADP)

| Stage | Location |
|-------|----------|
| Hotel config / intake | `fixtures/ai-demand-positioning/*-property-profile.json` · `lib/ai-demand-positioning/data-model.js` (`loadPropertyProfile`) |
| Census join | `fixtures/ai-demand-positioning/census-links-v1.json` · `lib/ai-demand-positioning/census-link-registry.js` |
| Scenario / persona universe | `lib/ai-demand-positioning/prompt-universe/scenario-registry.js` · `standard-scenarios.js` |
| Provider adapters | `lib/ai-demand-positioning/execution/multi-provider-runner.js` (openai, gemini, perplexity, claude) |
| Evidence parse / scoring | `execution/response-parser.js` · customer `owner-payload.js` · measurement contracts |
| Canonical persistence | `data-model.js` `savePeriod` · `published-snapshot.js` · optional Airtable via `airtable-published-report.js` when `ADP_AIRTABLE_PUBLISH_APPLY=true` |
| Narrative / actions | owner payload actions · monthly-review generators · action-intelligence |
| Customer page | owner ADP surfaces / monthly review HTML |
| PDF | admin `current-report-pdf` + monthly-review PDF paths |
| Share | `scripts/issue-adp-share-capability-v1.mjs` |
| AI Admin | `/admin/ai-demand` Reviews / Action Plan / current PDF APIs |
| Refresh / history | published manifests + `reports/ai-demand-positioning/monthly-review/archive/<propertyId>/` · competitive-history |

## Executable entry points (supported flags only)

### Shared monitoring coordinator

```bash
node scripts/run-ai-demand-positioning.mjs --property <adpPropertyId> --dry-run
node scripts/run-ai-demand-positioning.mjs --property <adpPropertyId> --apply
node scripts/run-ai-demand-positioning.mjs --property <adpPropertyId> --apply --providers openai,gemini,perplexity,claude
```

- Default without `--apply` is **dry-run** (structure only; no live provider calls claimed as coverage).  
- `--providers` optional comma list.  
- Live `--apply` parses, builds owner payload, saves period, writes published snapshot; Airtable upsert only if `ADP_AIRTABLE_PUBLISH_APPLY=true`.

### First official baseline pattern (per-hotel module when onboarded)

```bash
node scripts/run-adp-<hotel>-baseline-period-001.mjs --preflight-only
node scripts/run-adp-<hotel>-baseline-period-001.mjs --dry-run
node scripts/run-adp-<hotel>-baseline-period-001.mjs --apply --certify
```

Examples already in repo: W Rome, Hilton Times Square, YOTEL Geneva Lake, Westin Grand München, CALA six, etc. New hotels need an analogous execution module **after** profile + census link + governance readiness — do not invent flags.

### Share (after published snapshot exists)

```bash
node scripts/issue-adp-share-capability-v1.mjs --property <adpPropertyId>
```

Requires `ADP_SHARE_CAPABILITY_SECRET` (or explicit local-only allow).

### Not ADP

`scripts/gdi-research-to-page-workflow-v1.mjs` and GDI paid ledgers are **out of scope** for this batch.

## Provider / cost configuration (read from code)

| Provider | Model (config) | Est. $/call |
|----------|----------------|------------:|
| openai | gpt-4o | 0.03 |
| gemini | gemini-3.6-flash | 0.02 |
| perplexity | sonar | 0.05 |
| claude | claude-sonnet-4-6 | 0.03 |

`estimateCost(scenarioCount)` ≈ scenarioCount × sum(rates). Estimates ≠ billed; reconcile after runs.
