# Exact supported execution commands (post-activation)

**Do not run live `--apply` until founder activates ADP and approves budget.**  
All commands from `origin/main` canonical ADP path — no invented flags.

## 0. Preflight / unpaid (safe now; optional)

```bash
# Scenario + cost structure only — default dry-run
node scripts/run-ai-demand-positioning.mjs --property adp_renaissance_times_square --dry-run

node scripts/run-ai-demand-positioning.mjs --property adp_waterstone_boca_raton --dry-run
```

Dry-run does **not** constitute provider coverage.

## 1. First hotel — Renaissance (when ACTIVATE + $15 approved)

```bash
# Live multi-provider refresh
node scripts/run-ai-demand-positioning.mjs --property adp_renaissance_times_square --apply

# Optional provider subset (if authorized / keys missing)
node scripts/run-ai-demand-positioning.mjs --property adp_renaissance_times_square --apply --providers openai,gemini,perplexity,claude
```

Then (as supported by existing ops scripts / admin APIs — only if publish scope authorized):

```bash
# Airtable Live upsert only when env explicitly set
# ADP_AIRTABLE_PUBLISH_APPLY=true  (set in shell for that invocation only)

# Share token (requires ADP_SHARE_CAPABILITY_SECRET)
node scripts/issue-adp-share-capability-v1.mjs --property adp_renaissance_times_square
```

PDF / monthly-review generation: use existing AI Admin monthly-review generate + `current-report-pdf` endpoints — do not invent CLI flags that are not in-repo.

**Unsupported:** claiming GDI coordinator flags implement ADP; inventing `--adp-six-hotel-batch`; bypassing HOLD on Waterstone without documented override.

## 2. Waterstone (when separately authorized)

```bash
node scripts/run-ai-demand-positioning.mjs --property adp_waterstone_boca_raton --dry-run
node scripts/run-ai-demand-positioning.mjs --property adp_waterstone_boca_raton --apply
```

Customer-facing re-issue / new share push: **blocked** while `externalDistributionHold.status=HOLD` unless founder clears.

## 3. Four Marriott hotels (after HPC steward + ADP profile + census link)

1. Steward HPC create (GDI identity payloads) — separate authorization.  
2. Add `fixtures/ai-demand-positioning/<slug>-property-profile.json`.  
3. Link in `fixtures/ai-demand-positioning/census-links-v1.json`.  
4. Prefer hotel-specific `run-adp-<hotel>-baseline-period-001.mjs --preflight-only` once module exists (pattern: Hilton TS / W Rome), else:

```bash
node scripts/run-ai-demand-positioning.mjs --property <newAdpPropertyId> --dry-run
node scripts/run-ai-demand-positioning.mjs --property <newAdpPropertyId> --apply
```

## 4. Verification checklist (after activation delivery)

- Local `:8080` build SHA matches intended release.  
- Production Railway serves released build.  
- AI Admin opens correct hotel/report.  
- Share works with legitimate token; cross-hotel scoped.  
- Page / action plan / PDF version consistency.  
- GDI pages unaffected.  
- Do not claim production delivery from Airtable write alone.

## Preserve

Mayflower / San Diego GDI work · unrelated branches · stashes · customer ADP action-plan inputs.
