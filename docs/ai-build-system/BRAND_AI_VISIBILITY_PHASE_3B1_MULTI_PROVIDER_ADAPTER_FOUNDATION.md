# Brand AI Visibility — Phase 3B.1 Multi-Provider Adapter Foundation

> **Status:** COMPLETE (adapter foundation; no live provider calls)  
> **Date:** 2026-08-14  
> **Prior phase:** [3A.11 Live OpenAI Showcase Wave](./BRAND_AI_VISIBILITY_PHASE_3A11_LIVE_OPENAI_SHOWCASE_WAVE.md)

## Objective

Make Gemini, Perplexity, and Claude first-class execution providers behind the same governed Dealality monitoring methodology proven with OpenAI Wave-1 — without live calls, prompt changes, peer v2 changes, or metric formula changes.

## Deliverables

| Module | Purpose |
|--------|---------|
| `lib/ai-visibility/providers/gemini.js` | Gemini adapter (google_search grounding) |
| `lib/ai-visibility/providers/perplexity.js` | Perplexity Sonar adapter |
| `lib/ai-visibility/providers/claude.js` | Claude web_search adapter (bounded) |
| `lib/ai-visibility/providers/provider-interface.js` | Canonical adapter contract |
| `lib/ai-visibility/providers/normalized-citation.js` | Provider-neutral citation contract |
| `lib/ai-visibility/providers/normalized-response.js` | v1.1 additive contract extension |
| `lib/ai-visibility/providers/multi-provider-dry-run.js` | 252-request dry-run builder |
| `lib/ai-visibility/providers/validation-plan.js` | Controlled validation design |
| `lib/ai-visibility/providers/cross-provider-signals.js` | Future signal foundation (no calculation) |
| `lib/ai-visibility/providers/raw-text-repair.js` | Wave-1 rawText audit |
| `scripts/test-ai-visibility-phase3b1.mjs` | 21 tests |
| `scripts/ai-visibility-phase3b1-multi-provider-dry-run.mjs` | Report artifact generator |

## Canonical Provider IDs

`openai` · `gemini` · `perplexity` · `claude`

Display labels: OpenAI · Gemini · Perplexity · Claude

## Recommended Wave-1 Models (validation — not executed)

| Provider | Model / Mode |
|----------|----------------|
| OpenAI | `gpt-5.6` (existing baseline) |
| Gemini | `gemini-2.5-flash` + `google_search` |
| Perplexity | `sonar` (Sonar chat completions, NOT Agent API) |
| Claude | `claude-sonnet-4-6` + `web_search_20260209` (max_uses: 5) |

## Dry-Run Results

- **GEMINI_BUILDABLE:** 84  
- **PERPLEXITY_BUILDABLE:** 84  
- **CLAUDE_BUILDABLE:** 84  
- **TOTAL_BUILDABLE:** 252  
- **SEMANTIC_PROMPT_PARITY:** YES  
- **FINGERPRINT_ISOLATION:** VALID (no collisions across 4 providers)

## Controlled Validation Plan (Phase 3B.2 — not executed)

- **12 prompts × 3 providers = 36 calls**
- Prompt IDs: see `CONTROLLED_VALIDATION_PROMPT_IDS` in `validation-plan.js`
- Geo/language: Global EN, CALA EN/ES, Europe EN, Mexico EN/ES
- Cost estimate: `NEEDS_LIVE_USAGE_CALIBRATION` for all three new providers

## Full Four-Provider Baseline (future)

- OpenAI existing: 84  
- Incremental new: 252 (Gemini + Perplexity + Claude)  
- **Total observations:** 336  
- Do not rerun OpenAI unless methodology/model intentionally changes

## OpenAI Wave-1 rawText Repair

- **Flagged run:** `run_83fe4c10721f4d9b` / fingerprint `055e743962880bf527279eb8` / `p_global_design_local_character_v1`
- **RAW_TEXT_REPAIR_SAFE:** NO — no text in response, normalized, or raw artifact
- **APPLIED:** NO

## Hard Stop Compliance

- LIVE_OPENAI_CALLS: 0  
- LIVE_GEMINI_CALLS: 0  
- LIVE_PERPLEXITY_CALLS: 0  
- LIVE_CLAUDE_CALLS: 0  
- No prompt text changes · No peer v2 changes · No metric formula changes  
- No All AI · No provider blending · No UI redesign · No deploy

## Tests

```bash
npm run test:ai-visibility-phase3b1
npm run ai-visibility:phase3b1-multi-provider-dry-run
```

Regression: `test:ai-visibility-phase3a10`, `test:ai-visibility-phase3a11`

## Next Recommended Phase

**PHASE_3B2_CONTROLLED_MULTI_PROVIDER_VALIDATION**

Blockers before validation:
1. Configure `GEMINI_API_KEY`, `PERPLEXITY_API_KEY`, `ANTHROPIC_API_KEY`
2. Run 36-call controlled validation per `validation-plan.js`
3. Calibrate provider cost estimates from live usage

## Change Impact

**Medium** — new adapter modules, contract extensions, no live data or Airtable writes.

## Discoverability / Business Impact Handoff

`EARLY_DISCOVERABILITY_PHASE_RETAINED: YES` — after multi-provider data collection begins, move early into crawl/referral/business impact foundation (not implemented in 3B.1).
