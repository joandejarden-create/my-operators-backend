# Brand AI Visibility — Phase 3B.3 Multi-Provider Baseline Expansion

**Status:** Implementation complete · live execution via `npm run ai-visibility:phase3b3-execute`

## Scope

| Provider | Validation (12) | Baseline (84) | Notes |
|----------|-----------------|---------------|-------|
| OpenAI | — | **0 calls** | Wave-1 baseline untouched |
| Gemini | Yes | If GO | `gemini-2.5-flash` + Google Search grounding |
| Perplexity | — (3B.2 GO) | Yes | `$15` hard cap |
| Claude | If billing OK | If validation GO + cost cap | Billing probe first |

## Run purposes

- `validation` → `data/ai-visibility/runtime/provider-validation/{provider}/`
- `baseline` → `data/ai-visibility/runtime/provider-baselines/{provider}/`
- OpenAI Wave-1 → `data/ai-visibility/runtime/wave1-showcase/` (unchanged)

## Baseline series IDs

- `aiv_wave1_gemini_peer_v2_showcase_prompts_v1`
- `aiv_wave1_perplexity_peer_v2_showcase_prompts_v1`
- `aiv_wave1_claude_peer_v2_showcase_prompts_v1`

## Commands

```bash
npm run test:ai-visibility-phase3b3
node scripts/ai-visibility-phase3b3-live-env.mjs --verify-only
npm run ai-visibility:phase3b3-execute
```

## UI rule

Provider selector exposes only providers with `FULL_BASELINE` (84/84 completed, `monitoringRunPurpose: baseline`). Validation-only data never appears as measured.

## Constitution preserved

Provider-pure · no All AI · no blending · peer v2 · showcase_prompts_v1 · ai_visibility_metrics_v1 · provider failure ≠ brand absence.
