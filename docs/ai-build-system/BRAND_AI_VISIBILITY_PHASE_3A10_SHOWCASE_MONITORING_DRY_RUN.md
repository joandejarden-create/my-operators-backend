# Brand AI Visibility — Phase 3A.10 Showcase Monitoring Dry Run

> **Status:** PASS · 2026-08-14  
> **Prior:** Phase 3A.9.1 IHG Showcase Inclusion  
> **BUILD STATUS:** `BRAND_AI_VISIBILITY_PHASE_3A10_SHOWCASE_MONITORING_DRY_RUN_PASS`  
> **Hard stop:** 0 provider calls · peer v2 unchanged · 84 prompts unchanged  
> **Next:** `PHASE_3A11_LIVE_OPENAI_SHOWCASE_WAVE` (Founder-approved)

## Validated

- 84 prompts · 60 EN · 24 ES · 12×7 matrix · 14×6 intents · 24 semantic pairs  
- Peer v2 = 15 · fingerprint locked · Wave-1 baseline series id  
- OpenAI request buildable 84/0 failures (no network)  
- Fingerprints unique (84) · company perspectives do not multiply calls  
- Wave-1 store namespace: `data/ai-visibility/runtime/wave1-showcase`  
- Cost hard-cap recommendation: **$125** (historical HIGH ~$112 + buffer)  
- Retry budget: 1 retry/call · max 168 attempts  

## Key modules

- `lib/ai-visibility/wave1-showcase-plan.js`  
- `lib/ai-visibility/providers/normalized-response.js`  
- `buildOpenAiVisibilityRequest` (no-call)  
- `npm run ai-visibility:phase3a10-dry-run`  
- `npm run test:ai-visibility-phase3a10`

## Live command

Deferred — see dry-run report `LIVE_COMMAND_PREVIEW`. Do not run until Phase 3A.11 Founder approval.
