# Brand AI Visibility — Phase 3A.9 Bilingual Prompt Governance

> **Status:** PASS · 2026-08-14  
> **Prior:** Phase 3A.8 Showcase Eligibility Hardening  
> **BUILD STATUS:** `BRAND_AI_VISIBILITY_PHASE_3A9_BILINGUAL_PROMPT_GOVERNANCE_PASS`  
> **Hard stop honored:** 0 provider calls · 0 Brand Basics writes · 0 entitlements · 0 monitoring runs · 0 deploys  
> **Next:** `PHASE_3A10_SHOWCASE_MONITORING_DRY_RUN`

## What shipped

- **Eligibility terminology** standardized (Suitability → Eligibility where methodological; Sustainability preserved)
- **Six active showcase intents** (Mixed Use / New Build / broad Owner Economics deferred)
- **84 governed prompts** (60 EN + 24 ES) with peer v2, language, semantic pairs
- **Airtable:** Language + Semantic Pair ID fields created; 39 EN backfills; 84 new prompt rows
- **Eligibility config v1.1 → 1.2** — Branded Residences + Soft-Brand Affiliation Flexibility entries; legacy territories retained
- **Density:** 2 distinct owner framings per intent × 7 geo/language slots = **84 calls / period**

## Active intents

1. Conversion  
2. Collection / Soft Brand  
3. Lifestyle Positioning  
4. Upper-Upscale Positioning  
5. Branded Residences  
6. Soft-Brand Affiliation Flexibility  

## Deferred

- New Build (FUTURE_READY / partial footprint eligibility only)  
- Mixed Use (no governed Brand Basics field)  
- Broad Owner Economics / Flexibility (renamed/narrowed)

## Cost (historical OpenAI run costs)

- Per call: LOW $0.35 · EXPECTED ~$0.68 · HIGH ~$1.33  
- Wave (84): LOW ~$29 · EXPECTED ~$57 · HIGH ~$112  
- Biweekly → ~168 calls/month · EXPECTED ~$114/month  

## Key paths

- `fixtures/ai-visibility/phase3a9-showcase-prompt-seed.json`  
- `fixtures/ai-visibility/brand-decision-eligibility-v1.json` (v1.2)  
- `lib/ai-visibility/showcase-intents.js`  
- `lib/ai-visibility/eligibility-terminology.js`  
- `npm run test:ai-visibility-phase3a9`
