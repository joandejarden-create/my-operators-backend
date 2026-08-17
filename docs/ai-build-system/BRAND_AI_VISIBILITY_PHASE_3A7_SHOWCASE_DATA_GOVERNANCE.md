# Brand AI Visibility — Phase 3A.7 Showcase Data Governance

> **Status:** PASS · 2026-08-14  
> **Prior:** Phase 3A.5 design audit · Phase 3A.6 language foundation  
> **Hard stop honored:** 0 provider calls · 0 Spanish prompts · 0 production client writes · 0 deploys  
> **BUILD STATUS:** `BRAND_AI_VISIBILITY_PHASE_3A7_SHOWCASE_DATA_GOVERNANCE_PASS`  
> **Next:** `PHASE_3A8_BILINGUAL_PROMPT_GOVERNANCE`

## Applied (code/config only)

- `peers_uu_collection_lifestyle_owner_decision_v2` (15 brands) — v1 preserved
- `brand_ai_showcase_companies_v1` — Marriott / Hilton / Choice portfolios
- `brand_decision_eligibility_v1` — 15 × 7 deterministic matrix
- Parent normalize helper; archetypes; geography eligibility (UNKNOWN default)
- Entitlement dry-run report only (`PRODUCTION_CLIENT_ENTITLEMENT_WRITES: 0`)

## Key decisions

- ONE shared cohort + intent eligibility (not nested peer sets)
- Westin + Radisson Blu remain in cohort; NOT_ELIGIBLE for Collection / Soft Brand
- Radisson RED in Choice portfolio only — not in peer v2
- Parent display aliases normalized in methodology only (Airtable corrections need Founder approval)
- Geography eligibility UNKNOWN (no Brand Basics geo-eligibility field)
- ELIGIBILITY_LANGUAGE_NEUTRAL: YES

## Tests

`npm run test:ai-visibility-phase3a7` (+ language foundation + phase2d–3a4 + demo workspace)
