# Brand AI Visibility — Phase 3A.9.1 IHG Showcase Inclusion

> **Status:** PASS · 2026-08-14  
> **Prior:** Phase 3A.9 Bilingual Prompt Governance  
> **BUILD STATUS:** `BRAND_AI_VISIBILITY_PHASE_3A9_1_IHG_SHOWCASE_INCLUSION_PASS`  
> **Hard stop honored:** 0 provider calls · 0 Brand Basics writes · 0 entitlements · 0 monitoring · 0 deploys  
> **Next:** `PHASE_3A10_SHOWCASE_MONITORING_DRY_RUN`

## Decision

**Peer cohort change: NO**

`peers_uu_collection_lifestyle_owner_decision_v2` already includes Hotel Indigo + Kimpton as IHG competitive representation. Additional Active/Live IHG brands (Voco, Even, Vignette) enter **IHG showcase portfolio only**.

## IHG Showcase Portfolio V1 (5)

| Brand | Peer? | Role |
|-------|-------|------|
| Hotel Indigo | YES | UU lifestyle peer |
| Kimpton Hotels | YES | UU lifestyle peer |
| Voco Hotels | NO | Conversion Upscale portfolio |
| Even Hotels | NO | Lifestyle Upscale portfolio |
| Vignette Collection | NO | Luxury Collection portfolio |

## Config

- `brand_ai_showcase_companies_v1` → **version 1.1** (+ IHG)
- `brand_decision_eligibility_v1` → **version 1.3** (additive portfolio-only entries)
- Geography entries for Voco / Even / Vignette
- Peer v2 membership **unchanged** (15 brands)
- Wave-1 prompts **84 unchanged**
- OpenAI calls still **84** (company ≠ call multiplier)

## Excluded from portfolio (examples)

Inactive or off-strategy: InterContinental, Crowne Plaza, Holiday Inn / Express, avid, Atwell, Candlewood, Staybridge, Garner, HUALUXE, Regent, Six Senses.
