# Brand AI Visibility — Phase 3A.8 Showcase Eligibility Hardening

> **Status:** PASS · 2026-08-14  
> **Prior:** Phase 3A.7 showcase data governance  
> **Hard stop honored:** 0 provider calls · 0 Brand Basics writes · 0 entitlements · 0 deploys  
> **BUILD STATUS:** `BRAND_AI_VISIBILITY_PHASE_3A8_SHOWCASE_ELIGIBILITY_HARDENING_PASS`  
> **Next:** `PHASE_3A9_BILINGUAL_PROMPT_GOVERNANCE`

## What hardened

- **Geography DEVELOPMENT_ELIGIBILITY** from Brand Basics `Region Offered` (≠ Footprint operating presence)
- **New Build** from Brand Footprint `Total New Build Hotel` (>0 → ELIGIBLE; 0/missing → UNKNOWN, never NOT_ELIGIBLE)
- **Branded Residences** from Brand Basics `Branded Residences Status` (Yes/Case-by-Case → ELIGIBLE; No → NOT_ELIGIBLE)
- **Mixed Use** remains UNKNOWN (no Brand Basics field) — recommend SPLIT from Residences
- Lifestyle Collection UNKNOWNs preserved
- Eligibility config **v1 → v1.1** (28 entries changed)

## Taxonomy recommendation for prompts

KEEP: Conversion · Collection/Soft Brand · Lifestyle · Upper-Upscale · Branded Residences (split)  
MODIFY: Owner Economics → Soft-Brand Affiliation Flexibility  
DEFER: Mixed Use · New Build addressable framing (monitoring OK with UNKNOWN/partial)

## Mexico

Country eligibility UNKNOWN for all 15; SAFE for showcase (UNKNOWN ≠ NOT_ELIGIBLE) under CALA Region Offered.
