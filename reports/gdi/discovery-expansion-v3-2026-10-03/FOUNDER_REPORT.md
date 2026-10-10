# GDI Discovery Expansion V3

## A. Executive Summary

Surface bug fixed — **AidEx Geneva is now KEEP_ACTIVE and customer-ready** (YOTEL facing = 1). Multilingual routing + 9 scout families + next-best research + cross-language dedup shipped and run bounded across five hotels (54 SERP queries, ~$2.70). Thresholds unchanged. Watch discipline unchanged.

SERP title/snippet discovery produced **156 new candidates** but **0 new customer-ready / valid-watch promotions** from that thin layer — correct under gates (most hits fail multiple of timing + lodging + WHO simultaneously; next-best only fires on single-blocker near-misses). Default next step is page-level enrichment on lodging-hint / procurement hits, not threshold cuts.

## B. Known Surface Bug

- **Before:** AidEx → `DOWNGRADE_TO_DEMAND_GENERATOR` / ready=false
- **After:** AidEx → `KEEP_ACTIVE` / ready=true
- **Remaining blockers:** none

## C. Discovery Architecture Added

- `resolveGdiMarketLanguages`
- Localized canonical intents
- Association series timing states (watch-only for RECURRING_EXPECTED / ROTATION_PREDICTED)
- Scouts: Association, Procurement, CorporateTrigger, ProjectWorkforce, MedicalResearch, SportsHousing, University, TourDmc, HiddenDemand
- `getGdiNextBestResearchAction`
- Cross-language dedup
- Feeder-market query family
- Adaptive language/scout yield scores

## D. YOTEL Geneva Lake

Baseline n=13 ready=1 watch=1
New V3 candidates=49 qualified=0 ready=0 watch=0
Final facing=1 structuralReady=1 structuralWatch=1
Strongest families: AssociationScout (useful 0/16); ProcurementScout (useful 0/22); MedicalResearchScout (useful 0/8)

## E. AC A Coruña

Baseline n=32 ready=0 watch=4
New V3 candidates=40 qualified=0 ready=0 watch=0
Final facing=0 structuralReady=0 structuralWatch=4
Strongest families: AssociationScout (useful 0/12); ProcurementScout (useful 0/19); MedicalResearchScout (useful 0/8)

## F. Spice Island

Baseline n=77 ready=0 watch=3
New V3 candidates=20 qualified=0 ready=0 watch=0
Final facing=0 structuralReady=0 structuralWatch=3
Strongest families: TourDmcScout (useful 0/9); AssociationScout (useful 0/5); ProcurementScout (useful 0/5)

## G. Cambridge Beaches

Baseline n=8 ready=0 watch=0
New V3 candidates=25 qualified=0 ready=0 watch=0
Final facing=0 structuralReady=0 structuralWatch=0
Strongest families: TourDmcScout (useful 0/8); AssociationScout (useful 0/8); ProcurementScout (useful 0/8)

## H. NOW NOW NoHo

Baseline n=8 ready=0 watch=2
New V3 candidates=22 qualified=0 ready=0 watch=0
Final facing=0 structuralReady=0 structuralWatch=2
Strongest families: AssociationScout (useful 0/7); ProcurementScout (useful 0/9); UniversityDemandScout (useful 0/6)

## I. Multilingual Yield

Top languages by useful yield: fr (0), en (0), es (0)

## J. Scout Yield

Top scouts: AssociationScout (0), ProcurementScout (0), MedicalResearchScout (0)

## K. Future-Series Yield

Series rows captured: 11 (RECURRING_EXPECTED / ROTATION_PREDICTED are watch-only — never auto customer-ready)

## L. Hidden-Demand Yield

Hidden sub-demand candidates extracted: 6

## M. Targeted Research Completion

Next-best research actions: 0

## N. Cost

$2.70 across 54 queries

## O. What Should Become Default GDI Behavior

1. Keep surface motion-copy fix (structured evidence + thesis-safe)
2. Route languages by market profile — not blanket multilingual
3. Run procurement + association scouts early (timing+buyer+lodging coexist)
4. Always run one next-best research step on single-blocker near-misses before terminal reject
5. Keep Future Watch strict; never promote RECURRING_EXPECTED to ready
6. Stop/continue via languageYieldScore / scoutYieldScore

## RETURN CARD

| Key | Value |
|---|---|
| SURFACE BUG FIXED | YES |
| AIDEX CUSTOMER-READY AFTER FIX | YES |
| MULTILINGUAL ROUTING IMPLEMENTED | YES |
| ASSOCIATION SCOUT IMPLEMENTED | YES |
| PROCUREMENT SCOUT IMPLEMENTED | YES |
| CORPORATE TRIGGER SCOUT IMPLEMENTED | YES |
| PROJECT WORKFORCE SCOUT IMPLEMENTED | YES |
| MEDICAL RESEARCH SCOUT IMPLEMENTED | YES |
| SPORTS HOUSING SCOUT IMPLEMENTED | YES |
| UNIVERSITY DEMAND SCOUT IMPLEMENTED | YES |
| TOUR DMC SCOUT IMPLEMENTED | YES |
| HIDDEN DEMAND EXTRACTOR IMPLEMENTED | YES |
| NEXT-BEST-RESEARCH IMPLEMENTED | YES |
| YOTEL NEW CANDIDATES | 49 |
| YOTEL NEW QUALIFIED | 0 |
| YOTEL FINAL CUSTOMER READY | 1 |
| YOTEL FINAL FUTURE WATCH | 1 |
| AC NEW CANDIDATES | 40 |
| AC NEW QUALIFIED | 0 |
| AC FINAL CUSTOMER READY | 0 |
| AC FINAL FUTURE WATCH | 4 |
| SPICE NEW CANDIDATES | 20 |
| SPICE NEW QUALIFIED | 0 |
| SPICE FINAL CUSTOMER READY | 0 |
| SPICE FINAL FUTURE WATCH | 3 |
| CAMBRIDGE NEW CANDIDATES | 25 |
| CAMBRIDGE NEW QUALIFIED | 0 |
| CAMBRIDGE FINAL CUSTOMER READY | 0 |
| CAMBRIDGE FINAL FUTURE WATCH | 0 |
| NOW NOW NEW CANDIDATES | 22 |
| NOW NOW NEW QUALIFIED | 0 |
| NOW NOW FINAL CUSTOMER READY | 0 |
| NOW NOW FINAL FUTURE WATCH | 2 |
| TOTAL NEW CANDIDATES | 156 |
| TOTAL NEW QUALIFIED | 0 |
| TOTAL NEW CUSTOMER READY | 0 |
| TOTAL NEW VALID FUTURE WATCH | 0 |
| TOP 3 DISCOVERY FAMILIES | AssociationScout (0) · ProcurementScout (0) · MedicalResearchScout (0) |
| TOP 3 LANGUAGES | fr (0) · en (0) · es (0) |
| CROSS-LANGUAGE DUPLICATES REMOVED | 20 |
| PROMISING NBR COUNT | 0 |
| INCREMENTAL RESEARCH COST | $2.70 |
| GDI THRESHOLDS CHANGED? | NO |
| WATCH QUALITY STANDARD BYPASSED? | NO |
| ADP CHANGED? | NO |
| SHARE TOKENS CHANGED? | NO |

## FINAL VERDICT

**AidEx surface bug fixed and customer-ready. V3 discovery architecture is production-reusable across five hotels. Thin SERP hits did not (and should not) inflate ready/watch — next default investment is page-level completion on procurement/association lodging-hint rows, guided by scout/language yield scores.**

STOP.
