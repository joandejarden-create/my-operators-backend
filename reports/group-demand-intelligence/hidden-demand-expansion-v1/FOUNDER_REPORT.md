# GDI Hidden Demand Expansion V1 — Founder Report

Run: `gdi_hidden_demand_2026-09-27T14-41-57-792Z_quality_gate_reprocess` · Live discovery reused · Quality gate applied (rejected 38 noise entities)

## A. MARKET DISCOVERY

QUERIES: 30
FETCHES: 61
SIGNALS: 100
DEMAND GENERATORS: 94
HIDDEN ENTITIES: 1
HIDDEN-DEMAND CANDIDATES: 1
LODGING-SUPPORTED: 0

## B. VALUE DEPTH

OBVIOUS_MARKET_DEMAND: 0
ONE_LAYER_DEEP: 0
TWO_LAYERS_DEEP: 1
DIRECT_HIDDEN_SIGNAL: 0

## C. DEMAND FAMILY YIELD

| Family | Candidates | Qualified | Actionable |
|--------|------------|-----------|------------|
| EXHIBITOR_VENDOR | 0 | 0 | 0 |
| PRODUCTION_CREW | 0 | 0 | 0 |
| CORPORATE_PROJECT | 0 | 0 | 0 |
| TRAINING | 0 | 0 | 0 |
| TOUR_SERIES | 0 | 0 | 0 |
| DELEGATION | 0 | 0 | 0 |
| EDUCATION | 0 | 0 | 0 |
| SPORTS_ADJACENT | 0 | 0 | 0 |
| AGENCY | 0 | 0 | 0 |
| FASHION | 0 | 0 | 0 |
| MEDICAL_PHARMA | 0 | 0 | 0 |
| SOCIAL | 0 | 0 | 0 |
| ASSOCIATION_SUBGROUP | 1 | 0 | 0 |

## D. HILTON MATCHING

MATCHED: 1
ACTIONABLE: 0
WATCH: 1
DQ: 0

## E. RENAISSANCE MATCHING

MATCHED: 1
ACTIONABLE: 0
WATCH: 1
DQ: 0

## F. SHARED OPPORTUNITIES

BOTH HOTELS: 1
HILTON ONLY: 0
RENAISSANCE ONLY: 0
NEITHER: 0

## G. SHARED EXAMPLES

| Hidden Demand | Hilton Fit/Motion | Renaissance Fit/Motion |
|---------------|-------------------|------------------------|
| The Independent Lodging Congress Advisory Board ... | 62/ASSOCIATION_SUBGROUP | 72/LEADERSHIP_MEETING |

## H. TOP HILTON OPPORTUNITIES

| Opportunity | Demand Trigger | Motion | Lodging Evidence | Contact | Why Now |
|-------------|----------------|--------|------------------|---------|---------|
| The Independent Lodging Congress Advisory Board ... — ASSOCIATION SUBGROUP | The Independent Lodging Congress Advisor | ASSOCIATION_SUBGROUP | UNKNOWN | ORG_PATH | Monitor — future timing 2027; re-research when housing or te |

## I. TOP RENAISSANCE OPPORTUNITIES

| Opportunity | Demand Trigger | Motion | Lodging Evidence | Contact | Why Now |
|-------------|----------------|--------|------------------|---------|---------|
| The Independent Lodging Congress Advisory Board ... — LEADERSHIP MEETING | The Independent Lodging Congress Advisor | LEADERSHIP_MEETING | UNKNOWN | ORG_PATH | Monitor — future timing 2027; re-research when housing or te |

## J. CONTACT QUALITY — HILTON

NAMED_DIRECT: 0
NAMED_PARTIAL: 7
FUNCTIONAL: 0
ORG_PATH: 8
NO_CONTACT: 0

## K. CONTACT QUALITY — RENAISSANCE

NAMED_DIRECT: 8
NAMED_PARTIAL: 2
FUNCTIONAL: 0
ORG_PATH: 2
NO_CONTACT: 4

## L. EXISTING HILTON RECLASSIFICATION

GENUINELY_HIDDEN: 16
OBVIOUS_GENERATOR_ONLY: 0
DEEPENED: 0
REMOVED/DOWNGRADED: 0

## M. EXISTING RENAISSANCE RECLASSIFICATION

GENUINELY_HIDDEN: 0
OBVIOUS_GENERATOR_ONLY: 6
DEEPENED: 10
REMOVED/DOWNGRADED: 6

## N. JEV

CALLS: 86
SAFE APPLY: 6
HIDDEN_DEMAND_NEXT_LAYER: 6
HELPFUL_DIFFERENT: 6
SAME: 0
WRONG: 0
HIGH_CONF_WRONG: 0

## O. JEV IMPACT

NEW USEFUL PATHS: 6
FETCHES SAVED: 61
QUALIFIED OPPORTUNITIES ATTRIBUTABLE: 1
MATERIAL VALUE: LIMITED

## P. RESEARCH REUSE

SHARED MARKET FETCHES: 61
DUPLICATE HOTEL-SPECIFIC FETCHES AVOIDED: 61

## Q. CUSTOMER QUALITY

THIN DRAWERS: 0
INTERNAL ID LEAKS: 0
GENERATOR-ONLY CUSTOMER OPPS: 0
QUALITY_GATE_REJECTED_NOISE: 38

## R. DECISION

1. Separating obvious market demand from hidden opportunity: **YES**
2. Genuinely hidden demand opportunities found (post quality gate): **1**
3. Best yield families: **ASSOCIATION_SUBGROUP**
4. Families with no useful results: **EXHIBITOR_VENDOR, PRODUCTION_CREW, CORPORATE_PROJECT, TRAINING, TOUR_SERIES, DELEGATION, EDUCATION, SPORTS_ADJACENT, AGENCY, FASHION, MEDICAL_PHARMA, SOCIAL**
5. Opportunities fit both Hilton and Renaissance: **1**
6. Shared opportunities once at market level: **YES** (`hiddenDemandId` + separate `hotelOpportunityId`)
7. Independent fit/thesis/action per hotel: **YES**
8. Avoided duplicate research across hotels: **YES** (61 fetches saved)
9. Hilton lodging-primary opportunities improved: **ARCHITECTURE YES / YIELD LIMITED**
10. Renaissance gained opportunities it did not previously have: **ARCHITECTURE READY / PROMOTE HELD (validation)**
11. Obvious-event opportunities downgraded (after lodging-motion restore rule): **6**
12. Contact quality: **LIMITED** (Surfe AUTO 0)
13. Jev deeper-layer discovery: **LIMITED** (6 helpful different)
14. Wrong Jev decisions: **0**
15. Reusable across additional NYC hotels: **YES**
16. Reusable outside NYC: **YES**
17. Biggest gap: **entity extraction from SERP — need exhibitor-directory / program-PDF structured sources; 38 noise rows gated out**

## S. FINAL VERDICT

**GDI HIDDEN-DEMAND YIELD IMPROVED — ONE MORE SOURCE-EXPANSION CYCLE NEEDED**
