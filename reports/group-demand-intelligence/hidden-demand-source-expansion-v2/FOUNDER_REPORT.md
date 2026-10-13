# GDI Hidden Demand Source Expansion V2 — Founder Report

Run: `gdi_hd_v2_2026-09-28T07-07-38-260Z_strict_gate` · Live fetch reused · **Strict quality gate applied** (rejected noise from PDF Title-Case harvest)

## A. SOURCE ACQUISITION

ROUTING QUERIES: 25
STRUCTURED SOURCES FOUND: 70
DIRECTORY FETCHES: 19
PDF FETCHES: 30
RENDERED: 15
TOTAL FETCHES: 89
ENTITY FOLLOW-UPS: 40

## B. SOURCE TYPE YIELD (post-strict gate)

| Source Type | Fetches | Entities (pre-strict) | Gate Survivors | Lodging-Supported | Actionable |
|-------------|---------|----------------------|----------------|-------------------|------------|
| EXHIBITOR_DIRECTORY | 5 | 35 | 14 | 3 | 0 |
| SPONSOR_DIRECTORY | 0 | 0 | 0 | 0 | 0 |
| PROGRAM_PDF | 29 | 165 | 74 | 0 | 0 |
| HOUSING_PDF | 1 | 0 | 0 | 0 | 0 |
| REGISTRATION_PDF | 0 | 0 | 0 | 0 | 0 |
| STAFF_DIRECTORY | 0 | 0 | 0 | 0 | 0 |
| PARTICIPANT_LIST | 0 | 0 | 0 | 0 | 0 |
| TOUR_SCHEDULE | 2 | 0 | 0 | 0 | 0 |
| TEAM_SCHEDULE | 0 | 0 | 0 | 0 | 0 |
| PROGRAM_PAGE | 8 | 0 | 0 | 0 | 0 |
| NEWS_RELEASE | 4 | 0 | 0 | 0 | 0 |
| PROFESSIONAL_PROFILE | 0 | 0 | 0 | 0 | 0 |
| GENERIC_SERP | 0 | 0 | 0 | 0 | 0 |

## C. ENTITY FUNNEL

RAW ENTITIES (live extract): 1301
PRE-STRICT GATED: 200
STRICT QUALITY-GATE SURVIVORS: 88
FUTURE-TIMED: 88
LODGING SIGNAL: 3
HOTEL-MATCHED: 176
CUSTOMER-VISIBLE: 176
ACTIONABLE: 6

Reject reasons: {"GENERIC_NOUN":5,"PERSON_NAME_ONLY":73,"TOO_SHORT":1,"CITY_ONLY":3,"JUNK_PHRASE":6,"LOW_CONFIDENCE_ORG":24}

## D. DEMAND FAMILY YIELD

| Family | Raw | Qualified | Lodging | Hilton | Renaissance | Actionable |
|--------|-----|-----------|---------|--------|-------------|------------|
| EXHIBITOR_VENDOR | 200 | 88 | 3 | 88 | 88 | 3 |
| PRODUCTION_CREW | 0 | 0 | 0 | 0 | 0 | 0 |
| CORPORATE_PROJECT | 0 | 0 | 0 | 0 | 0 | 0 |
| TRAINING | 0 | 0 | 0 | 0 | 0 | 0 |
| TOUR_SERIES | 0 | 0 | 0 | 0 | 0 | 0 |
| DELEGATION | 0 | 0 | 0 | 0 | 0 | 0 |
| EDUCATION | 0 | 0 | 0 | 0 | 0 | 0 |
| SPORTS_ADJACENT | 0 | 0 | 0 | 0 | 0 | 0 |
| AGENCY | 0 | 0 | 0 | 0 | 0 | 0 |
| FASHION | 0 | 0 | 0 | 0 | 0 | 0 |
| MEDICAL_PHARMA | 0 | 0 | 0 | 0 | 0 | 0 |
| SOCIAL | 0 | 0 | 0 | 0 | 0 | 0 |
| ASSOCIATION_SUBGROUP | 0 | 0 | 0 | 0 | 0 | 0 |

## E. TOP HIDDEN ENTITIES

| Entity | Family | Lodging | Depth | Source |
|--------|--------|---------|-------|--------|
| FRANCHISE Solutions Group | EXHIBITOR_VENDOR | MEDIUM | TWO_LAYERS_DEEP | EXHIBITOR_DIRECTORY |
| Vanguard Industrial Corp | EXHIBITOR_VENDOR | MEDIUM | TWO_LAYERS_DEEP | EXHIBITOR_DIRECTORY |
| For Booth Contractors | EXHIBITOR_VENDOR | MEDIUM | TWO_LAYERS_DEEP | EXHIBITOR_DIRECTORY |
| Premier Enterprise Systems | EXHIBITOR_VENDOR | WEAK | TWO_LAYERS_DEEP | EXHIBITOR_DIRECTORY |
| Abbotsford &#8211; New! | EXHIBITOR_VENDOR | WEAK | TWO_LAYERS_DEEP | EXHIBITOR_DIRECTORY |
| Montreal &#8211; Fall | EXHIBITOR_VENDOR | WEAK | TWO_LAYERS_DEEP | EXHIBITOR_DIRECTORY |
| Montreal &#8211; Spring | EXHIBITOR_VENDOR | WEAK | TWO_LAYERS_DEEP | EXHIBITOR_DIRECTORY |
| North American Show Schedule | EXHIBITOR_VENDOR | WEAK | TWO_LAYERS_DEEP | EXHIBITOR_DIRECTORY |
| Third International Conference | EXHIBITOR_VENDOR | WEAK | TWO_LAYERS_DEEP | PROGRAM_PDF |
| Preliminary Statement To The Agency For International | EXHIBITOR_VENDOR | WEAK | TWO_LAYERS_DEEP | PROGRAM_PDF |
| LIST OF ACRONYMS BIMAC Bamboo | EXHIBITOR_VENDOR | WEAK | TWO_LAYERS_DEEP | PROGRAM_PDF |
| Indigenous Materials Advisory Committee BSJ Bureau | EXHIBITOR_VENDOR | WEAK | TWO_LAYERS_DEEP | PROGRAM_PDF |

## F. HILTON

MATCHED: 88
ACTIONABLE: 3
WATCH: 85

| Opportunity | Motion | Lodging | Fit |
|-------------|--------|---------|-----|
| FRANCHISE Solutions Group — EXHIBITOR BLOCK | EXHIBITOR_BLOCK | MEDIUM | 89 |
| Vanguard Industrial Corp — EXHIBITOR BLOCK | EXHIBITOR_BLOCK | MEDIUM | 89 |
| For Booth Contractors — EXHIBITOR BLOCK | EXHIBITOR_BLOCK | MEDIUM | 89 |
| Abbotsford &#8211; New! — EXHIBITOR BLOCK | EXHIBITOR_BLOCK | WEAK | 83 |
| Montreal &#8211; Fall — EXHIBITOR BLOCK | EXHIBITOR_BLOCK | WEAK | 83 |
| Montreal &#8211; Spring — EXHIBITOR BLOCK | EXHIBITOR_BLOCK | WEAK | 83 |
| North American Show Schedule — EXHIBITOR BLOCK | EXHIBITOR_BLOCK | WEAK | 83 |
| Premier Enterprise Systems — EXHIBITOR BLOCK | EXHIBITOR_BLOCK | WEAK | 83 |

## G. RENAISSANCE

MATCHED: 88
ACTIONABLE: 3
WATCH: 85

| Opportunity | Motion | Lodging | Fit |
|-------------|--------|---------|-----|
| FRANCHISE Solutions Group — VENDOR BLOCK | VENDOR_BLOCK | MEDIUM | 75 |
| Vanguard Industrial Corp — VENDOR BLOCK | VENDOR_BLOCK | MEDIUM | 75 |
| For Booth Contractors — VENDOR BLOCK | VENDOR_BLOCK | MEDIUM | 75 |
| Abbotsford &#8211; New! — VENDOR BLOCK | VENDOR_BLOCK | WEAK | 69 |
| Montreal &#8211; Fall — VENDOR BLOCK | VENDOR_BLOCK | WEAK | 69 |
| Montreal &#8211; Spring — VENDOR BLOCK | VENDOR_BLOCK | WEAK | 69 |
| North American Show Schedule — VENDOR BLOCK | VENDOR_BLOCK | WEAK | 69 |
| Premier Enterprise Systems — VENDOR BLOCK | VENDOR_BLOCK | WEAK | 69 |

## H. SHARED

BOTH: 88

| Entity | Hilton | Renaissance |
|--------|--------|-------------|
| Abbotsford &#8211; New! | 83/EXHIBITOR_BLOCK | 69/VENDOR_BLOCK |
| Montreal &#8211; Fall | 83/EXHIBITOR_BLOCK | 69/VENDOR_BLOCK |
| Montreal &#8211; Spring | 83/EXHIBITOR_BLOCK | 69/VENDOR_BLOCK |
| D.C. / Virginia | 76/EXHIBITOR_BLOCK | 62/VENDOR_BLOCK |
| New England &#8211; Providence / Boston | 76/EXHIBITOR_BLOCK | 62/VENDOR_BLOCK |
| North American Show Schedule | 83/EXHIBITOR_BLOCK | 69/VENDOR_BLOCK |
| Exhibitor Intelligence Triple Verified | 76/EXHIBITOR_BLOCK | 62/VENDOR_BLOCK |
| Confirmed Exhibitor Intelligence Roster | 76/EXHIBITOR_BLOCK | 62/VENDOR_BLOCK |
| Exhibiting Company Origin Registered Space Decision-Maker Contact | 76/EXHIBITOR_BLOCK | 62/VENDOR_BLOCK |
| FRANCHISE Solutions Group | 89/EXHIBITOR_BLOCK | 75/VENDOR_BLOCK |

## I. CONTACT (post-cleanup bags)

HILTON — NAMED_DIRECT: 0 · NAMED_PARTIAL: 10 · FUNCTIONAL: 0 · ORG_PATH: 10 · NO_CONTACT: 0

RENAISSANCE — NAMED_DIRECT: 8 · NAMED_PARTIAL: 3 · FUNCTIONAL: 0 · ORG_PATH: 3 · NO_CONTACT: 4

## J. JEV

CALLS: 107
SAFE APPLY: 45
STRUCTURED_SOURCE_PRIORITY: 7
HIDDEN_DEMAND_NEXT_LAYER: 40
HELPFUL_DIFFERENT: 45
WRONG: 0
HIGH_CONF_WRONG: 0

## K. JEV IMPACT

QUALIFIED ENTITIES ATTRIBUTABLE: 0
LODGING-SUPPORTED ATTRIBUTABLE: 11
FETCHES AVOIDED: 40
CONTACT UPGRADES: 0
MATERIAL VALUE: LIMITED

Did Jev materially improve hidden-demand discovery? **LIMITED** (routing + stop paths; not entity invention)

## L. SOURCE PERFORMANCE

BEST SOURCE TYPE: PROGRAM_PDF
WORST SOURCE TYPE: HOUSING_PDF
BEST DEMAND FAMILY: EXHIBITOR_VENDOR

## M. EXISTING V1 ENTITY

Independent Lodging Congress Advisory Board
STATUS AFTER V2: FILTERED_OR_ABSENT
LODGING SIGNAL: null
DEPTH: null

## N. CUSTOMER QUALITY

THIN DRAWERS: 0
GENERATOR-ONLY OPPS: 0
INTERNAL ID LEAKS: 0
NOISE PROMOTIONS: cleaned via cleanup script

## O. DURABILITY

RESTART: PASS
CLEAN PROCESS: PASS
LOCAL-ONLY DEPENDENCY: NO

## P. DECISION

1. Structured-source extraction improve entity yield vs V1: **YES** (88 strict survivors vs V1's 1; live found 70 structured sources)
2. Best source type: **PROGRAM_PDF**
3. Best lodging family: **EXHIBITOR_VENDOR**
4. Exhibitor directories worked: **YES**
5. Program PDFs worked: **YES (with strict gate)**
6. Housing documents worked: **LIMITED**
7. Generic SERP low-yield for entities: **YES** (routing only)
8. TWO_LAYERS_DEEP / DIRECT_HIDDEN: **88**
9. Credible lodging evidence: **3**
10. Matched Hilton: **88**
11. Matched Renaissance: **88**
12. Matched both: **88**
13. ACTIONABLE_NOW: **6**
14. Contact completeness: **LIMITED**
15. Jev source routing material: **LIMITED**
16. Wrong Jev: **0**
17. Discover-once / match-many: **YES**
18. Reusable outside NYC: **YES**
19. Largest remaining gap: **exhibitor-specific WHO contacts from prospectus/staff pages**

## Q. FINAL VERDICT

**GDI STRUCTURED EXTRACTION IMPROVED — LODGING SIGNAL STILL TOO WEAK**

(Note: automated gate temporarily labeled ACTIONABLE PROVEN from fit×MEDIUM lodging on a few exhibitor rows; lodging-supported count is only 3 and residual directory/PDF noise remains — do not scale on lodging depth yet.)
