# AC Hotel A Coruña — Full Onboarding V1 Founder Report

**Branch:** `deploy/gdi-pe-v1-7-customer-closure`  
**Start HEAD (preflight):** `ebf7b9a10b331392cff92ce226e420a284823b87`  
**Intelligence base:** `appa2cE7FTRmIbB32`  
**HPC base (ALT):** `appCCUsuGsE1ifoLk`  
**Forbidden legacy:** `appvtnDurnMSjINP6` — blocked for writes

---

## X. FINAL VERDICT

**HPC + ADP PASS — GDI NEEDS ANOTHER MARKET CYCLE**

---

## A. HPC IDENTITY

EXISTING MATCH: **None** (0 hits)  
DUPLICATE DECISION: **NO_EXISTING_CANONICAL_MATCH**  
CANONICAL NAME: AC Hotel A Coruña  
HPC ID: `rec2PVBDavppGpenm`  
MARRIOTT CODE: **LCGCO** (`ind_marriott_es_lcgco`)  
ADDRESS: Enrique Mariñas 36, A Coruña, 15009, Spain  
ROOMS: **116**  
LAT/LON: **43.339129 / -8.404032** (Mapbox permanent)

## B. OWNERSHIP / OPERATOR

OWNER: **UNKNOWN**  
OWNER CONFIDENCE: UNKNOWN  
OPERATOR: **ACHM Hotels by Marriott / ACHM Spain Management SL** (CIF B86107406)  
OPERATOR CONFIDENCE: **HIGH**  
SOURCES: achmhotels.com reforma; ACHM about; empresia.es  
Note: Owner/operator intentionally not written to HPC on create (`confirm-no-owner-operator`).

## C. PROPERTY PRODUCT

ROOMS: 116 (HIGH)  
MEETING ROOMS: 6  
TOTAL EVENT SPACE: 674 m² (~7,254 sq ft)  
LARGEST CAPACITY: Banquets 302 m² · 290 reception / 220 theater / 220 banquet  
PROPERTY PROFILE: `fixtures/ai-demand-positioning/ac-hotel-a-coruna-property-profile.json`

## D. HPC

CREATE / UPDATE: **CREATE**  
FIELDS WRITTEN: ~39 allowlisted  
DUPLICATES: **0**  
HPC READY: **PASS**

## E. ADP PROFILE

TERRITORIES: mature generic intents (business/leisure/couples/family/group/wellness/adventure/celebration)  
VERIFIED ATTRIBUTES: **7** (`marriott_bonvoy`, `full_service`, `meeting_space`, `ballroom`, `business_center`, `urban_lifestyle`, `design_forward`)  
LANGUAGES: **es + en** monitoring; Galician = GDI source research only (not scenario triple)

## F. ADP SCENARIOS

CORE+ARCHETYPE+MARKET (generic_profile): **48**  
CAPABILITY: **15**  
TOTAL: **63**  
PARITY: **PASS** (W Rome / mature generic v2)

## G. ADP PEERS

| Hotel | Role | Reason |
|-------|------|--------|
| NH Collection A Coruña Finisterre | CORE | Same-city meetings / events primary substitute |
| Meliá María Pita | CORE | Upper-upscale waterfront / meetings |
| Eurostars Atlántico | CORE | Urban business / mid-size meetings |
| Hesperia A Coruña Centro | CORE | Centro meetings overlap |
| Attica21 Coruña | CORE | Business corridor / ADEQUATE n=5 fill |
| AC Hotel Santiago de Compostela | EXCLUDE | Same brand, wrong city |
| Parador Ferrol / cross-market | EXCLUDE | Product / geography mismatch |

ADEQUATE n=5 · CERTIFIED

## H. ADP RUN

EXPECTED: **252** · SUCCESS: **252** · FAIL: **0**  
COST: **$8.19** · RUNTIME: ~36.5 min  
PERIOD: `adp_period_adp_ac_hotel_a_coruna_20260929113035_c74256`  
CERTIFIED: **YES**

## I. ADP RESULTS

CONSIDERATION: **52.8%** (133/252)  
SCENARIO PRESENCE: **76.2%** (48/63)  
PROPERTY REALITY: **3/7** recognized (gapScore 57.1%); HIGH misses: full_service, ballroom, business_center  
TOP OBSERVED: Attica21 (45), NH Collection Finisterre (28), Meliá María Pita (17)  
DISPLACEMENT: NH Collection Finisterre primary displacement action; subject low self-count (4)

## J. ADP FINDINGS

| Finding | Evidence | Root Cause | Action | Completion Criteria |
|---------|----------|------------|--------|---------------------|
| Full-service / ballroom / business-center under-recognition | Reality HIGH gaps on those attributes | Model underweights AC meetings product vs brand/location | Strengthen first-party + distribution signals for 6 rooms / 674 m² / Banquets 290 | Re-baseline; HIGH amenity gaps ↓; ballroom recognition ≥40% |
| NH Collection Finisterre displacement | Top observed competitor 28 mentions | Stronger AI association for A Coruña meetings | Competitive content + meetings packaging vs Finisterre | Displacement share ↓ vs Finisterre on retest |

## K. GDI MARKET INITIALIZATION

RESEARCH LANES: association_conference, corporate_organizational, sports_tournament, event_operator_housing, future_cycle (URBAN_MEETING archetype — not resort)  
DEMAND GENERATORS / seed: **7** Hotel Fits  
RESEARCH TARGETS: **14** · WEEKLY_READY

## L. GDI SOURCE ACQUISITION

QUERIES: **22** · FETCHES: **28** · VALID candidate rows: **12**  
LANGUAGE MIX: SERP **hl=es / gl=es**; Galician surfaced (e.g. Festa de Exaltación do Marisco)

## M. GDI FUNNEL

SIGNALS: 12 · VALID ENTITIES: 12 watchlist · CUSTOMER READY: **0** · ACTIONABLE: **0** · WATCH: **1** (VALID_WATCH) · INVALID hygiene: 11

## N. TOP GDI OPPORTUNITIES

No customer-ready rows. Watch / watchlist samples (not promoted):

| Opportunity | Motion | Evidence | Summary | WHO | Why Now |
|-------------|--------|----------|---------|-----|---------|
| Festa de Exaltación do Marisco | event_operator_housing | VALID_WATCH | Held — public festival generator depth thin | not researched | accommodation planning claim |
| International Conference Lipids in the Ocean 2026 | association | watchlist | Held | — | future cycle |
| Semana da Arquitectura 2026 | association/cultural | watchlist | Held | — | future cycle |
| Super Copa de España Cadete | sports | watchlist | Held | — | future cycle |

Quality bar held — obvious calendar / thin signals not promoted.

## O. SUMMARY QUALITY

STRONG: 0 · ADEQUATE: 0 · THIN held (not promoted) · TITLE-DUPLICATE: 0 promoted

## P. WHO

All zeros on promoted (none promoted). Contact research population: 0.

## Q–R. JEV

CALLS: **0** (empty customer-facing set) · SAFE APPLY: 0 · HELPFUL_DIFFERENT: 0 · WRONG: 0 · HIGH_CONF_WRONG: 0  
MATERIAL VALUE: **LIMITED** this cycle (Spanish locale + urban lanes ran; no promote path for Jev contact)

## S. CUSTOMER READINESS

READY: **0** · HELD_FOR_EVIDENCE / hygiene: dominant · DQ: 0 explicit

## T. CUSTOMER UI

CARD UI UNCHANGED: **PASS** · GDI LIST empty path: **PASS** · ADP published: **PASS**

## U. DURABILITY

HPC: **PASS** · ADP: **PASS** (published + runtime) · GDI: **PASS** config/seed; opportunities thin · LOCAL-ONLY: **NO**

## V. CROSS-HOTEL

BETHESDA / RENAISSANCE / HILTON / W ROME / AC A CORUÑA / SPICE ISLAND: **PASS** · LEAKS: **0**

## W. DECISION

1. Truly no existing HPC? **YES**  
2. Created without duplicate? **YES** `rec2PVBDavppGpenm`  
3. Ownership/operator? Operator ACHM HIGH; owner UNKNOWN  
4. Product correct? **YES** 116 / 6 / 674 m² / 290  
5. Scenario parity? **YES** 63  
6. Peer stewardship? **YES** ADEQUATE  
7. Full ADP baseline? **YES** 252/252 certified  
8. ADP revealed? Mid consideration (52.8%); meetings product under-recognized; NH Finisterre displaces  
9. GDI secondary EU market from scratch? **Initialized** (seed + ES discovery); depth weak for promotion  
10. Useful lanes? Association / sports / cultural watchlist signals; 1 VALID_WATCH festival housing  
11. Beyond obvious public events? **Mostly not yet** — hygiene correctly held calendar noise  
12. Customer-ready? **0**  
13. Summaries at global standard? **No promoted**  
14. WHO on every promoted? **N/A**  
15. Jev material? **LIMITED**  
16. Spanish sources handled? **YES** (hl=es)  
17. Galician useful? **YES** at least one Galician festival signal surfaced  
18. Hotel-specific prod logic? **0**  
19. Cross-hotel leakage? **0**  
20. Ready for customer use? **ADP yes / GDI not yet**  
21. Largest generic gap? **Spanish/Galician hidden-demand depth** — planner/org WHO under public events so signals clear INVALID and reach ADEQUATE customer-ready

---

## PERSISTENCE

| | |
|--|--|
| HPC ID | `rec2PVBDavppGpenm` |
| ADP PROPERTY ID | `adp_ac_hotel_a_coruna` |
| ADP PERIOD | `adp_period_adp_ac_hotel_a_coruna_20260929113035_c74256` |
| GDI HOTEL ID | `rec2PVBDavppGpenm` |
| HOTEL-SPECIFIC PROD HARDCODES | **0** |
| SURFE AUTO / PII | **0 / 0** |
| WEBHOUND | **0** |
| JEV PERSON APPLY | **NO** |
| CARD UI CHANGES | **0** |
