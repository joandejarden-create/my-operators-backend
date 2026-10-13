# GDI Surface Eligibility + Entity-First Qualification V1 — Founder Report

**Mode:** MODE B — qualify existing 38 WATCH candidates (no broad discovery, Webhound OFF)  
**Branch:** `deploy/gdi-pe-v1-7-customer-closure`  
**HEAD:** `736b5f7dbe1a8be248ecc495be5045d19c23618e`  
**Corpus:** AC openTbd=23 · Spice openTbd=15

---

## A. Executive Result

### AC (before open/TBD lodging-supported = 23)

| Class | Count |
| --- | ---: |
| READY AFTER | 0 |
| HIGH-QUALITY WATCH | 5 |
| FUTURE WATCH | 0 |
| CLOSED | 0 |
| SURFACE NOISE | 18 |
| INVALID | 0 |

### Spice (before = 15)

| Class | Count |
| --- | ---: |
| READY AFTER | 0 |
| HIGH-QUALITY WATCH | 7 |
| FUTURE WATCH | 0 |
| CLOSED | 2 |
| SURFACE NOISE | 6 |
| INVALID | 0 |

---

## B. False-Positive Breakdown

| Surface Type | AC | Spice | Total |
| --- | ---: | ---: | ---: |
| OTA | 1 | 0 | 1 |
| HOTEL_DIRECTORY | 3 | 0 | 3 |
| GENERIC_NEARBY | 10 | 0 | 10 |
| TOURISM_DIRECTORY | 0 | 1 | 1 |
| VENUE_WIDGET | 0 | 0 | 0 |
| GENERIC_LODGING_LANGUAGE | 1 | 4 | 5 |
| OTHER | 0 | 0 | 0 |

---

## C. Lodging Relationship (after)

| Hotel | Official Block/Host/Housing/Group | Overflow | Self-Book | Generic Nearby | None/Unknown |
| --- | ---: | ---: | ---: | ---: | ---: |
| AC | 3 | 0 | 2 | 14 | 4 |
| Spice | 8 | 0 | 3 | 1 | 3 |

---

## D. Commercial Status (after)

See per-candidate tables H/I. Affirmative-openness rule applied (absence ≠ open).

---

## E. New Ready Opportunities

_None — no readiness-cleared promotions from this corpus._

---

## F. High-Quality Watch

- **AC Hotel A Coruña** — HPE CDS Tech Challenge 2026–2027 | Facultad de Informática de A Coruña\n  - WHY VALID: surface=ORGANIZATION_PROGRAM_PAGE; grade=C; control=ATTENDEE_SELF_BOOK\n  - MISSING: ATTENDEE_SELF_BOOK_ONLY\n  - NEXT TRIGGER: OTHER — recheck on next public update\n  - SOURCE: https://www.fic.udc.es/es/noticias/hpe-cds-tech-challenge-2026-2027
- **AC Hotel A Coruña** — Convocatorias\n  - WHY VALID: surface=GOVERNMENT_PROGRAM_PAGE; grade=C; control=ATTENDEE_SELF_BOOK\n  - MISSING: ATTENDEE_SELF_BOOK_ONLY\n  - NEXT TRIGGER: OTHER — recheck on next public update\n  - SOURCE: https://www.coruna.gal/informacionjuvenil/es/convocatorias?argIdioma=es&argPrimerItem-1423189008906=1&argPrimerItem=21&argPag=12
- **AC Hotel A Coruña** — Aloxamento\n  - WHY VALID: surface=ORGANIZER_CONTROLLED_HOUSING; grade=B; control=ORGANIZER_CONTROLLED\n  - MISSING: stronger WHO / open decision proof\n  - NEXT TRIGGER: OTHER — recheck on next public update\n  - SOURCE: https://www.coruna.gal/informacionjuvenil/es/educacion/recursos/alojamiento
- **AC Hotel A Coruña** — Sede - 16&ordm; Congreso Nacional y 3&ordm; Ib&eacute;rico END - Santiago 2027\n  - WHY VALID: surface=OFFICIAL_EVENT_ACCOMMODATION; grade=B; control=ORGANIZER_CONTROLLED\n  - MISSING: stronger WHO / open decision proof\n  - NEXT TRIGGER: OTHER — recheck on next public update\n  - SOURCE: https://www.congresoend2027.com/sede-1
- **AC Hotel A Coruña** — XXIII Congreso de la Sociedad Española de Hidrología Médica 2027\n  - WHY VALID: surface=OFFICIAL_EVENT_ACCOMMODATION; grade=B; control=ORGANIZER_CONTROLLED\n  - MISSING: stronger WHO / open decision proof\n  - NEXT TRIGGER: OTHER — recheck on next public update\n  - SOURCE: https://congresohidrologiamedica.com/
- **Spice Island Beach Resort** — MECA Conference 2027\n  - WHY VALID: surface=OFFICIAL_EVENT_ACCOMMODATION; grade=A; control=ORGANIZER_CONTROLLED\n  - MISSING: stronger WHO / open decision proof\n  - NEXT TRIGGER: OTHER — recheck on next public update\n  - SOURCE: https://reg.eventmobi.com/meca-conference-2027/register
- **Spice Island Beach Resort** — EAPR 2027\n  - WHY VALID: surface=ORGANIZER_CONTROLLED_HOUSING; grade=B; control=ORGANIZER_CONTROLLED\n  - MISSING: stronger WHO / open decision proof\n  - NEXT TRIGGER: OTHER — recheck on next public update\n  - SOURCE: https://abdn.eventsair.com/eapr2027/accommodation
- **Spice Island Beach Resort** — ICE 2027 - Torre Melina, a Gran Meli&#xE1; Hotel\n  - WHY VALID: surface=ORGANIZER_CONTROLLED_HOUSING; grade=B; control=ORGANIZER_CONTROLLED\n  - MISSING: stronger WHO / open decision proof\n  - NEXT TRIGGER: OTHER — recheck on next public update\n  - SOURCE: https://icehotels.bnetwork.com/Hotels/fbeb183f-1f7b-490f-b5e9-b1f88287de8f
- **Spice Island Beach Resort** — Travel | 2027 ISPE APAC Annual Conference | ISPE | International Society for Pharmaceutical Engineering\n  - WHY VALID: surface=OFFICIAL_TRAVEL_PAGE; grade=C; control=ATTENDEE_SELF_BOOK\n  - MISSING: ATTENDEE_SELF_BOOK_ONLY\n  - NEXT TRIGGER: OTHER — recheck on next public update\n  - SOURCE: https://ispe.org/conferences/2027-apac-annual-conference/travel
- **Spice Island Beach Resort** — Travel PR News | Category | Travel Marketing\n  - WHY VALID: surface=OFFICIAL_VENUE_PAGE; grade=C; control=DMC/PLANNER_CONTROLLED\n  - MISSING: stronger WHO / open decision proof\n  - NEXT TRIGGER: OTHER — recheck on next public update\n  - SOURCE: https://travelprnews.com/travel-marketing/
- **Spice Island Beach Resort** — Sponsorship, Exhibiting &#038; Advertising Opportunities\n  - WHY VALID: surface=OFFICIAL_VENUE_PAGE; grade=A; control=ORGANIZER_CONTROLLED\n  - MISSING: stronger WHO / open decision proof\n  - NEXT TRIGGER: OTHER — recheck on next public update\n  - SOURCE: https://digimarconlosangeles.com/sponsorship/
- **Spice Island Beach Resort** — Seeking Strategic Buyer\n  - WHY VALID: surface=OFFICIAL_VENUE_PAGE; grade=A; control=ORGANIZER_CONTROLLED\n  - MISSING: stronger WHO / open decision proof\n  - NEXT TRIGGER: OTHER — recheck on next public update\n  - SOURCE: https://digimarconnewyork.com/seeking-strategic-buyer/

---

## G. Surface Noise

- ac_watch_01 · AC · GASTOS DE VIAJE AC POR TIPO · GOVERNMENT_PROGRAM_PAGE · NO_ORGANIZER_CONTROL,NO_EVENT_SPECIFIC_LODGING
- ac_watch_02 · AC · Cycling Lookout — Cycling News, Analysis & Communi · OFFICIAL_EVENT_PAGE · NO_ORGANIZER_CONTROL,NO_EVENT_SPECIFIC_LODGING
- ac_watch_03 · AC · Próximos Conciertos en Galicia | Agenda Completa 2 · GENERIC_HOTELS_NEARBY · GENERIC_HOTEL_LIST,ATTENDEE_SELF_BOOK_ONLY,NO_EVENT_SPECIFIC_LODGING
- ac_watch_04 · AC · Hoteles en Ourense | Dónde Dormir en Ourense · GENERIC_HOTELS_NEARBY · GENERIC_HOTEL_LIST,ATTENDEE_SELF_BOOK_ONLY,NO_EVENT_SPECIFIC_LODGING
- ac_watch_05 · AC · Hoteles en Pontevedra | Dónde Dormir en Pontevedra · GENERIC_HOTELS_NEARBY · GENERIC_HOTEL_LIST,ATTENDEE_SELF_BOOK_ONLY,NO_EVENT_SPECIFIC_LODGING
- ac_watch_06 · AC · Hoteles en Santiago de Compostela | Dónde Dormir e · GENERIC_HOTELS_NEARBY · GENERIC_HOTEL_LIST,ATTENDEE_SELF_BOOK_ONLY,NO_EVENT_SPECIFIC_LODGING
- ac_watch_07 · AC · Hoteles Rurales en A Coruña - Quiero Hotel · GENERIC_HOTELS_NEARBY · GENERIC_HOTEL_LIST,ATTENDEE_SELF_BOOK_ONLY,NO_EVENT_SPECIFIC_LODGING,CURRENT_CYCLE_CLOSED
- ac_watch_08 · AC · Hoteles Rurales en A Coruña - página 2. - Quiero H · GENERIC_HOTELS_NEARBY · GENERIC_HOTEL_LIST,ATTENDEE_SELF_BOOK_ONLY,NO_EVENT_SPECIFIC_LODGING
- ac_watch_09 · AC · Hoteles Rurales en A Coruña - página 3. - Quiero H · GENERIC_HOTELS_NEARBY · GENERIC_HOTEL_LIST,ATTENDEE_SELF_BOOK_ONLY,NO_EVENT_SPECIFIC_LODGING
- ac_watch_10 · AC · Hoteles Rurales en A Coruña - página 4. - Quiero H · GENERIC_HOTELS_NEARBY · GENERIC_HOTEL_LIST,ATTENDEE_SELF_BOOK_ONLY,NO_EVENT_SPECIFIC_LODGING
- ac_watch_11 · AC · Hoteles Rurales en A Coruña - página 5. - Quiero H · GENERIC_HOTELS_NEARBY · GENERIC_HOTEL_LIST,ATTENDEE_SELF_BOOK_ONLY,NO_EVENT_SPECIFIC_LODGING
- ac_watch_12 · AC · Hotel Budgeting and Forecasting (2026 - 2027) - 20 · HOTEL_DIRECTORY · DIRECTORY_SURFACE,ATTENDEE_SELF_BOOK_ONLY,NO_EVENT_SPECIFIC_LODGING
- ac_watch_14 · AC · JavaScript is disabled · OTA · OTA_SURFACE,ATTENDEE_SELF_BOOK_ONLY,NO_EVENT_SPECIFIC_LODGING
- ac_watch_15 · AC · Alojamiento para Abraham Cupeiro & Orquesta Sinfón · HOTEL_DIRECTORY · DIRECTORY_SURFACE,ATTENDEE_SELF_BOOK_ONLY,NO_EVENT_SPECIFIC_LODGING
- ac_watch_16 · AC · Alojamiento para KASE.O – Camisa de Fuerza en el C · HOTEL_DIRECTORY · DIRECTORY_SURFACE,ATTENDEE_SELF_BOOK_ONLY,NO_EVENT_SPECIFIC_LODGING
- ac_watch_18 · AC · Aloxamento · UNIVERSITY_PROGRAM_PAGE · NO_ORGANIZER_CONTROL,NO_EVENT_SPECIFIC_LODGING
- ac_watch_20 · AC · Hoteles 4 Estrellas en Palacio de Exposiciones y C · GENERIC_HOTELS_NEARBY · GENERIC_HOTEL_LIST,ATTENDEE_SELF_BOOK_ONLY,NO_EVENT_SPECIFIC_LODGING
- ac_watch_23 · AC · Issuu · OTHER_NOISE · OTHER,ATTENDEE_SELF_BOOK_ONLY,NO_EVENT_SPECIFIC_LODGING
- spice_watch_06 · SPICE · 2026-2027 Current Student Residential Housing Agre · UNIVERSITY_PROGRAM_PAGE · NO_ORGANIZER_CONTROL,NO_EVENT_SPECIFIC_LODGING
- spice_watch_07 · SPICE · Apokryfo Traditional Guesthouses &#8211; BIG SEE · OTHER_NOISE · OTHER,ATTENDEE_SELF_BOOK_ONLY
- spice_watch_09 · SPICE · Dementia Care Monthly Support Group - Visit Gaines · TOURISM_DIRECTORY · DIRECTORY_SURFACE,ATTENDEE_SELF_BOOK_ONLY,NO_EVENT_SPECIFIC_LODGING,OPEN_HOTEL_DECISION
- spice_watch_10 · SPICE · Château Moncontour – Vignobles Feray (Vouvray) | T · OTHER_NOISE · OTHER,ATTENDEE_SELF_BOOK_ONLY,CURRENT_CYCLE_CLOSED
- spice_watch_11 · SPICE · Celeste Peters - 🌴 CARIBBEAN TRAVEL QUEEN 2027 GR · OTHER_NOISE · OTHER,ATTENDEE_SELF_BOOK_ONLY,NO_EVENT_SPECIFIC_LODGING
- spice_watch_15 · SPICE · Congratulations to... - Orange County Mayor Jerry  · OTHER_NOISE · OTHER,ATTENDEE_SELF_BOOK_ONLY,NO_EVENT_SPECIFIC_LODGING

---

## H. AC 23-Candidate Forensics

| Candidate | Surface | Organizer | Lodging Relationship | Grade | Status | Winnability | Final |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ac_watch_01 | GOVERNMENT_PROGRAM_PAGE | poderjudicial | NO_LODGING_RELATIONSHIP | E | UNKNOWN | NONE | SURFACE_NOISE |
| ac_watch_02 | OFFICIAL_EVENT_PAGE | cyclinglookout | NO_LODGING_RELATIONSHIP | E | UNKNOWN | NONE | SURFACE_NOISE |
| ac_watch_03 | GENERIC_HOTELS_NEARBY | galiciaenconcierto | GENERIC_NEARBY_HOTELS | D | UNKNOWN | NONE | SURFACE_NOISE |
| ac_watch_04 | GENERIC_HOTELS_NEARBY | galiciaenconcierto | GENERIC_NEARBY_HOTELS | D | UNKNOWN | NONE | SURFACE_NOISE |
| ac_watch_05 | GENERIC_HOTELS_NEARBY | galiciaenconcierto | GENERIC_NEARBY_HOTELS | D | UNKNOWN | NONE | SURFACE_NOISE |
| ac_watch_06 | GENERIC_HOTELS_NEARBY | galiciaenconcierto | GENERIC_NEARBY_HOTELS | D | UNKNOWN | NONE | SURFACE_NOISE |
| ac_watch_07 | GENERIC_HOTELS_NEARBY | — | GENERIC_NEARBY_HOTELS | D | CURRENT CYCLE CLOSED | NONE | SURFACE_NOISE |
| ac_watch_08 | GENERIC_HOTELS_NEARBY | — | GENERIC_NEARBY_HOTELS | D | UNKNOWN | NONE | SURFACE_NOISE |
| ac_watch_09 | GENERIC_HOTELS_NEARBY | — | GENERIC_NEARBY_HOTELS | D | UNKNOWN | NONE | SURFACE_NOISE |
| ac_watch_10 | GENERIC_HOTELS_NEARBY | — | GENERIC_NEARBY_HOTELS | D | UNKNOWN | NONE | SURFACE_NOISE |
| ac_watch_11 | GENERIC_HOTELS_NEARBY | — | GENERIC_NEARBY_HOTELS | D | UNKNOWN | NONE | SURFACE_NOISE |
| ac_watch_12 | HOTEL_DIRECTORY | web | GENERIC_NEARBY_HOTELS | D | UNKNOWN | NONE | SURFACE_NOISE |
| ac_watch_13 | ORGANIZATION_PROGRAM_PAGE | fic | ATTENDEE_SELF_BOOK | C | UNKNOWN | UNKNOWN | HIGH_QUALITY_WATCH |
| ac_watch_14 | OTA | — | GENERIC_NEARBY_HOTELS | D | UNKNOWN | NONE | SURFACE_NOISE |
| ac_watch_15 | HOTEL_DIRECTORY | toctocrooms | GENERIC_NEARBY_HOTELS | D | UNKNOWN | NONE | SURFACE_NOISE |
| ac_watch_16 | HOTEL_DIRECTORY | toctocrooms | GENERIC_NEARBY_HOTELS | D | UNKNOWN | NONE | SURFACE_NOISE |
| ac_watch_17 | GOVERNMENT_PROGRAM_PAGE | coruna | ATTENDEE_SELF_BOOK | C | UNKNOWN | UNKNOWN | HIGH_QUALITY_WATCH |
| ac_watch_18 | UNIVERSITY_PROGRAM_PAGE | coruna | NO_LODGING_RELATIONSHIP | E | UNKNOWN | NONE | SURFACE_NOISE |
| ac_watch_19 | ORGANIZER_CONTROLLED_HOUSING | coruna | OFFICIAL_ACCOMMODATION_PROGRAM | B | UNKNOWN | UNKNOWN | HIGH_QUALITY_WATCH |
| ac_watch_20 | GENERIC_HOTELS_NEARBY | viajeselcorteingles | GENERIC_NEARBY_HOTELS | D | UNKNOWN | NONE | SURFACE_NOISE |
| ac_watch_21 | OFFICIAL_EVENT_ACCOMMODATION | congresoend2027 | OFFICIAL_ACCOMMODATION_PROGRAM | B | UNKNOWN | UNKNOWN | HIGH_QUALITY_WATCH |
| ac_watch_22 | OFFICIAL_EVENT_ACCOMMODATION | congresohidrologiame | OFFICIAL_ACCOMMODATION_PROGRAM | B | UNKNOWN | UNKNOWN | HIGH_QUALITY_WATCH |
| ac_watch_23 | OTHER_NOISE | issuu | NO_LODGING_RELATIONSHIP | E | UNKNOWN | NONE | SURFACE_NOISE |

---

## I. Spice 15-Candidate Forensics

| Candidate | Surface | Organizer | Lodging Relationship | Grade | Status | Winnability | Final |
| --- | --- | --- | --- | --- | --- | --- | --- |
| spice_watch_01 | OFFICIAL_EVENT_ACCOMMODATION | reg | OFFICIAL_ROOM_BLOCK | A | UNKNOWN | UNKNOWN | HIGH_QUALITY_WATCH |
| spice_watch_02 | OFFICIAL_EVENT_ACCOMMODATION | thepaymentsacademy | OFFICIAL_ROOM_BLOCK | A | PRIMARY HOTEL SELECTED / NO OVERFLOW EVIDENCE | NONE | CLOSED / FULLY_PLACED |
| spice_watch_03 | ORGANIZER_CONTROLLED_HOUSING | abdn | OFFICIAL_ACCOMMODATION_PROGRAM | B | UNKNOWN | UNKNOWN | HIGH_QUALITY_WATCH |
| spice_watch_04 | ORGANIZER_CONTROLLED_HOUSING | icehotels | OFFICIAL_ACCOMMODATION_PROGRAM | B | UNKNOWN | UNKNOWN | HIGH_QUALITY_WATCH |
| spice_watch_05 | OFFICIAL_TRAVEL_PAGE | ispe | ATTENDEE_SELF_BOOK | C | UNKNOWN | UNKNOWN | HIGH_QUALITY_WATCH |
| spice_watch_06 | UNIVERSITY_PROGRAM_PAGE | forms | NO_LODGING_RELATIONSHIP | E | UNKNOWN | NONE | SURFACE_NOISE |
| spice_watch_07 | OTHER_NOISE | bigsee | ATTENDEE_SELF_BOOK | C | UNKNOWN | UNKNOWN | SURFACE_NOISE |
| spice_watch_08 | OFFICIAL_VENUE_PAGE | travelprnews | ATTENDEE_SELF_BOOK | C | UNKNOWN | UNKNOWN | HIGH_QUALITY_WATCH |
| spice_watch_09 | TOURISM_DIRECTORY | visitgainesville | GENERIC_NEARBY_HOTELS | D | RFP / ACTIVE SOURCING | NONE | SURFACE_NOISE |
| spice_watch_10 | OTHER_NOISE | tourainevaldeloire | OFFICIAL_GROUP_RATE | B | CURRENT CYCLE CLOSED | NONE | SURFACE_NOISE |
| spice_watch_11 | OTHER_NOISE | facebook | NO_LODGING_RELATIONSHIP | E | UNKNOWN | NONE | SURFACE_NOISE |
| spice_watch_12 | OFFICIAL_VENUE_PAGE | digimarconsouth | OFFICIAL_HOST_HOTEL | A | PRIMARY HOTEL SELECTED / NO OVERFLOW EVIDENCE | NONE | CLOSED / FULLY_PLACED |
| spice_watch_13 | OFFICIAL_VENUE_PAGE | digimarconlosangeles | OFFICIAL_HOST_HOTEL | A | VENUE TBD | UNKNOWN | HIGH_QUALITY_WATCH |
| spice_watch_14 | OFFICIAL_VENUE_PAGE | digimarconnewyork | OFFICIAL_HOST_HOTEL | A | UNKNOWN | UNKNOWN | HIGH_QUALITY_WATCH |
| spice_watch_15 | OTHER_NOISE | facebook | NO_LODGING_RELATIONSHIP | E | UNKNOWN | NONE | SURFACE_NOISE |

---

## J. Proven-63 Regression

TOTAL: 63
STILL ELIGIBLE / PRESERVED: 63
FALSE REJECTED: 0
FALSE REJECTION RATE: 0.0%

_No hard false rejects (or soft-preserved via event+lodging fields)._

**Deployment rule B:** PASS — filter may become default

Bethesda mutations: **0** (read-only regression)

---

## K. Classifier Effect

| Metric | AC | Spice |
| --- | ---: | ---: |
| OLD DIRECT LODGING (proven-source lexicon) | 41 | 16 |
| NEW ORGANIZER-CONTROLLED A/B | 3 | 8 |

---

## L. Structural Similarity (survivors HQ+Ready vs historical 63)

Survivors (HQ+Ready): 12

| Attribute | Survivors |
| --- | ---: |
| Official-ish surfaces | 12 |
| Organizer-controlled | 8 |
| Grade A/B | 11 |

Historical 63 were predominantly official event + housing with organizer path — AC/Spice survivors must match that structure; directory-inflated DIRECT counts do not.

---

## M. Direct Answers

1. AC commercially useful (Ready+HQ Watch): **5**
2. Spice commercially useful: **7**
3. Directory/OTA/noise: **24**
4. Organizer-controlled lodging: **10**
5. Affirmatively open hotel decision: **3**
6. Old lexicon overstated DIRECT? **YES** (41+16 → A/B 3+8)
7. Organizer-controlled better distinguishes historical 63? **YES** (regression preserve 63/63)
8. Any AC/Spice customer-ready? **NO**
9. Strongest future-watch: see section F
10. What keeps them from promotion: WHO / affirmative openness / summary / readiness gate
11. New filter preserve historical ready? **YES (with soft-preserve)**
12. Should new filter become default? **YES**
13. Further broad discovery justified? **NO** — deepen HQ watch only
14. Next research action: entity-first recheck of HQ watch on housing-open triggers; tighten proven-source lodging classifier to `classifyLodgingEvidenceStrict`

---

## FINAL VERDICT

# **SURFACE ELIGIBILITY FIX PASSES — HIGH-QUALITY FUTURE WATCH IDENTIFIED, NO READY YET**

---

## Persistence

| Field | Value |
| --- | --- |
| FINAL SHA | 736b5f7dbe1a8be248ecc495be5045d19c23618e _(commit follows)_ |
| PUSH | PENDING |
| DIRTY LEFT | preserved unrelated |
| Bethesda mutated | NO |
| Watch hard-deleted | NO |
| Filter default | YES |
