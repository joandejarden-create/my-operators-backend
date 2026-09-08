# CALA_25_PILOT_SELECTION

**Dealality Packet 2.8A — Stratified 25-Hotel CALA Factory Pilot**

| Field | Value |
|--------|--------|
| Packet | **2.8A** |
| Purpose | Factory pilot selection only — ownership / brand / operator / diligence path diversity |
| Census baseline | **5,956** production Hotel Property Census hotels |
| Sources | Packet 2.8 corpus + Phase 2A provenance audit + Tripadvisor Apify benchmark samples + Cancún/GDL radar fixtures + ADP census links + Alliance sibling map |
| Live census dump | **Not used** (no full `data/` HPC export found) |
| ID confidence | **25/25 CONFIRMED_ID** from fixtures/reports |
| **HARD STOP** | **No research execution. No Webhound. No paid runs. Selection artifact only.** |

---

## 1. Census baseline & source inventory

### Authoritative count

- **`cala_census_baseline_hotels`: 5,956** — `reports/hotel-intelligence/packet-2.8/HOTEL_INTELLIGENCE_LEARNING_CORPUS_V1.json`
- Confirmed in `docs/data-intelligence/full-cala-phase-2a-global-provenance-coverage-audit.md` (`PRODUCTION_CENSUS_COUNT = 5956`)
- Also cited in Packet 2.8 `RESEARCH_RUN_INVENTORY.md`

### Production by country (Phase 2A audit — top)

| Country | Count | Share of 5,956 |
|---------|------:|---------------:|
| Mexico | 2,181 | ~37% |
| Colombia | 967 | ~16% |
| Costa Rica | 748 | ~13% |
| Dominican Republic | 654 | ~11% |
| Brazil | 494 | ~8% |
| Panama | 325 | ~5% |
| Argentina | 129 | ~2% |
| Jamaica | 78 | ~1% |
| Chile | 65 | ~1% |
| Peru | 57 | ~1% |

### Candidate ID sources used

| Source | What it provides |
|--------|------------------|
| Tripadvisor Apify benchmark phase2/phase3 | Hotels with `dealality_record_id`, name, country, city, rooms |
| Cancún / Guadalajara / Zapopan radar fixtures | Census IDs + brand / chain scale / market / management |
| ADP census links | JW / Westin Monterrey Valle |
| Mexico Explorer Alliance sibling map | Package sibling census IDs |

---

## 2. Selection criteria (summary)

Stratify across country, market, chain scale, brand family, independent vs branded, owner/operator type, public/private, ownership confidence, brand-history complexity, development signal, data completeness, difficulty (~10 easy · ~8 medium · ~7 hard).

**Non-goals:** not random sample of 5,956; not regression set; not research authorization.

---

## 3. Table of 25 candidates (CONFIRMED_ID)

| Rank | Name | Census ID | Country | Market / city | Brand | Difficulty | ID status |
|-----:|------|-----------|---------|---------------|-------|------------|-----------|
| 1 | Comfort Inn & Suites Querétaro | `rec0qmO7Xj7uyjWLZ` | Mexico | Querétaro | Comfort Inn (Choice) | EASY | CONFIRMED_ID |
| 2 | JW Marriott Hotel Monterrey Valle | `recsn3BUKJ9PNfeZW` | Mexico | Monterrey Valle | JW Marriott | MEDIUM | CONFIRMED_ID |
| 3 | voco Guadalajara Expo Area | `recGZZCek9vDQGG1L` | Mexico | Guadalajara / Expo | voco / Real Inn lineage (IHG) | HARD | CONFIRMED_ID |
| 4 | Nizuc Resort & Spa | `recFzDPBquimFYEKU` | Mexico | Cancún | Independent | HARD | CONFIRMED_ID |
| 5 | Breathless Cancun Soul Resort & Spa | `rec8m9O8nnNZgK9pT` | Mexico | Cancún Hotel Zone | Breathless (Hyatt / ALG) | MEDIUM | CONFIRMED_ID |
| 6 | InterContinental Presidente Cancun Resort | `recUP5BmDKKRj3ic8` | Mexico | Cancún | InterContinental / Grupo Presidente | MEDIUM | CONFIRMED_ID |
| 7 | Grand Velas Riviera Maya | `rec3t0vOGGf5LeXDb` | Mexico | Playa del Carmen / Riviera Maya | Grand Velas | MEDIUM | CONFIRMED_ID |
| 8 | Nobu Hotel Los Cabos | `rec4HVKN6WefcrS25` | Mexico | Los Cabos | Nobu | MEDIUM | CONFIRMED_ID |
| 9 | Hotel Xcaret Mexico | `recQoyPT90YimUCxb` | Mexico | Playa del Carmen | Xcaret | HARD | CONFIRMED_ID |
| 10 | JW Marriott Hotel Bogota | `rec07FR57B9Yt3TCh` | Colombia | Bogotá | JW Marriott | EASY | CONFIRMED_ID |
| 11 | Faranda Collection Cartagena | `rec3c9fCwqOCA9TAO` | Colombia | Cartagena | Faranda Collection | MEDIUM | CONFIRMED_ID |
| 12 | Hotel Casa Don Luis by Faranda Boutique | `rec9CkQpW17aFPfyg` | Colombia | Cartagena | Faranda Boutique | EASY | CONFIRMED_ID |
| 13 | TRIBE Medellín | `recHvjyMVmQ1vPXGf` | Colombia | Medellín | TRIBE (Accor) | EASY | CONFIRMED_ID |
| 14 | Iberostar Waves Punta Cana | `rec0aE97rzRABwJon` | Dominican Republic | Punta Cana / Bávaro | Iberostar | EASY | CONFIRMED_ID |
| 15 | Occidental Caribe (former Barceló Punta Cana) | `receb9kXfXpygywj3` | Dominican Republic | Bávaro | Occidental (ex-Barceló) | HARD | CONFIRMED_ID |
| 16 | Hotel Casa Coco | `recCWs5oRz4Eji2No` | Dominican Republic | Boca Chica | Independent | HARD | CONFIRMED_ID |
| 17 | Santarena Hotel | `recST2nLpCY63suUs` | Costa Rica | Las Catalinas | Independent | EASY | CONFIRMED_ID |
| 18 | Arenas Del Mar | `recN8LWXIJj4FylHB` | Costa Rica | Manuel Antonio | Independent | HARD | CONFIRMED_ID |
| 19 | Dovle Branded Residences | `recke3dkUlz0mULbB` | Panama | Panama City | Branded residences | HARD | CONFIRMED_ID |
| 20 | Residence Inn by Marriott San Juan Isla Verde | `rec01qR3VIWPOzFc4` | Puerto Rico | San Juan / Carolina | Residence Inn | EASY | CONFIRMED_ID |
| 21 | Alaia Belize, Autograph Collection | `rec3YKuBtW5opXnsa` | Belize | Belize | Autograph Collection | MEDIUM | CONFIRMED_ID |
| 22 | ROK Hotel Kingston, Tapestry Collection by Hilton | `rec07xNs8B2wqs0HN` | Jamaica | Kingston | Tapestry Collection | MEDIUM | CONFIRMED_ID |
| 23 | The Westin Camino Real, Guatemala | `rec4EXqes1zXVslcR` | Guatemala | Guatemala City | Westin / Camino Real | MEDIUM | CONFIRMED_ID |
| 24 | ibis budget Lima Miraflores | `rec02LxNNFjOzwN8K` | Peru | Lima / Miraflores | ibis budget (Accor) | EASY | CONFIRMED_ID |
| 25 | Kimpton Seafire Resort + Spa | `recRGqxkCLrdrgc9y` | Cayman Islands | Grand Cayman | Kimpton | HARD | CONFIRMED_ID |

---

## 4. Exclusions (mandatory golden regression)

| Record ID | Name | Reason |
|-----------|------|--------|
| `recUNycnMwOVFX0hc` | Krystal Grand Puerto Vallarta | Golden regression |
| `recIwaP1etgx2g9nA` | Cambridge Beaches Resort & Spa | Golden regression |
| `recsYJb2R1jarPpK3` | Sheraton Guadalajara Expo | Golden regression |
| `recTYaiA4S6fR6ixx` | voco Cancún / Real Inn | Golden regression |

---

## 5. Recommended factory wave order (still no research)

| Wave | Ranks | Intent |
|------|-------|--------|
| W1 Smoke | 1, 10, 14, 17, 24 | Easy franchise / known AI / boutique control |
| W2 Operator graphs | 2, 11, 12, 5, 6 | Aimbridge / Faranda / ALG / Presidente |
| W3 Hard ownership | 3, 4, 9, 15, 16, 18, 19, 25 | Reflag, opaque, mega OO, micro, residences, island luxury |
| W4 Soft-brand ring | 8, 13, 21, 22, 23 | Nobu / TRIBE / Autograph / Tapestry / Westin–Camino Real |

---

## 6. HARD STOP

```
NO RESEARCH EXECUTION
NO WEBHOUND / PAID PROVIDER RUNS
NO CENSUS WRITES
NO DOSSIER GENERATION
SELECTION ARTIFACT ONLY — Packet 2.8A
```

Next: founder approval → then optional Packet **2.8B** / authorized native batch.
