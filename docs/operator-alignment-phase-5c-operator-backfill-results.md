# Operator Alignment Phase 5C — Operator Setup Backfill Results

**Date:** 2026-05-25  
**Sample deal:** `recIeGRZP21udmTnt` (Aeropuerto Cancún Select-Service Hotel)  
**Operators updated:** 10 active Operator Setup Master records  
**Backfill report:** `reports/operator-setup-alignment-backfill-2026-05-25T184332.json`  
**Scoring audit:** `reports/operator-alignment-scoring-phase5c-recIeGRZP21udmTnt.json`

---

## Summary

Phase 5C populated structured **Operator Setup** fields using **live Airtable option labels only** (`reports/operator-alignment-live-airtable-options.json`). Each operator received a **differentiated** service/structure/profile bundle (9 unique `Offered Services` sets).

Scores moved from Phase 5E deal-only wiring (~72–76) to a wider **74–86** band with **7 Strong** and **3 Moderate** companies — improvement where operator data supports the deal, not universal inflation.

---

## Operators updated (10)

| Operator | Master ID | Archetype | Offered Services (count) |
|----------|-----------|-----------|--------------------------|
| Cenote Azul Operadores | recQ6Cf8O2z0tiqBz | Yucatán select / upper-mid | 8 |
| Viento Sur Gestión Hotelera | recZPHT2zqc8K6itx | Andean commercial platform | 10 |
| Antillano Norte Hospitality Group | recTUjuDxL96yWcQA | Caribbean upscale | 8 |
| Barrio Hotelero CDMX | recq3NiRxOerg4kZU | Mexico select-service | 8 |
| Metro Lodging São Paulo | recwbyY4qfNP1bV3r | Brazil regional mixed | 7 |
| Mangle Azul Hospitalidad | recZgNR85WZKDItLF | Full-service / lifestyle F&B | 8 |
| Panamerican Lodging Partners S.A. | recbT3q8ApRIBu4j5 | Institutional / luxury | 7 |
| Río Plata Hotel Partners | reckO98E46sKTn3F3 | Southern cone full-service | 6 |
| Hotel Equities (CALA) | recWPKu5laVZxsvpn | CALA resort / all-inclusive | 8 |
| Oro Verde Lodge & Hotel Operators | recxAa86Qoc0nFRSt | Central America resort | 8 |

---

## Fields populated by table

| Table | Fields written (when empty) |
|-------|----------------------------|
| **Master** | Data Confidence Level (`Inferred`), Source Type (`Imported sample data`), Last Updated Date |
| **Platform & Markets** | Active Countries, Active Markets / Cities, Market Presence Type |
| **Profile & Positioning** | Service Models Supported, Brand Families Operated (where archetype supports) |
| **Profile** (skipped if present) | `chainScalesSupported` — preserved existing values |
| **Commercial** | Management Structures Supported, New-Build Opening Experience, Pre-Opening Support Capability |
| **Governance** | Offered Services, Owner Reporting Level, F&B Capability Level, Revenue Management Capability, Sales Platform, Governance Cadence |

---

## Fields skipped / not written

| Item | Reason |
|------|--------|
| `chainScalesSupported` | Already populated on all 10 profiles — not overwritten |
| **Uruguay** as Active Country | Not a live Airtable option — Montevideo appears in market text for several operators; logged in plan notes |
| Non-select fields | `Last Updated Date` written as ISO date (not validated as select) |

---

## Score comparison (recIeGRZP21udmTnt)

| Phase | Avg | Min–Max | Strong (80+) | Moderate (65–79) | Conditional (50–64) |
|-------|-----|---------|--------------|------------------|---------------------|
| Pre-5E (legacy deal fields) | 64.1 | 54–69 | 0 | 5 | 5 |
| 5E (structured deal only) | 71.8 | 65.6–75.6 | 0 | 10 | 0 |
| **5C (+ operator backfill)** | **80.7** | **74.3–85.8** | **7** | **3** | **0** |

### Factor changes (typical top operator)

| Factor | 5E | 5C |
|--------|-----|-----|
| dealStructureAssignment | ~72 | **100** (management structures documented) |
| serviceOfferings | ~44 | **91–100** (Offered Services aligned to deal must-haves) |
| geographyMarkets | 88–100 | 92 (Active Countries + Cities) |
| assetProjectStageFit | ~60 | 59–69 (pre-opening differentiation) |
| systemsReporting | ~90 | 90 (Owner Reporting Level set) |

### Intentional lower scores (differentiation)

| Operator | Score | Band | Why lower |
|----------|------:|------|-----------|
| Hotel Equities (CALA) | 75.9 | Moderate | **Limited** pre-opening / new-build vs deal pre-development + pre-opening required |
| Oro Verde Lodge & Hotel Operators | 74.3 | Moderate | Chain scale partial (25 on factor for one operator pattern); stage/pre-open mix |
| Río Plata Hotel Partners | 77.1 | Moderate | Thinner service bundle (6 services); institutional reporting vs monthly deal expectation |

**Cenote Azul** and **Viento Sur** rank highest (85+) — strong Mexico/Cancún presence, full third-party + franchise support, and high service overlap with Cancún deal must-haves.

---

## Narrative differentiation

| Area | Improvement |
|------|-------------|
| Factor rationales | Structure/service/geo/reporting now emit **field-specific** `rationale` strings (e.g. monthly reporting, Mexico presence, third-party path) |
| Service overlap | Different `Offered Services` → different service factor scores (82.9 vs 91.4 vs 100) |
| OAS UI defaults | Phase 5E reduced unconditional default bullets when ≥2 real signals exist |

Full de-templating of `humanizeCompanyAlignmentSignal` remains optional **Phase 5F** work.

---

## Validation

```bash
node scripts/validate-operator-setup-alignment-backfill.mjs   # passed
node scripts/backfill-operator-setup-alignment-fields.mjs --active-operators  # dry-run
node scripts/backfill-operator-setup-alignment-fields.mjs --active-operators --apply
node scripts/audit-operator-alignment-scoring.mjs recIeGRZP21udmTnt --out reports/operator-alignment-scoring-phase5c-recIeGRZP21udmTnt.json
```

---

## Scripts / libraries

| Artifact | Path |
|----------|------|
| Backfill script | `scripts/backfill-operator-setup-alignment-fields.mjs` |
| Plan library | `lib/operator-alignment-operator-backfill-plans.js` |
| Field map | `lib/operator-alignment-operator-field-map.js` |
| Validation | `scripts/validate-operator-setup-alignment-backfill.mjs` |

---

## Manual enrichment still useful

- Real **brand portfolio** links (brand IDs) for `brandPortfolioRelevance` factor
- **Case studies** / verified operator-provided data → upgrade `Data Confidence Level` from `Inferred`
- Add **Uruguay** to Airtable country options if Río Plata Montevideo presence should be structured
- Operator-specific **PMS / systems** fields for `systemsReporting` depth beyond reporting level

---

## Confirmations

- No `OPERATOR_MATCH_WEIGHTS` changes  
- No BAS / OCS / PDF layout changes  
- No new Airtable select options created  
- No overwrite of existing `chainScalesSupported` without `--overwrite`
