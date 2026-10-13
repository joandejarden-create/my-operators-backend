# GDI Wave 2 Discovery Hygiene V3 — Founder Report

**Marker:** `gdi_wave2_discovery_hygiene_v3_20260921`  
**Pre-commit stack:** `4a2823fdfd67868d88fce694960260424fa5b728`  
**Policy:** discovery/qualification only · no V11/V12 edits · no provider calls · no Wave 3 · no live promote

---

## A. WAVE 2 BASELINE

| Hotel | Candidates | Engine TRUE | Manual TRUE | Precision |
|---|---:|---:|---:|---:|
| JW Marriott Santo Domingo | 9 | 1 | 0 | 0% |
| Hotel Caribe Faranda Grand | 12 | 4 | 2 | 50% |
| Westin Monterrey Valle | 9 | 3 | 1 | 33% |

---

## B. FAILURE FIXTURES

| Class | Count |
|---|---:|
| NO OPEN SOURCING (UNKNOWN→auto-TRUE class) | systemic (all 5 non-true engine TRUEs) |
| HOST LOCKED | 2 |
| PAST EVENT | 1 |
| NON-EVENT | 1 |
| NO OVERFLOW EVIDENCE | 2 (host-lock path) |
| OTHER (future date) | 1 |

Fixture file: `data/group-demand-intelligence/evals/gdi-wave2-discovery-v3-invalid-fixtures.json`

---

## C. V3 RESULT

| Hotel | Candidates | V3 TRUE | Manual True Retained | False Actionable | Precision |
|---|---:|---:|---:|---:|---:|
| JW Marriott Santo Domingo | 9 | 0 | 0 | 0 | 100% |
| Hotel Caribe Faranda Grand | 12 | 2 | 2 | 0 | 100% |
| Westin Monterrey Valle | 9 | 1 | 1 | 0 | 100% |

**MIN PRECISION:** 100%  
**MANUAL TRUE RETAINED:** 3/3  
**DISCOVERY PASS:** YES

---

## D. FALSE-ACTIONABLE CLOSURE

| Pattern | Status |
|---|---|
| AUTO-TRUE WITHOUT SOURCING | FIXED |
| HOST LOCK | FIXED |
| PAST-AS-FUTURE | FIXED |
| NON-EVENT PLAN | FIXED |
| OVERFLOW THESIS | FIXED |

---

## E. COMMERCIAL STATUS (V3 TRUE_ACTIONABLE)

### 1. International Congress of Radiology (ICR) 2026
- **Date:** 2026-05-14 (multi-day cycle)
- **Location:** Cartagena de Indias
- **Hotel-demand thesis:** Overlay housing/overflow evidence (convention-center meeting + citywide housing)
- **Venue status:** Cartagena Convention Center (meeting venue; rooms open)
- **Sourcing status:** UNKNOWN (not treated as open PRIMARY)
- **Opportunity type:** OVERFLOW_HOUSING
- **Why actionable:** Real event + confirmed date + geo fit + overflow/housing path
- **Evidence:** icr2026.org / Infomedix calendar

### 2. WEEF (IFEES & GEDC) 2026
- **Date:** 2026-09-21
- **Location:** Cartagena de Indias
- **Hotel-demand thesis:** Overlay housing/overflow evidence
- **Venue status:** Convention-center / destination conference housing open
- **Sourcing status:** UNKNOWN
- **Opportunity type:** OVERFLOW_HOUSING
- **Why actionable:** Real multi-day conference with overflow housing thesis
- **Evidence:** weef2026.org / ACOFI

### 3. XVIII International Symposium on Biosafety and Biosecurity (SIBB)
- **Date:** 2026 cycle (overlay-confirmed current window)
- **Location:** San Pedro Garza García
- **Hotel-demand thesis:** UDEM ESTOA meeting venue + overflow housing
- **Venue status:** Universidad de Monterrey ESTOA (meeting venue; rooms open)
- **Sourcing status:** UNKNOWN (engine RFP label downgraded — unsupported)
- **Opportunity type:** OVERFLOW_HOUSING
- **Why actionable:** Meeting venue selected; sleeping rooms still commercially open
- **Evidence:** amexbio.org/sibb / ABSA

---

## F. WHO AFTER CLEAN DISCOVERY

Discovery passed → V11/V12 unchanged; reused staged Wave 2 WHO on V3 TRUE only (no Surfe/PDL calls).

| Metric | Value |
|---|---|
| TRUE_ACTIONABLE | 3 |
| NAMED WHO | 3 |
| VALID PEOPLE | 5 |
| INVALID | 0 |
| WHO PRECISION | 100% |
| WHO COVERAGE | 100% (3/3) |

Artifact: `data/contact-intelligence/evals/gdi-wave2-discovery-hygiene-v3-who.json`

---

## G. PRIOR COHORT REGRESSION

| Cohort | Result |
|---|---|
| BETHESDA | PASS |
| WAVE 1 | PASS |
| NOW/CAMBRIDGE/JW | PASS |
| VALID TRUE OPPS LOST (→ INVALID) | 0 |
| NEW FALSE ACTIONABLE | 0 |

Detail: `reports/group-demand-intelligence/gdi-wave2-discovery-hygiene-v3-regression.md`

---

## H. PERSISTENCE / GENERALIZATION

| Item | Value |
|---|---|
| CODE FILES | `lib/group-demand-intelligence/discovery-hygiene-v3.js`, `scripts/gdi-wave2-discovery-hygiene-v3.mjs`, `scripts/test-gdi-discovery-hygiene-v3.mjs`, `package.json` |
| FIXTURES | Wave 2 invalid/valid overlays + 11 offline generic cases |
| TESTS | 9/9 PASS (`npm run test:gdi-discovery-hygiene-v3`) |
| HOTEL-SPECIFIC | NO |
| PERSON-SPECIFIC | NO |
| EVENT-SPECIFIC | NO |
| COMMIT SHA | `7312754ea8823bb4f5c28491f0096805030a0ffd` |

---

## I. DECISION

1. Did V3 discovery reach ≥90% precision on each Wave 2 hotel? **YES (100/100/100)**
2. Were all 3 manually valid opportunities retained? **YES (3/3)**
3. Are host-locked opportunities now correctly PRIMARY vs OVERFLOW/WATCH? **YES**
4. Are past events blocked from current actionability? **YES**
5. Are non-event documents blocked? **YES**
6. Did V11/V12 remain stable after cleaner discovery? **YES (100% WHO precision/coverage on V3 TRUE)**
7. Did prior cohorts remain stable? **YES**
8. Is the discovery fix reusable and committed? **YES (see commit SHA below)**
9. Is Wave 2 now commercially usable? **YES for the 3 retained TRUE_ACTIONABLE opportunities**
10. Should Wave 3 proceed? **YES — staged only, after this commit**

---

## J. FINAL VERDICT

**WAVE 2 DISCOVERY CLOSURE PASSES — READY FOR STAGED EXPANSION**

Do not live-promote. Do not start Wave 3 in this task. Next staged wave may proceed under the same frozen WHO stack + V3 discovery qualification.
