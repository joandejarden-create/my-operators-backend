# Operator Alignment Phase 5F — Narrative Differentiation Polish

**Date:** 2026-05-25  
**Sample deal:** `recIeGRZP21udmTnt` (Aeropuerto Cancún Select-Service Hotel)  
**Scoring audit (final):** `reports/operator-alignment-scoring-phase5f-final-recIeGRZP21udmTnt.json`  
**Validation:** `node scripts/validate-operator-alignment-narrative-diversity.mjs recIeGRZP21udmTnt`

---

## Summary

Phase 5F improves **owner-facing narrative** on Operator Alignment Snapshot company cards and section tables. Scoring weights, Airtable schema, BAS, OCS, and PDF layout structure are **unchanged**. Scores for the sample deal remain at Phase 5C levels (**avg 80.7**, range **74.3–85.8**, 7 Strong / 3 Moderate).

Narratives now pull from structured **Operator Setup** fields (Active Countries/Markets, Offered Services, pre-opening, reporting, commercial platform, etc.) plus existing factor breakdown rationales — with neutral language only.

---

## What changed

| Area | Change |
|------|--------|
| **Server narrative pack** | `lib/operator-alignment-company-narratives.js` — `buildOperatorNarrativePack()` builds rationale, supports, validation, weaken, questions, key consideration, review status, factors reviewed |
| **Company API payload** | `lib/operator-alignment-company-utils.js` — attaches narrative fields on each `buildCompanyAlignmentResult()` row |
| **UI / PDF renderer** | `public/js/operator-alignment-snapshot.js` — prefers server fields when present; falls back to legacy humanizers |
| **Demo metadata (non-scoring)** | `lib/operator-alignment-operator-narrative-meta.js` — archetype labels per operator ID for thin profiles only |
| **Validation** | `scripts/validate-operator-alignment-narrative-diversity.mjs` |
| **Audit output** | `scripts/audit-operator-alignment-scoring.mjs` — includes narrative fields in JSON for Phase 5F reviews |

---

## Operator fields used in text

| Field | Example use in narrative |
|-------|---------------------------|
| Active Countries / Active Markets | “Market alignment is supported by documented Mexico presence…” |
| Service Models Supported | Service model fit / mismatch (e.g. resort vs select-service) |
| Management Structures Supported | Third-party management path alignment |
| Offered Services | Must-have service overlap bullets |
| Pre-Opening Support Capability | New-build / pre-development validation lines |
| Owner Reporting Level | Monthly vs institutional reporting validation |
| Revenue Management / Sales Platform | Commercial platform strength lines |
| F&B Capability Level | Conditional weaken if F&B scope differs |
| Brand Families Operated | Brand/portfolio validation questions |
| Data Confidence Level | Inferred-data validation bullet |

All lines are generated from documented values only; no invented geography, scores, or recommendations.

---

## Before / after examples (sample deal)

### Owner-facing rationale (generic → specific)

**Before (client default):**  
*“Cenote Azul Operadores currently shows Strong Alignment Signals based on available Operator Setup data. Current signals appear to be concentrated around market overlap, chain-scale overlap, and project-stage fit. Before outreach, the owner/advisor should validate whether the operator's management structure, service platform, and active market coverage align with the intended third-party management path.”*

**After (Phase 5F):**  
*“Cenote Azul Operadores currently shows Strong Alignment Signals based on available Operator Setup data. Market alignment is supported by documented Mexico presence in Operator Setup. Management scope appears directionally aligned with the deal's third-party management path… Service platform overlap appears supported for Full hotel management, Pre-opening planning, Revenue management, and Sales… Before outreach, validate whether documented markets, management structures, and service scope align with the deal path.”*

### Key consideration (table — was often identical)

| Operator | Phase 5F key consideration |
|----------|----------------------------|
| Cenote Azul Operadores | Pre-opening / project-stage fit appears relevant; confirm opening support scope. |
| Viento Sur Gestión Hotelera | Commercial platform appears relevant; validate service delivery depth. |
| Antillano Norte Hospitality Group | Resort/all-inclusive capability may be relevant; validate fit to select-service project. |
| Panamerican Lodging Partners S.A. | Institutional reporting appears relevant; confirm governance cadence. |
| Hotel Equities (CALA) | Pre-opening capability appears relevant; confirm opening support scope. |

### Hotel Equities (CALA) — profile-specific validation

Moderate band narrative now surfaces **limited pre-opening** and **resort/all-inclusive** service models vs a select-service airport deal — without advisory language.

---

## Final Narrative Prioritization Pass

Second pass on `lib/operator-alignment-company-narratives.js` to **prioritize distinctive signals** per operator instead of echoing every scoring factor.

### Repeated phrases removed

| Removed from supports / rationale | Routed to |
|-----------------------------------|-----------|
| “Current signals appear to be concentrated around market overlap, chain-scale overlap…” | Replaced with “Stronger alignment signals appear around {top 2–3 themes}.” |
| Generic breakdown: “Brand agreement is franchise; evaluated separately…” | Validation only (franchise split), deprioritized for key consideration |
| “Fee / commercial assumptions may need validation” | **WHAT NEEDS VALIDATION** only (never supports) |
| “Validate open alignment factors before external sharing.” | Removed; replaced with deal-specific validation lines |
| Scoring rationales with score ≥ 75 (generic geography / franchise copy) | Excluded from supports via `GENERIC_SUPPORT_PATTERNS` |

### Prioritization logic

1. Build **signal candidates** with priority scores (market city > country > services overlap > pre-opening > RM > management > franchise split).
2. Pick up to **4 support bullets** from highest-priority `support` lines only (no breakdown dump).
3. Pick **2–4 validation bullets** from profile-specific gaps (pre-opening, resort/select-service, franchise split, fee/commercial, inferred data).
4. **Key consideration** uses explicit key order (resort mismatch → pre-opening → commercial → market → franchise), not “first validation in list.”
5. **Owner-facing rationale** leads with top 2–3 **theme labels** (e.g. “Mexico market presence”, “revenue management capability”), not a factor laundry list.
6. Market/support copy includes **operator footprint + archetype accent** so cards differ even when Mexico is shared.

### Before / after (final pass)

**Before (first 5F pass — repetitive):**  
*“Current signals appear to be concentrated around market overlap, chain-scale overlap, project-stage fit, service platform fit, and brand or portfolio relevance.”*  
Supports on every card: market + management + full must-have list + fee/commercial.

**After (Cenote Azul):**  
*“Stronger alignment signals appear around Mexico market presence, third-party management scope, and must-have service overlap.”*  
Supports: Mexico City/Cancún/Monterrey (Yucatán select-service accent), management scope, “Full hotel management + Pre-opening + Procurement”, select-service model.

**After (Viento Sur):**  
Key consideration: *Confirm commercial platform depth for revenue management delivery.*  
Supports emphasize **Advanced centralized RM** and distinctive offered services.

**After (Antillano Norte):**  
Key consideration: *Validate fit between resort/all-inclusive capabilities and select-service project.*  
Weaken: resort vs select-service airport hotel.

### Final validation results (2026-05-25)

```
Avg score: 80.7 (unchanged)
Distinct rationales (top 5): 5 / 5
Distinct first sentences (top 5): 5 / 5
Distinct key considerations (top 8): 4 / 8
Distinct supports bundles (top 5): 5 / 5
PASS — narrative prioritization and diversity checks OK.
```

Top 5 key considerations (sample):

| Operator | Key consideration |
|----------|-------------------|
| Cenote Azul Operadores | Validate pre-opening support scope. |
| Viento Sur Gestión Hotelera | Confirm commercial platform depth for revenue management delivery. |
| Antillano Norte Hospitality Group | Validate fit between resort/all-inclusive capabilities and select-service project. |
| Barrio Hotelero CDMX | Confirm active operations in Mexico. |
| Metro Lodging São Paulo | Validate pre-opening support scope. |

### PDF refresh

Re-open `http://localhost:8080/operator-alignment-snapshot.html?dealId=recIeGRZP21udmTnt` (hard refresh / cache-bust on `operator-alignment-snapshot.js` if needed), then **Print / Save as PDF**. Layout unchanged; operator detail cards (page 2+) should show prioritized copy.

---

## Executive Summary Parity with Brand Alignment Snapshot

### What changed

- New module `lib/operator-alignment-executive-summary.js` mirrors the Brand Alignment executive summary pattern (6 short paragraphs).
- `buildOperatorAlignmentCompaniesSnapshot()` attaches `operatorAlignmentSummaryParagraphs` on the companies API payload.
- Client `buildOperatorSummaryParagraphs()` renders server-built paragraphs when present (no generic “operator review appears relevant” fallback when companies are loaded).

### Dynamic inputs used

| Paragraph | Source |
|-----------|--------|
| Deal context | `dealContext` + `normalizeOperatorAlignmentDealInputs` (Brand Agreement Structure, Operating Model, keys, chain scale, project type, market) |
| Review set | `companiesForConsideration.length`, table limit (8), `countAlignmentBands()` |
| Strongest signals | Top 3 operators by Strong/Moderate band (names only; neutral phrasing) |
| Recurring factors | Aggregated from top companies’ `whatSupportsReview` / `factorsReviewed` |
| Validation areas | Aggregated from `whatNeedsValidation` + structured deal fields |
| Internal-use note | Fixed neutral disclaimer (same intent as Brand summary para 5) |

### Before / after (`recIeGRZP21udmTnt`)

**Before (2 short paragraphs):**

> The current inputs describe Aeropuerto Cancún Select-Service Hotel as a 162-key, Upper Midscale, New Build hospitality opportunity in Cancún, Mexico. The current deal profile indicates Third-party managed is in scope. Based on available deal inputs and Operator Setup data, operator review appears particularly relevant for structured screening.
>
> This snapshot includes 5 operator profile pathways… Recurring themes include market coverage, chain-scale overlap…

**After (6 paragraphs, ~242 words):**

1. *The current inputs describe Aeropuerto Cancún Select-Service Hotel as a 162-key, upper-midscale, new build hospitality opportunity in Cancún, Mexico. The deal is currently structured around a franchise brand path with a third-party managed operating model in scope.*
2. *The current company-level review set includes 10 operating companies, with 8 shown in this snapshot table and detail section. Based on available deal and Operator Setup data, the alignment pattern is concentrated among Strong Alignment Signals.*
3. *Cenote Azul Operadores, Viento Sur Gestión Hotelera, and Antillano Norte Hospitality Group currently show stronger alignment signals in the review set…*
4. *The strongest recurring signals appear to be market presence, management structure overlap, service platform coverage, pre-opening support, owner reporting, and chain-scale and service model compatibility…*
5. *Several important factors still require validation before controlled operator outreach, including fee/commercial assumptions, pre-opening scope, active market coverage…*
6. *This snapshot should be used as an internal screening and discussion tool… does not determine final operator selection…*

### Validation (executive summary checks)

`node scripts/validate-operator-alignment-narrative-diversity.mjs recIeGRZP21udmTnt` verifies review set count, alignment pattern, top operator name, validation themes, internal-use language, word count (~325 cap), and banned recommendation phrases.

### Remaining limitations

- Profile pathway count is not woven into paragraph 2 when company-level data is available (pathways remain in section 2 table).
- Recurring-signal aggregation is theme-based on narrative text, not raw factor breakdown objects (breakdown is not exposed on company API rows).
- When companies are gated, summary falls back to a short profile-only message (3 paragraphs).

---

## Validation results (2026-05-25, initial 5F)

```
Distinct rationales (top 5): 5 / 5
Distinct key considerations (top 10): 5 / 10
Distinct supports bundles (top 10): 7
Distinct validation bundles (top 10): 7
PASS — narrative diversity and language checks OK.
```

Checks: no banned recommendation phrases, no raw internal factor keys in owner text, no invented field names.

---

## Scores unchanged (confirm Phase 5C wiring)

| Metric | Phase 5C / 5F |
|--------|----------------|
| Avg score | 80.7 |
| Range | 74.3 – 85.8 |
| Strong | 7 |
| Moderate | 3 |
| `OPERATOR_MATCH_WEIGHTS` | Unchanged |

---

## Remaining limitations

- **Data confidence `Inferred`** appears on all 10 demo operators — many cards include a validation bullet to confirm fields before outreach.
- **City-level market lines** require both deal city and operator Active Markets to match; some operators only surface country-level presence.
- **Brand / portfolio factor** often missing from denominator — not always reflected in factors reviewed.
- **Key considerations** can still repeat when multiple operators share the same dominant validation theme (e.g. pre-opening on new-build deals); diversity validation requires ≥3 distinct values, not 10/10 unique.
- **PDF refresh** is a browser print/export of the same layout; re-open OAS for `recIeGRZP21udmTnt` after deploy to verify visual copy (no layout structure change).

---

## Files modified

- `lib/operator-alignment-executive-summary.js` (new)
- `lib/operator-alignment-company-narratives.js` (new)
- `lib/operator-alignment-operator-narrative-meta.js` (new)
- `lib/operator-alignment-company-utils.js`
- `public/js/operator-alignment-snapshot.js`
- `scripts/validate-operator-alignment-narrative-diversity.mjs` (new)
- `scripts/audit-operator-alignment-scoring.mjs`
- `docs/operator-alignment-phase-5f-narrative-polish.md` (this file)
- `docs/operator-alignment-snapshot-implementation-checklist.md`
