# Operator Fit — Scoring Documentation Discrepancies

**Date:** 2026-08-04  
**Rule:** Current code is implementation truth. This assignment did **not** change code, weights, Airtable, or enablement.

---

## Discrepancies found

### 1. Deal C preferred brands on pilot evaluation path vs contemplated brands elsewhere

| Source | Claim |
| ------ | ----- |
| `scripts/operator-fit-evidence-closure-evaluate.mjs` | Contemplates Hilton / Marriott / Krystal for `pilot_deal_c` |
| Shadow review `preferredBrandCount` | 4 |
| **Current pilot evaluate path** (`operator-fit-internal-pilot-evaluate.mjs` → Deal C `siData`) | Does **not** pass preferred brands → `preferredBrands: []` |
| Live Deal C dump | Brand Experience **N/A**; Brand Compatibility **Not Applicable** |

**Truth for founder math today:** Deal C worked example treats brand layer as N/A. Wiring contemplated brands into the pilot project adapter would change scores — out of scope here.

### 2. Early shadow Deal C ranking vs current Ranking Ready production list

| Era | #1 example |
| --- | ---------- |
| `reports/operator-fit-real-deal-shadow-review.json` (older) | Hotel Equities · Displayed 34 · Limited confidence · Conditionally Rankable |
| Current final pilot / scoring dump | Grupo Hotelero Santa Fe · 38.6 · Strong · Ranking Ready |

Calibration / Market Presence enrichment changed the production pool. Spec uses **current** evaluation.

### 3. Naming: Evidence Confidence vs Evidence Strength

| Layer | Name |
| ----- | ---- |
| Engine / config | Evidence Confidence |
| Owner freeze | Evidence Strength |

Same field; presentation rename only. Spec states both.

### 4. “Official documentation” as evidence class

Some product language lists official documentation as a class. **Config classes** are: verified_project_level · independently_referenced · detailed_operator_provided · portfolio_level_operator · general_operator_claim · unknown. Official docs are absorbed via source `verified` / `independent` flags — not a separate enum value.

### 5. Comparability Strength High / Moderate / Limited

Not implemented as named categories. Comparables affect asset/development **scores** and evidence ranks only.

### 6. Key-count eligibility

No implemented key-count hard gate in `eligibility.js`.

### 7. Owner structure preferences on Deal C

Shadow `preferredStructures: null` and pilot path pass empty structures → structure alignment **unknown** for Deal C top operators. Validate Next correctly asks to capture / confirm structures. Older narrative implying confirmed structure match for Deal C #1 would be inaccurate for the current path.

### 8. Ranking when scores tie

Prior materials may imply material score separation between Santa Fe and Highgate on Deal C. **Current math:** identical 38.6; order is **stable candidateId tie-break**. Spec documents this explicitly.

---

## Zero / non-issues verified aligned with code

- Evidence ceilings Limited 69 / Moderate 84 / Strong uncapped — confirmed in `config.js`  
- Unknown-in-denominator rule — confirmed in `aggregateOperatorProjectAlignment`  
- Ranking order eligibility → displayed → evidence → coverage → risk → ID — confirmed in `top5-selector.js`  
- Ranking Ready 50% threshold — confirmed in `readiness.js`  
- Table-stakes tokens never positive differentiators — confirmed  
- Brand relationship ≠ project approval — confirmed in brand compatibility + depth helpers  
- Option C band-first owner presentation — confirmed in `owner-presentation.js`  
- Owner pilot disabled / My Deals unwired — unchanged  

---

## Action

None in this assignment. Optional future product tasks (not authorized here): pass Deal C preferred brands into pilot project adaptation; capture owner structure preferences on the deal record before owner enablement.
