# Operator Alignment — Scoring Weight Model



**Date:** 2026-07-03 (audited against live engine)  

**FPP task:** 2.02 — Define Dealality scoring weights for deal, brand, and operator fit  

**Platform (admin):** Admin Resources → Scoring Weight Model (`/support/scoring-weight-model`)



---



## Source of truth (code)



| Surface | Config module | Engine |

|---------|---------------|--------|

| Operator fit | `lib/operator-alignment-scoring-weight-config.js` | `scoreOperatorMatchForDeal` in `api/my-deals.js` |

| Brand fit (Match Score New) | `lib/brand-match-scoring-weight-config.js` | `computeMatchScoreNew` via `computeMatchScoreForDealBrand` in `api/match-score-server.js` |

| Brand fit (legacy 19-factor) | `WEIGHTS` in `api/match-score-server.js` | Brand Development Dashboard (migration TBD) |



**Rule:** Change weights in config modules only — admin runbook page reads from those modules at request time.



---



## Operator match weights (pilot)



| Factor | Weight | MVP data quality |

|--------|--------|------------------|

| Geography & Markets | 18 | Partial |

| Chain Scale | 8 | Strong |

| Asset / Project / Stage Fit | 14 | Partial |

| Deal Structure / Assignment | 12 | Partial |

| Fee / Commercial | 10 | Weak |

| Service Offerings | 8 | Partial |

| Systems & Reporting | 6 | Partial |

| Owner Relations | 6 | Weak |

| Brand / Portfolio Relevance | 6 | Partial |

| Negative-Fit Penalty | 2 | Partial |



**Totals:** 88 positive-factor weight + 2 negative-fit weight = **90** documented engine total.



### Operator aggregation (deployed)



- `finalScore = sum(score × weight) / sum(weight)` for factors with **non-null** scores only.

- Missing data **excludes** a factor from the denominator.

- `negativeFitPenalty` is **not** a flat subtraction: it scores **20** when deal breakers overlap `lessIdealSituations`, else **100**, with weight **2**.



### Operator UI bands (deployed)



| Min score | Label | CSS class |

|-----------|-------|-----------|

| 80 | Strong alignment signals | `match-score-high` |

| 50 | Moderate alignment — review gaps | `match-score-medium` |

| 25 | Weak alignment — significant gaps | `match-score-weak` |

| 0 | Very limited alignment | `match-score-poor` |



Used via `window.DcOperatorMatchScoreUi` (fed by `/js/generated/operator-match-scoring-config.js`).

### UI wiring

- **Config module:** `lib/operator-alignment-scoring-weight-config.js`
- **Generated script:** `GET /js/generated/operator-match-scoring-config.js`
- **Client helper:** `public/js/operator-match-score-ui.js`
- **Pages:** `my-deals.html`, `operator-development-dashboard.html`



---



## Brand Match New weights



| Factor | Weight |

|--------|--------|

| Chain scale proximity | 10 |

| Service model alignment | 5 |

| Preferred brand | 8 |

| Project type compatibility | 10 |

| Building type compatibility | 5 |

| Project stage compatibility | 5 |

| Brand standards compatibility | 10 |

| Agreements type compatibility | 10 |

| Room range fit | 10 |

| Key money willingness | 12 |

| Incentives match | 5 |

| Fees tolerance | 10 |



**Total:** 100



### Brand aggregation (deployed)



- All **12 weights always** in the denominator.

- Null factor scores contribute **0** to the numerator but still reduce the total (unlike operator scoring).



---



## Audit notes (doc vs engine)



| Topic | Status |

|-------|--------|

| Operator factor weights | Match `OPERATOR_MATCH_WEIGHTS` → `api/my-deals.js` |

| Brand factor weights | Match `BRAND_MATCH_NEW_WEIGHTS` → `api/match-score-server.js` |

| Admin runbook page | Generated from config modules at API request time |

| Score bands | **80/50/25** in UI (was incorrectly documented as 75/55) |

| Negative fit | **Weighted factor**, not flat penalty (was incorrectly documented) |



---



## Change process



1. Edit `lib/operator-alignment-scoring-weight-config.js` and/or `lib/brand-match-scoring-weight-config.js`.

2. Run `node scripts/validate-operator-alignment-phase-5b.mjs`.

3. Spot-check operator match + brand recommendation on a sample deal.

4. Hard refresh Admin Resources → Scoring Weight Model; Joan signs off → FPP 2.02 Completed.



---



## Related docs



- [operator-alignment-field-matrix.md](./operator-alignment-field-matrix.md)

- [operator-alignment-scoring-data-quality-audit.md](./operator-alignment-scoring-data-quality-audit.md)

- [internal-resources-hub.md](./internal-resources-hub.md)

