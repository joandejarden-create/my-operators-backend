# Operator Fit Differentiation Audit — Founder Review

**Date:** 2026-08-04  
**Status:** Audit complete · **No scoring / UI / Airtable / enablement changes**

---

## 1. What triggered the audit

Deal C’s top two Ranking Ready operators — **Grupo Hotelero Santa Fe** and **Highgate** — produce **identical** engine math (59.0 → 48.6 → −10 → **38.6** Displayed, Strong evidence, Limited band, With Conditions), yet owner cards show **different** comparable stories. Rank #1 vs #2 is only a stable ID sort.

## 2. The tie (exact)

| | Santa Fe | Highgate |
| - | -------: | -------: |
| Displayed | 38.6 | 38.6 |
| Rank | 1 | 2 |
| Tie-break | `reckyv9…` sorts first | `recLjxt…` |

No hidden decimal difference.

## 3. Why the tie exists

Every ranking key ties. Differences in real evidence are **collapsed into the same factor buckets** (especially Asset/Development = 80 and Geography = 78). Brand and structure layers are **N/A / unknown** on the current Deal C evaluation path, so they add no separation. Risk −10 is the **same two unknown items** for both.

## 4. Factor-level differences

| Factor | SF | HG | Differentiates numerically? |
| ------ | -: | -: | --------------------------- |
| Geography | 78 | 78 | No |
| Segment | 100 | 100 | No |
| Asset/development | 80 | 80 | **No — despite different comps** |
| Complexity | 0 | 0 | No |
| Brand / governance | N/A | N/A | No |
| Regional / commercial | unknown | unknown | No |

## 5. Comparable scoring

Comparables **do** affect the asset score (`70 + 10×relevant count`) but **Comparability Strength is not implemented**. Mexico portfolio vs DR Ocean Club both count as one “relevant” comp → both **80**. Highgate’s **Aloft Tulum (Mexico)** is often not treated as relevant for this Deal C filter — lost signal. Why text still shows different property names → **Class B narrative/score mismatch**.

Audit-only CRI simulations show more granular comparable math *can* move scores, but naive multi-comp bonuses can also reverse intuition — any future CRI must avoid double-counting.

## 6–7. Geography & development

Geography (22) is city/country/none only — **not** presence-type depth. Development (20) composites many modes into one score — Deal C leaders bucket together.

## 8–11. Brand, governance, regional, commercial

- **Brand:** empty preferred brands on Deal C path → N/A for both.  
- **Governance:** no owner reporting requirements → N/A.  
- **Regional:** unknown both → 0 points **and** −5 risk.  
- **Commercial:** unknown both; table-stakes correctly earn 0.

## 12–13. The −10 risk

| Item | Kind | Points | Double penalty? |
| ---- | ---- | -----: | --------------- |
| Structure unconfirmed | unknown_validation | 5 | Yes — also 0 in 15% layer + coverage |
| Regional unconfirmed | unknown_validation | 5 | Yes — also 0 factor + coverage |

**No confirmed risks** in this −10. If unknowns were validation-only, both would show **48.6 Potential** instead of **38.6 Limited**.

## 14–16. Compression & distribution

70/15/15 verified. Unknown structure compresses Deal C (59→48.6). Across 126 eligible samples: displayed mean ~34, **Limited-heavy** after risk/ceilings; exact-tie members 44; near ties within 1 pt: 247 pairs. RR subset still has exact ties (incl. Deal C). Engine is not globally flat — Deals D/E/F separate — but ties and Limited mass are real.

## 17. Narrative vs score

Deal C = **Class B** (narrative differentiates; score does not). Deal E = Class C (score differs; Why similar). Others mostly Class A.

## 18. Owner candidate-set

Ranking Ready ≠ “good enough to lead.” Deal C RR set is **five Limited** operators. Policy Option **D (tiered)** is the honest frame: today Deal C has **no Leading/Potential tier** — only Additional/Validation — unless risk/data policy changes.

## 19. Tie presentation

Do **not** show owner #1/#2 on Δ&lt;1. Prefer co-lead / “Leading candidates” / bands. Keep ID tie-break **internal**.

## 20. Performance layer

Absent by design. Keep **Project Fit** separate from future **Performance Evidence**.

## 21. Root causes (top)

1. Narrative/ordinal vs tied score (Critical)  
2. Comparable underuse / bucket collapse (Critical)  
3. Unknown double-penalty risk (High)  
4. Coarse geography (High)  
5. Thin Deal C brand/structure path data (High)

## 22–24. Remediation options

1. **Minimal:** tie-aware UI + candidate tiers + optional unknown-risk policy / data path fill  
2. **Moderate:** CRI + geography depth; keep top weights  
3. **Structural:** new factors / performance layer / rethink 70/15/15  

## 25. Recommended boundary

**B — Targeted scoring refinement**, gated behind **A — Presentation-only** before any owner sees ordinals.

Not “presentation only forever” (real data differences are scored away). Not full structural rebuild yet (other deals show useful spread).

## 26. Founder decisions required before any change

1. Approve tie presentation rule (Δ&lt;1 = tie)?  
2. Approve owner candidate-set Option D (or alternative)?  
3. Approve reviewing unknown-only risk as double penalty (policy change)?  
4. Authorize comparable/geography granularity work **without** changing top-level weights?  
5. Authorize Deal C path data (structures / brands / regional) before enablement?  
6. Explicitly **do not** enable owner pilot until presentation honesty lands?

---

**Artifacts:** baseline, tie reproduction, comparable/geo/dev/risk audits, compression, narrative consistency, root cause, remediation options, CRI simulation, policy docs.  
**Harness:** `scripts/operator-fit-differentiation-audit.mjs` (audit-only).
