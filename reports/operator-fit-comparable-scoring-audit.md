# Operator Fit — Comparable Scoring Audit

**Deal C · Santa Fe vs Highgate · Audit only**

## Santa Fe comparables available

| Property | Country | Dev / situation | Comparability Strength | Evidence class | Consumed by scoring? | Contribution |
| -------- | ------- | --------------- | ---------------------- | -------------- | -------------------- | ------------ |
| GSF Mexico third-party managed hotels (portfolio) | Multiple, Mexico | Operating / conversion | **null** (not implemented) | detailed / referenced via flags | Yes if “relevant” | Part of asset **80** (70+10×1) |
| City / brand / keys / mixed-use / residences | Mostly sparse on record | — | — | — | Partial heuristics only | Narrative via property name |

## Highgate comparables available

| Property | Country | Dev / situation | Comparability Strength | Consumed as relevant for Deal C? | Notes |
| -------- | ------- | --------------- | ---------------------- | -------------------------------- | ----- |
| The Ocean Club, Luxury Collection (DR) | Costa Norte, DR | Operating resort | null | **Yes** (resort/urban keyword path) | Appears in Why |
| Tambo del Inka (Peru) | Urubamba, Peru | Operating | null | Typically **no** for MX New Build/Mixed-Use | Available but not driving Deal C asset score |
| Aloft Tulum | Tulum, Mexico | Operating | null | Often **not** “relevant” under current filter despite Mexico | Lost Mexico depth signal |

## Answers (current implementation)

| # | Question | Answer |
| - | -------- | ------ |
| 1 | Does Comparability Strength affect numerical scoring? | **No** — not a controlled enum in engine; field unused |
| 2 | Does number of relevant comparables affect scoring? | **Yes** — `min(100, 70 + 10×n)` for relevant comps |
| 3 | One High vs five High? | No “High” level; five **relevant** comps → higher score than one |
| 4 | Geographic similarity in comparable scoring? | **Only** via relevance heuristic / string hay — no dedicated geo weight inside comps |
| 5 | Development-type similarity? | Partial string / keyword overlap |
| 6 | Segment similarity? | Via hotelType/segment strings if present (often null) |
| 7 | Brand similarity? | Not a dedicated comparable sub-score |
| 8 | Recency? | Not in asset factor |
| 9 | Repeated vs one example? | Count of **relevant** comps only; no “repeated verified” multiplier |
| 10 | Double-counted elsewhere? | Comps also feed Evidence Strength class; not double in other factors |
| 11 | Mostly narrative? | **For Deal C tie: yes for differentiation** — different texts, same score bucket |

## Verdict

Comparables are **partially numerical** but **low-resolution**. Deal C collapses distinct Mexico-portfolio vs DR-resort (and unused Mexico Aloft) into the **same asset score 80**, while Why This Operator still implies different fit stories. That is a **comparable underuse + granularity** issue, not pure narrative-only architecture.
