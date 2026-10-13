# GDI Comp Set Demand Mining V1

## A. Executive Summary

Systemic competitor-demand mining is implemented: ADP-canonical (or documented YOTEL fallback) comp sets → public search pivots (name/alias/phone/address/domain) → page-level validation → evidence classes → repeat/cross-competitor patterns → buyer resolution → target-hotel thesis → canonical GDI gates.

**20 competitors** searched · **30 traces** · **5 DIRECT / 1 STRONG** · **5 GDI candidates** · **0 ready / 0 watch**.

## B. Why Competitor Demand Mining Matters

Bethesda/NYC-quality opportunities often leave public lodging footprints at competitor hotels. Mining those footprints finds **buyers of hotel rooms** with historic proof — not generic “events happening.”

## C. Comp Sets Searched

| Hotel | Source | Competitors |
|-------|--------|-------------|
| YOTEL | GDI_DOCUMENTED_FALLBACK_NO_ADP_DECLARED | 4 |
| AC | ADP_DECLARED_COMP_SET | 4 |
| SPICE | ADP_DECLARED_COMP_SET | 4 |
| CAMBRIDGE | ADP_DECLARED_COMP_SET | 4 |
| NOW_NOW | ADP_DECLARED_COMP_SET | 4 |

See `COMP_SET_CANONICAL.csv`. Phones/addresses filled only when publicly extracted — never invented.

## D. Phone / Alias / Address Search Yield

- Phone-search hit rows: **6**
- Phone → useful (DIRECT/STRONG) yield: **0%**
- Top pivot by useful yield: **EXACT_HOTEL_NAME**
- Alias / address / domain pivots included in `SEARCH_PIVOTS.csv`

**Rule enforced:** phone hit = page pointer only until page context confirms group + competitor lodging/event tie.

## E. Validated Group Traces

- Total traces: **30**
- DIRECT_CONFIRMED: **5**
- STRONG_ASSOCIATION: **1**
- Only DIRECT/STRONG feed deeper pattern/opportunity research.

## F. Repeat / Recurring Demand

Repeat patterns: **5**. Future cycles identified: **5**.  
Top repeat groups: none

Cadence states are evidence-backed only — no fabricated next dates.

## G. Buyer / Organizer Intelligence

Buyer entities resolved: **0**. Public contact paths: **0**.  
Top patterns: none

WHO uses public sources only; Surfe not used.

## H. Competitive Demand Patterns

Cross-competitor patterns: **5** (same org across hotels/cycles when evidenced).

## I. Target Hotel Opportunity Thesis

STRONG_FIT: **0** · PLAUSIBLE_FIT: **5**  
Theses separate FACT / INFERENCE / UNKNOWN. Win angles are hypotheses only.

## J. Hotel Results

| Hotel | Comps | Traces | Deep | Candidates | Ready | Watch |
|-------|------:|-------:|-----:|-----------:|------:|------:|
| YOTEL | 4 | 6 | 1 | 1 | 0 | 0 |
| AC | 4 | 6 | 0 | 0 | 0 | 0 |
| SPICE | 4 | 6 | 2 | 2 | 0 | 0 |
| CAMBRIDGE | 4 | 6 | 0 | 0 | 0 | 0 |
| NOW NOW | 4 | 6 | 2 | 2 | 0 | 0 |

## K. Precision / QA

Sample audited: **30**. Precision (TRUE_COMP_DEMAND / VALID_REPEAT): **16.7%**.  
See `QUALITY_AUDIT.csv`.

## L. Cost / Research Efficiency

Incremental cost: **$3.92**. Details in `COST_REPORT.md`.

## M. Recommended Default GDI Behavior

1. Resolve ADP (or documented fallback) comp set — never invent comps.
2. Build public pivots (name, alias, phone variants, address, domain, meeting rooms).
3. SERP = pointer; **open the page**.
4. Classify evidence; only DIRECT/STRONG deepen.
5. Mine repeat + cross-competitor patterns without fabricating cycles.
6. Resolve buyer entity/function from public sources.
7. Build target thesis with FACT/INFERENCE/UNKNOWN + fit class.
8. Convert via `isGdiResearchLeadWorthPursuing` + canonical ready/watch — no manual promotion.
9. Jev advises next research only after a validated pattern exists.
