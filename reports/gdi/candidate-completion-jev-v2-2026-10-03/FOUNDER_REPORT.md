# GDI Candidate Completion + Jev Research Controller V2

**Date:** 2026-10-03  
**Mode:** MODE B — page-level completion on V3 discovery candidates  
**Law:** Jev decides WHERE / HOW FAR to look. Dealality decides WHAT is true.

## A. Executive Summary

V3 produced **156** new candidates and **0** qualified / customer-ready / valid future watch promotions — correctly refusing thin SERP hits. This phase rebuilt a researchable pool of **116** candidates from V3 scout CSVs (+ association series), prioritized P0/P1, and ran page-level completion with adaptive depth (hard cap 3).

| Metric | Count |
|--------|------:|
| V3 official candidates | 156 |
| Research pool (reconstructed) | 116 |
| P0 | 17 |
| P1 | 13 |
| Fully researched | 30 |
| CUSTOMER_READY | 0 |
| VALID_FUTURE_WATCH | 0 |
| REJECTED_CONFIRMED | 116 |
| UNRESOLVED_PUBLIC_DATA_CEILING | 0 |

**AssociationScout note:** V3 did not persist AssociationScout detail rows to a RESULTS.csv (SCOUT_YIELD shows **48** Association hits). Pool uses detail scout CSVs + ASSOCIATION_SERIES rows only. No new broad discovery was run.

## B. Why V3 Produced 156 Candidates but 0 Qualified

Discovery stopped at SERP snippets. Qualification requires page-level lodging, timing, WHO/contact, and surface thesis. Multi-blocker thin hits correctly failed `isGdiCustomerOpportunityReady` / `isValidFutureWatch` — architecture working as designed. Bottleneck = completion, not thresholds.

## C. Jev's Role

Advisory only:
- next question / source family / language / feeder / depth / stop
- **never** verified facts, promotion, rejection, threshold changes, or canonical writes

Deterministic controller accepts/rejects Jev advice under depth policy + hard caps.

## D. Candidate Priority Logic

Deterministic `buildGdiCompletionPriority()`: lodging hint, procurement/RFP, housing URL, named organizer, future timing, repeat series, known venue, hotel fit, public contact route, overnight/travel language → lodging-bearing P0 (score ≥5) / P1 medium / P2 defer. Thesis boilerplate excluded from lodging scoring.

## E. Adaptive Research Depth

| Priority | Default depth |
|----------|---------------|
| P0 | up to 2 steps (`DEPTH_2`) |
| P1 | 1 step (`DEPTH_1`) |
| P2 | none unless high-info Jev exception |
| DEPTH_3 | only strong fit + entity + geo + lodging partial + commercial + cost |

Hard cap: **3** completion steps/candidate.

## F. YOTEL Geneva Lake

V3 pool 38 · P0 5 · P1 5 · researched 10 · ready 0 · watch 0 · rejected 38 · ceiling 0

## G. AC A Coruña

V3 pool 30 · P0 4 · P1 5 · researched 9 · ready 0 · watch 0 · rejected 30 · ceiling 0

## H. Spice Island

V3 pool 15 · P0 2 · P1 1 · researched 3 · ready 0 · watch 0 · rejected 15 · ceiling 0

## I. Cambridge Beaches

V3 pool 17 · P0 3 · P1 0 · researched 3 · ready 0 · watch 0 · rejected 17 · ceiling 0

## J. NOW NOW

V3 pool 16 · P0 3 · P1 2 · researched 5 · ready 0 · watch 0 · rejected 16 · ceiling 0

## K. Timing Findings

Timing remains the systemic kill gate. Page extraction only stamps dates when public future patterns appear — no invented dates. Timing blockers resolved (partial/pass after fail): **17**.

## L. Lodging Findings

Lodging classes used: DIRECT_ROOM_BLOCK / OFFICIAL_HOUSING_PROGRAM / OFFICIAL_ACCOMMODATION_GUIDANCE / TRAVELING_DELEGATION / MULTI_DAY_GROUP_INFERENCE / NO_LODGING_SUPPORT. Attendance or duration alone never counts as room demand. Lodging blockers improved: **10**.

## M. WHO Findings

WHO resolved (email or org contact URL): **15**. Surfe not used. Public-data ceiling stamped when contact research attempted without path.

## N. Rotation / Repeat Findings

Series IDs from association series retained where present. No invented historic hosts. Further historical-cycle research stopped when page evidence did not establish recurrence.

## O. Competitor Demand Findings

Competitor FACT vs INFERENCE kept separate. No hosting inferred without evidence. See COMPETITOR_DEMAND_COMPLETION.csv.

## P. Jev Performance

| Metric | Value |
|--------|------:|
| Recommendations issued | 75 |
| Accepted | 75 |
| Rejected by policy | 0 |
| Resolving blockers | 20 |
| Changing classification | 0 |
| Low-yield | 2 |

## Q. Research Depth Economics

| Depth | N | Useful | Avg cost |
|-------|--:|-------:|---------:|
| DEPTH_1 | 13 | 0 | 0.010 |
| DEPTH_2 | 17 | 0 | 0.068 |
| DEPTH_3 | 0 | 0 | 0 |

Cost/useful = N/A USD.

## R. Demand Engine Yield

Top by completed useful yield: **Association / NGO (0 useful / 109 candidates)**. Full table in DEMAND_ENGINE_YIELD.csv.

## S. Language / Feeder Market Yield

Top language by useful: **en (0)**.  
Top feeder by useful: **NONE (0)**.  
Judged from completed useful opportunities, not SERP volume.

## T. Recommended Default GDI Research Behavior

| Control | Recommendation |
|---------|----------------|
| Candidate prioritization | **NO** (keep deterministic baseline) |
| Next blocker selection | **CONDITIONAL** |
| Source selection | **CONDITIONAL** |
| Language selection | **NO** |
| Feeder-market selection | **NO** |
| Research-depth decision | **NO** |
| Stop/continue decision | **CONDITIONAL** |

Based on measured yield this run — not theory.

---

### Safety attestations

| Check | Result |
|-------|--------|
| GDI thresholds changed? | **NO** |
| Jev wrote verified facts? | **NO** |
| Jev promoted opportunities? | **NO** |
| Watch quality standard bypassed? | **NO** |
| New broad discovery run? | **NO** |
| ADP changed? | **NO** |
| Share tokens changed? | **NO** |

### Final verdict

Page-level completion confirmed V3 thinness: **0** useful promotions after bounded P0/P1 research. Depth policy + public-data ceilings terminated research without threshold cuts. Jev stop/continue economics are the primary learning for default GDI behavior.

**STOP.**
