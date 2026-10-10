# GDI Opportunity Discovery Rebuild V5

## A. Executive Summary

Buyer-first hotel-demand motion discovery is implemented and run across five hotels. Funnel enforces SIGNAL → RESEARCH_LEAD (≥2 supporting signals) → page-validated CANDIDATE → canonical CUSTOMER_READY. Thresholds unchanged. Jev advisory only after admission.

**Controls analyzed:** 58 Bethesda/NYC ready opportunities.
**V5 totals:** signals=182, research leads=154, candidates=34, ready=0, watch=0.

## B. Why Bethesda / NYC Worked

Successful ready opportunities combine named organizer/buyer, future timing/cycle, explicit hotel-motion thesis, public contact or housing path, and market-credible venue — often discovered via housing pages, association site-selection, procurement, or competitor host patterns rather than bare event calendars.

## C. Why Recent Markets Failed

V3/V4 discovery optimized for "things happening" (conference exists, company news, project announced). Thin SERP hits rarely carried lodging + buyer + future decision together. Page completion can resolve WHO/timing/lodging blockers, but cannot rescue low-quality leads that never had a hotel-demand motion.

## D. Signal vs Research Lead vs Candidate vs Opportunity

| Stage | Rule |
|-------|------|
| SIGNAL | Market activity only |
| RESEARCH_LEAD | Entity + market + future group motion + ≥2 support signals |
| CANDIDATE | Org + motion + future + lodging + fit + buyer path; **page-validated** |
| CUSTOMER_READY | Canonical `isGdiCustomerOpportunityReady` |

No SIGNAL → CANDIDATE shortcut.

## E. Successful Opportunity Pattern

See `SUCCESS_PATTERN_ANALYSIS.md`. Min supporting signals for research admission: **2**.

Top structural differences vs zero-yield:
- Named organizer/buyer entity (not SERP title fragment)
- Future date or recurring cycle with decision window
- Explicit hotel-motion thesis (overflow / housing / primary / stay-to-play)
- Public contact OR housing/organizer path
- Market-credible destination/venue — not generic activity news
- Often discovered via housing pages, association site-selection, procurement, or competitor host patterns — not bare event calendars

## F. Buyer-First Discovery

`resolveGdiDemandBuyer()` resolves entity/function first (association, housing bureau, DMC, procurement, federation, contractor, program office, etc.). Named person not required at admission.

Buyer entities resolved: **32**. Public contact paths: **16**.

## G. Hotel-Demand Motion Queries

Query library targets accommodation / room block / housing / RFP / organizer / DMC / housing bureau — localized FR/ES/GL/DE where configured. See `BUYER_FIRST_QUERY_LIBRARY.csv` (60 selected priority queries executed subset).

## H. Competitive Demand Mining

`buildGdiCompetitiveDemandLead()` builds org + historic hotel + meeting type + cycle + compete-next thesis. Competitive leads: **3**.

## I. Repeat / Rotation Intelligence

States: CONFIRMED_FUTURE / RECURRING_EXPECTED / ROTATION_PREDICTED / FUTURE_UNCONFIRMED / HISTORICAL_ONLY. No invented future dates. Rotation rows: **40**.

## J. Multilingual + Feeder Market Results

Multilingual signals: **1**. Feeder-market signals: **38**.
originMarket tracked separately from lodgingMarket.

## K. Hotel Results

| Hotel | Signals | Leads | Candidates | Ready | Watch |
|-------|--------:|------:|-----------:|------:|------:|
| YOTEL | 33 | 28 | 6 | 0 | 0 |
| AC | 39 | 34 | 7 | 0 | 0 |
| SPICE | 37 | 31 | 6 | 0 | 0 |
| CAMBRIDGE | 37 | 35 | 7 | 0 | 0 |
| NOW NOW | 36 | 26 | 8 | 0 | 0 |

## L. Old vs New Funnel

| Metric | OLD V3 | NEW V5 |
|--------|-------:|-------:|
| Signals | 156 | 182 |
| Research leads | 156 | 154 |
| Candidates | 156 | 34 |
| Ready | 0 | 0 |
| Watch | 0 | 0 |
| Candidate→useful % | 0 | 0 |
| Cost USD | 2.7 | 3.40 |

V5 goal: fewer weak candidates, higher conversion — admission rejects thin activity before research spend.

## M. Cost / Research Efficiency

Incremental cost: **$3.40**. Cost/useful: **n/a**. See `COST_REPORT.md`.

## N. Recommended Default GDI Discovery Architecture

1. Buyer-first hotel-demand query library (destination + feeder markets; multilingual where justified).
2. SERP = SIGNAL only.
3. `isGdiResearchLeadWorthPursuing` (≥2 support signals) before any page/Jev spend.
4. Page-level validation before CANDIDATE.
5. `resolveGdiDemandBuyer` (entity/function).
6. Competitive + rotation intelligence for next decision point.
7. Jev advisory only after admission (blocker/source/stop) — never truth/promotion/thresholds.
8. Canonical readiness + valid Future Watch unchanged.
