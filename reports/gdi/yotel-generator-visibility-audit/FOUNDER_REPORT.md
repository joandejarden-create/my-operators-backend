# YOTEL GDI Demand Generator Visibility Audit

## A. Executive Summary

YOTEL looked empty because **8 of 10 known generators were never created canonically**, and the UI only showed account-level customer-ready opportunities. AidEx existed as an opportunity; the rest were research memory / report artifacts only.

**Repair:** registered **10** visible demand campaigns (generators). Child decomposition still **not run** (0 children). Thresholds unchanged. No speculative opportunities created.

## B. Pre-repair scoreboard

| Generator | Pre-repair |
|-----------|------------|
| AidEx Geneva 2026 | OPP_FOUND |
| Geneva Health Forum 2026 | REPORT_ONLY |
| CHI Geneva Centennial | MISSING |
| WHO Executive Board 160th Session | REPORT_ONLY |
| Art Genève 2027 | MISSING |
| Watches & Wonders 2027 | MISSING |
| SETAC Europe 37th Annual Meeting | MISSING |
| World Health Assembly 80 | REPORT_ONLY |
| ECOSOC Humanitarian Affairs Segment | MISSING |
| AI for Good Global Summit 2027 | MISSING |

## C. Post-repair

Visible campaigns: **10**  
Customer-facing opportunities (strict): **1**  
Child research leads across 10: **0** (decomposition not yet run)

## D. Generator vs opportunity contract

`isGdiDemandGeneratorVisible` ≠ `isGdiCustomerOpportunityReady` / `isValidFutureWatch`. Separated.

## E. Empty state

Fixed: when campaigns > 0 and opportunities = 0, UI states campaigns are being researched.

## F. Acceptance

- Future generators visible as campaigns: YES  
- Ready/Watch still strict: YES  
- No threshold cuts: YES  
