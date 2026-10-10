# Customer-Ready Contract Audit — 2026-10-03

## Definition (live code)

Customer-facing ⇔ `isActiveCustomerOpportunity` (surface) AND (`isGdiCustomerOpportunityReady` OR legacy compatibility).

`isGdiCustomerOpportunityReady` requires:

1. `isCustomerSurfaceActiveEligible` (entity, date, lodging/thesis depth, not stamped DQ)
2. title + organization
3. source URL
4. summary ADEQUATE/STRONG
5. whoResearchAttempted
6. hotel fit signal (score or why-hotel)
7. whyNow
8. recommendedAction

## Field realism

| field | required? | typical public availability | source types | failure frequency (subjects) | classification |
|---|---|---|---|---|---|
| valid_entity | true | HIGH | org site, association, event page | medium on subjects (directory/chrome) | ESSENTIAL_FOR_CUSTOMER_READY |
| future_timing | true | HIGH | event dates, cycle announcements | low-medium | ESSENTIAL_FOR_CUSTOMER_READY |
| geography_in_market | true | HIGH | venue/destination pages | medium (thin geo fields on subjects) | ESSENTIAL_FOR_CUSTOMER_READY |
| lodgingEvidence.roomBlockMentioned|housingPageFound | true | MEDIUM | housing pages, host hotel PDFs, housing bureau | HIGH on subjects — most rows NO_LODGING_SIGNAL | ESSENTIAL_FOR_CUSTOMER_READY |
| hotel_opportunity_thesis (non-boilerplate) | true | MEDIUM | analyst synthesis from public pages | HIGH — template thesis stripped as bare demand generator | ESSENTIAL_FOR_CUSTOMER_READY |
| who_research_attempted | true | MEDIUM | staff pages, contact forms, LinkedIn (manual) | HIGH — NOT_RESEARCHED common; named person rare | ESSENTIAL_FOR_CUSTOMER_READY |
| named_person | false | LOW | staff directory, press | very high | USEFUL_BUT_NOT_REQUIRED |
| summary ADEQUATE/STRONG | true | MEDIUM | compiled from public facts | HIGH when enrichment skipped | ESSENTIAL_FOR_CUSTOMER_READY |
| why_now + recommended_action | true | MEDIUM | sales synthesis | HIGH on thin bags | ESSENTIAL_FOR_CUSTOMER_READY |
| hotelFitScore | false | INTERNAL | fit model | medium | USEFUL_BUT_NOT_REQUIRED |
| Surfe identity / private email | false | LOW | paid enrichment | n/a (off) | INTERNAL_RESEARCH_FIELD |

## Can competent public research satisfy every required field?

**Mostly yes**, with two friction points:

1. **Lodging mentioned flags** — public housing pages exist for many events, but bags often lack stamped `roomBlockMentioned` / `housingPageFound`; template theses are stripped → surface death.
2. **WHO research stamp** — named contacts are rare; org/functional/ceiling paths are allowed but frequently **not stamped**, so readiness fails on NOT_RESEARCHED.

Neither requires Surfe/private email. Contract is strict but **not inherently impossible** for public data — subject bags are under-enriched relative to Bethesda/NYC.

PUBLIC-DATA CONTRACT UNREALISTIC: **NO** — Bethesda/NYC satisfy the same contract with public evidence; subject zero-yield is enrichment/timing-stamp depth, not an impossible field set.
