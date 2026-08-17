# Brand Explorer 39 — ADR / Forbidden Language Scrub

Applied as part of final freeze-blocker cleanup (v2 + residual valueOwners pass).

## Primary targets (original Lane B)
- small-luxury-hotels-of-the-world
- suburban-studios
- trademark-collection-by-wyndham
- woodspring-suites

## Residual Active/Live targets (same failure class)
Overview / scenario ADR tokens remaining after Wave 12:
- ascend, comfort-inn-suites, country-inn-suites, everhome-suites, hotel-indigo, kimpton, mgallery-collection, preferred-hotels-and-resorts, quality-inn, radisson, radisson-blu, radisson-individuals-by-choice

## Display unblock
- bw-premier-collection: `fee stack` → `affiliation economics` (avoid mechanical `participation cost categories` token)

## ValueOwners scenario ADR (quality-audit blockers)
Scrubbed `valueOwners.scenario.*` Body fields on:
- voco-hotels, avid-hotels, holiday-inn-express, suburban-studios, woodspring-suites
- quality-inn, radisson, radisson-blu, radisson-red, tempo-by-hilton

## Rules applied
- no ADR / RevPAR / fee-stack / FDD / Item 19 / LOI / raw URLs in owner-facing Presentation copy
- no Company Validated / Source Library / Registry / Brand Status / release field writes
