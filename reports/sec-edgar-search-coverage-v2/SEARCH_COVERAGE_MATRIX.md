# SEC EDGAR Search Coverage Matrix

**Version:** `sec-edgar-search-coverage-matrix-v2`  
**Taxonomy:** `sec-edgar-search-taxonomy-v2.1.0`  
**Generated:** 2026-11-20T06:51:01.662Z

| Status | Count |
|---|---|
| IMPLEMENTED | 9 |
| PARTIAL | 14 |
| MISSING | 0 |
| NOT_APPLICABLE | 1 |

## Pilot limit notes

- **v1_sinceYear_default:** currentYear - 3 (filing-discovery-v1)
- **v1_pilot_sinceYear:** currentYear - 2
- **v1_pilot_filing_limits:** REIT 10–12, brand/franchisor 6 (V1 pilot); V2 historical uses batchSize against full eligible inventory
- **v1_contract_keywords:** 8
- **v2_taxonomy_families:** 14
- **v2_historical_sinceYear_default:** 1994

## Coverage rows

| Area | Status | Evidence | Gap |
|---|---|---|---|
| SEC registrants and historical CIKs | PARTIAL | V1 pilot 4 cos + V2 registry (Hilton, DiamondRock, Park, RLJ, Apple, PEB, CLDT, XHR, INN). Live ticker→CIK resolve. Predecessor CIK graph incomplete. | Need systematic predecessor/successor CIK mapping and dissolved-entity harvest. |
| Company aliases and predecessor names | PARTIAL | Brand-family aliases + f/k/a taxonomy patterns. No dedicated registrant alias registry beyond CIK map. | Registrant legal-name alias table (e.g. Hilton Hotels Corp → Hilton Worldwide). |
| Filing form types (10-K, 8-K, S-11, 424B, …) | IMPLEMENTED | V1 forms=11; V2 historical forms=27 incl S-11/424B/DEF 14A. | — |
| Exhibit document types (EX-10, schedules) | PARTIAL | EX-10 treated as form match when present in submissions.form; primaryDocument download. No exhibit-index crawl of all EX-10.* within a filing package. | Parse filing index.json / FilingSummary to enumerate exhibits separately from form types. |
| Historical filing years | PARTIAL | V1 pilot used ~2-year window (sinceYear = current-2). V2 default sinceYear=1994 + additional submissions shards. Pilot runner still can pass narrow windows. | Make complete eligible inventory the default path; keep short windows only for smoke tests. |
| Additional historical submission files | IMPLEMENTED | loadAllSubmissionBodies + getSubmissionsAdditionalFile. | — |
| Complete eligible filing inventory before download | PARTIAL | listHistoricalFilings returns eligibleCount + remainingInQueue; batch limit still truncates returned set. | Persist full eligible inventory artifact per CIK before any document fetch. |
| Search-term families (14 taxonomy families) | IMPLEMENTED | 14 versioned families in search-taxonomy-v2 (sec-edgar-search-taxonomy-v2.1.0). | Continuous learning loop to promote high-yield phrases. |
| Legacy 8 contract keyword patterns | IMPLEMENTED | Still in contract-extractor-v1; taxonomy now expands extractor patterns. | Do not rely on these 8 alone — already mitigated by taxonomy + schedule extractors. |
| Hotel name variants | PARTIAL | hotel-discovery-v1 + brand families + schedule extractor. Historical aliases field present but sparsely populated. | Systematic alias capture from f/k/a / conversion language. |
| Legal entity and subsidiary names | PARTIAL | Taxonomy patterns; HI Organizations persist path when matched. No subsidiary graph extractor. | Subsidiary-name extractor from Exhibit 21 / org charts. |
| Contract terminology | IMPLEMENTED | Taxonomy families signing/effectiveness/expiration/termination/renewal + verification classes. | — |
| Ownership and management terminology | PARTIAL | Taxonomy + opportunity/relationship planner. Limited owner/operator entity resolution from prose. | Named-entity owner/operator linker with evidence spans. |
| Brand conversion terminology | PARTIAL | Taxonomy brand_conversions + brand-family detector. Conversion events rarely persisted as structured milestones. | Structured conversion event findings with before/after brand. |
| Geographic variations | PARTIAL | US city/state parsers; country field default United States. Intl readiness adapter stub only. | Non-US geography normalization for 20-F/6-K property lists. |
| Structured property-table extraction | IMPLEMENTED | franchise-schedule-extractor-v2 (DiamondRock-style) + hotel-discovery schedule lines. | More table layouts (HTML <table>, XBRL). |
| Structured agreement-exhibit extraction | PARTIAL | Deterministic contract extractor on primary docs; EX-10 exhibit package crawl incomplete. | Exhibit-index driven EX-10.* download + extract. |
| Adaptive registrant↔hotel↔owner expansion | PARTIAL | adaptive-discovery-queue-v2 module; deduped queue with priority caps. | Auto-enqueue only after verified entity extraction quality gates. |
| HPC hotel → SEC company reverse path | PARTIAL | Seed HPC candidates + name match; reverse identity→CIK search not fully automated. | HPC Affiliation/Operator → ticker/CIK resolve → enqueue. |
| V1 pilot limits (2-year window, 6/12 filings) | PARTIAL | Still present on V1 pilot script for smoke/cost control. V2 historical runner uses batch/checkpoint instead of universe caps. | Document V1 limits as smoke-only; V2 inventory-first is the production research path. |
| Fair-access / cache / checkpoint | IMPLEMENTED | edgar-client rate limit + cache; research-checkpoint-v2. | — |
| Paid AI ceiling $25 | IMPLEMENTED | budget-guard-v1; paid AI off by default. | — |
| Durable unmatched hotel inventory | IMPLEMENTED | data/sec-edgar/discovery-inventory/hotels.json + MH inventory export. | Airtable unmatched-hotel table requires founder schema approval. |
| Customer publication | NOT_APPLICABLE | Blocked by PUBLICATION_GATE; not in scope. | — |
