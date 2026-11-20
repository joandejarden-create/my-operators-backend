# SEC EDGAR Search Coverage & Adaptive Discovery V2

**Generated:** 2026-11-20T06:51:04.987Z

## Coverage matrix
- **IMPLEMENTED:** 9
- **PARTIAL:** 14
- **MISSING:** 0
- **NOT_APPLICABLE:** 1

See `SEARCH_COVERAGE_MATRIX.md`.

## Taxonomy
- Version: `sec-edgar-search-taxonomy-v2.1.0`
- Families: 14
- Patterns: 127
- Legacy contract keywords (audit baseline): 8

## DiamondRock fixture eval
- Hotel recall: **1**
- Expiration recall: **1**
- Hotels discovered: 24
- False-positive hotel count: 0

## Coverage accounting
- Eligible filings (if inventory built): 3863
- Processed (checkpoint): 50
- Remaining: 3813
- Durable hotels: 1612

## Build eligible inventories
```bash
npm run sec-edgar-search-coverage:eval -- --build-eligible --companies diamondrock-hospitality,marriott-international,hilton-worldwide,sunstone-hotel-investors,host-hotels-resorts,ashford-hospitality-trust
```
