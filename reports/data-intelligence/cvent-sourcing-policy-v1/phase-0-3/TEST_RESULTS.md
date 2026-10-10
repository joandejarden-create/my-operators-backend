# Test Results — source-policy-v1 Cvent gates

**Commands:**
- `node scripts/test-source-policy-cvent-v1.mjs` → **10 / 10 PASS**
- `node scripts/test-census-cvent-venue-parse.mjs` → **PASS** (Choice matcher discovery-only; no Rooms/Keys write)
- `node scripts/test-census-cvent-latam-harvest.mjs` → **PASS** (LATAM update/insert discovery-only)

**Airtable writes:** 0

| # | Assertion | Result |
|---|-----------|--------|
| 1 | Cvent venue Rooms cannot persist canonical | PASS |
| 2 | Cvent venue meeting-space cannot persist canonical | PASS |
| 3 | Cvent description cannot display as verified customer truth | PASS |
| 4 | Cvent-only HI fact cannot activate verified ADP attribute | PASS |
| 5 | Cvent-only hotel capability cannot influence verified GDI fit | PASS |
| 6 | Cvent event evidence not globally blocked | PASS |
| 7 | Cvent discovery can create research candidate | PASS |
| 8 | Independent first-party verification can promote candidate | PASS |
| 9 | Mixed Cvent + verified follows verified provenance | PASS |
| 10 | Unknown remains unknown without verification (+ external policy block) | PASS |
| — | Choice matcher strips Rooms/Keys / description / meeting flag | PASS |
| — | LATAM matcher update/insert strips Rooms/Keys / description | PASS |

Policy version under test: `source-policy-v1`
