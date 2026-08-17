# Choice Hotels International — FDD inventory (Dealality folder)

**Folder:** `G:\My Drive\Dealality™\Platform Design & Build\Brand Reference Material\Choice Hotels International`

**Last scanned:** May 2026 — **29 unique numbered FDD PDFs** (was 16 on prior scan)

## Summary

| Status | Brands |
|--------|--------|
| Item 19 CP + Enterprise/Proprietary % | 16 brands |
| FDD on file, no CP % in Item 19 | Everhome, Radisson Individual, WoodSpring |
| No FDD in folder yet | Radisson RED, Radisson Collection, Park Plaza, Radisson Inn & Suites |

The **Radisson Red** subfolder duplicates root FDDs; filenames are state registration IDs, not brand names.

## FDD count by brand (latest filing used)

| Brand | Primary FDD file | FY | CP contribution | Enterprise / Proprietary |
|-------|------------------|-----|-----------------|--------------------------|
| Ascend Hotel Collection | 35768-202604-08 | 2025 | 45.0% | Proprietary 45.8% |
| Cambria Hotels | 35798-202604-03 | 2025 | 50.8% | Proprietary 51.9% |
| Clarion | 35770-202604-09 | 2025 | 41.1% | Enterprise 75.9% |
| Clarion Pointe | (same FDD as Clarion) | 2025 | 41.1%* | 75.9%* |
| Comfort Inn & Suites | 35771-202604-09 | 2025 | 51.8% | Enterprise 81.3% |
| Country Inn & Suites by Radisson | 35772-202604-09 | 2025 | 43.7% | Enterprise 81.8% |
| Econo Lodge | 35773-202604-09 | 2025 | 33.8% | Enterprise 57.9% |
| Everhome Suites | 35774-202604-09 | — | — | No performance rep |
| MainStay Suites | 35775-202604-09 | 2025 | 50.7% | Enterprise 74.6% |
| Park Inn by Radisson | 35776-202604-09 | 2025 | 49.5% | Enterprise 80.8% |
| Quality Inn | 35778-202604-09 | 2025 | 44.9% | Enterprise 74.2% |
| Radisson (Choice) | 35779-202604-10 | 2025 | 36.3% | Proprietary 39.6% |
| Radisson Blu (Choice) | 35781-202604-09 | 2025 | 39.6% | Proprietary 43.3% |
| Radisson Individual (Choice) | 35782-202604-05 | — | — | No performance rep |
| Rodeway Inn | 35784-202604-09 | 2025 | 26.8% | Enterprise 51.1% |
| Sleep Inn | 35785-202604-09 | 2025 | 48.8% | Enterprise 78.8% |
| Suburban Studios | 35786-202604-09 | 2025 | 44.9% | Enterprise 61.6% |
| WoodSpring Suites | 33395-202504-08 | 2024 | — | Occ/ADR/RevPAR tables only |

\*Combined CLARION + CLARION POINTE performance sample in one FDD.

## All numbered FDD files (29)

`30940`, `30944`, `30945`, `33372`, `33374`, `33378`, `33379`, `33385`, `33389`, `33395`, `34297`, `35768`, `35770`, `35771`, `35772` (×2), `35773`, `35774` (×2), `35775`, `35776`, `35778`, `35779`, `35781`, `35782`, `35784`, `35785`, `35786`, `35798`

## Tooling

```bash
node scripts/extract-choice-fdd-item19.mjs
node scripts/parse-choice-fdd-item19.mjs
node scripts/apply-choice-loyalty-commercial-batch.mjs --overwrite
node scripts/apply-choice-fee-structure-batch.mjs --dry-run   # Brand Setup - Fee Structure (Item 6 + profiles)
node scripts/apply-choice-fee-structure-batch.mjs --overwrite
node scripts/apply-choice-deal-terms-batch.mjs --dry-run
node scripts/apply-choice-deal-terms-batch.mjs --overwrite
```

Constants: `scripts/lib/choice-fdd-item19.mjs`  
Text: `fixtures/choice-fdd-text/`
