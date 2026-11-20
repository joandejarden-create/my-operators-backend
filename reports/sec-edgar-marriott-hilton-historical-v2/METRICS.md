# Marriott & Hilton Historical Contract Intelligence V2

**Mode:** DRY_RUN_BATCH (checkpointed)  
**Program:** `SEC_EDGAR_HISTORICAL_INTELLIGENCE_V2`  
**Generated:** 2026-11-20

## Headline results (post identity-dedupe)

| Metric | Value |
|---|---|
| Historical period covered | 2004–present (capability from 1994) |
| Companies in research registry (priority ≤2) | Marriott, Hilton, DiamondRock, Host, Sunstone, Ashford, Park, RLJ, Apple, … |
| DiamondRock eligible filings found | 482 |
| Filings processed this wave (DRH+APLE+RLJ) | 50 checkpointed docs |
| Remaining in queue (those companies) | ~1,200+ |
| Unique hotels in durable inventory | **1,612** |
| Marriott-affiliated (inventory export) | **879** |
| Hilton-affiliated (inventory export) | **466** |
| Hotels with historically verified expirations (MH export) | Marriott 6+ / Hilton 15+ schedule-backed rows; broader hist-verified milestones present |
| Currently verified expirations | **0** (by design — no auto-promotion) |
| Hotels in HPC | **4** |
| Hotels missing from HPC | **1,608** |
| Paid research cost | **$0** |
| Airtable apply / read-back | Not run (dry-run only) |

## Coverage denominator

Batch finish ≠ research complete. Resume:

```bash
npm run sec-edgar-marriott-hilton-historical:v2 -- --companies diamondrock-hospitality,apple-hospitality-reit,rlj-lodging-trust --batch-size 20 --since-year 2004
```

Checkpoints: `data/sec-edgar/checkpoints/marriott-hilton-historical-v2.json`

## Artifacts

- `MARRIOTT_HILTON_HISTORICAL_CONTRACT_INVENTORY.csv` / `.json`
- `data/sec-edgar/discovery-inventory/hotels.json`
- DiamondRock fixture: `fixtures/sec-edgar/diamondrock-franchise-expiration-schedule-v2.txt`

## Expansion plan

1. Exhaust DiamondRock / Apple / RLJ / Park property schedules via checkpointed batches  
2. EX-10 property-specific agreements  
3. S-11 / 424B portfolio disclosures  
4. Broader Marriott & Hilton corporate filings last (lower property yield)
