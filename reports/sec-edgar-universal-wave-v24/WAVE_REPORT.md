# SEC EDGAR Universal Research Wave V2.4

**Generated:** 2026-11-20  
**PR:** #492  
**Wave ID:** `sec-edgar-universal-wave-v24`  
**Paid research spend:** $0  
**Customer publication:** BLOCKED  

## Companies

| Metric | Value |
|---|---|
| Static registry size | **32** (target 25–40) |
| Live-validated (ticker resolve) | **29** |
| Failed ticker resolve (delisted / missing) | 3 — Hersha HT, Condor CDOR, Sotherly SOHO |
| CIK corrections applied | SVC, CLDT, INN |
| Companies with eligible inventory | **29** |
| Eligible filings (1994+) | **11,940** cumulative inventoried |

Registry is **not** Marriott/Hilton-restricted — includes Hyatt, Choice, Wyndham, Ryman, MGM, LVS, Wynn, Caesars, historical FelCor/LaSalle/Chesapeake/Strategic, etc.

## Research execution (this wave)

| Metric | Value |
|---|---|
| Documents processed (checkpoint) | **30** |
| Exhibits enumerated / processed | see exhibit ledger + prior V2.2/V2.3 |
| Remaining eligible (approx) | ~11,900 |
| Unique verified hotels (Sunstone harvest) | **35** |
| Verified precision | **100%** (35/35) — gate ≥95% PASS |
| Probable (wave-tagged) | included in filesystem inventory upsert |
| Airtable persisted (new) | **35** |
| Airtable read-backs | **35/35 OK** |
| HPC matches / fields enriched | **0 / 0** |
| New owners (explicit LEGAL_OWNER) | low — brand/gaming 10-Ks mostly registrant-only |
| Managers | schedule Manager columns where present (prior DRH + this wave) |

### Yield lesson

- Recent inline-XBRL brand/gaming/REIT 10-Ks often return **0** property-table hotels with the current HTML extractor.
- Historical REIT HTML 10-Ks (e.g. Sunstone 2006–2014) remain high-yield for property schedules.
- Batch scoring now prefers REIT + 1996–2018 10-Ks over newest XBRL packages.

## Cumulative coverage ledger

See `COVERAGE_LEDGER.md` / `ELIGIBLE_INVENTORY_SUMMARY.json`.

Do **not** merge EXTENDED/historical cohorts into the original CORE_SIX 3,863 denominator without an explicit cohort label.

## Next highest-yield targets

1. Additional historical Sunstone / Host / Ashford / FelCor / LaSalle 10-K schedule years  
2. DiamondRock pre-XBRL franchise expiration tables (dated milestones)  
3. Exhibit-index EX-10 hotel HMA/franchise content filter (skip employment/credit)  
4. EX-21 SPE → property ownership linking  
5. Rebuild eligible inventory for live-corrected CIKs (SVC/CLDT/INN) where paths drifted  
6. Modern XBRL property-table parser (Room / Location / Brand axes)

## Guarantees

- Identity quality gate unchanged  
- Registrant ≠ owner/manager without explicit language  
- Historical dates ≠ currently applicable without later evidence  
- No HPC auto-create; no customer publish  
- Ambiguous/artifacts retained in filesystem archive — not promoted  

## Artifacts

- `REGISTRY_STATIC.json` / `REGISTRY_VALIDATED.json`  
- `ELIGIBLE_INVENTORY_SUMMARY.json`  
- `SUNSTONE_HARVEST.json`  
- `AIRTABLE_PERSIST.json`  
- `data/sec-edgar/checkpoints/universal-research-v24.json`  
