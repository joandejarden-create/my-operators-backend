# SEC EDGAR Contract Intelligence V1 + Historical Intelligence V2

**Contract:** `SEC_EDGAR_CONTRACT_INTELLIGENCE_V1`  
**Historical program:** `SEC_EDGAR_HISTORICAL_INTELLIGENCE_V2` (Marriott & Hilton)  
**Status:** Pilot + independent hotel discovery + historical research runner — internal persistence only  
**Customer publication:** BLOCKED without separate founder approval

## Purpose

Discover, verify, and persist hotel-level contractual intelligence from official SEC EDGAR filings.

**Critical rule:** SEC research is **not** restricted to hotels already in HPC. Missing HPC coverage does not invalidate discovery or verified contract findings.

## Three stages (independent)

1. **Discover** identifiable hotels from filings / exhibits / property schedules  
2. **Extract + verify** hotel-level contractual intelligence  
3. **Match** verified intelligence against HPC  

Failure at stage 3 does **not** discard stages 1–2.

### Discovery status vocabulary

| Status | Meaning |
|---|---|
| `DISCOVERED_MATCHED_HPC` | Named hotel matched to an existing HPC record |
| `DISCOVERED_NOT_IN_HPC` | Named hotel retained; no HPC match yet |
| `CONTRACT_VERIFIED_PENDING_HPC` | Verified contracts held until HPC identity exists |
| `MATCH_AMBIGUOUS` | Multiple HPC candidates — no auto-match / no apply |
| `PORTFOLIO_ONLY_EVIDENCE` | Portfolio-wide language — not property-attributed |
| `UNATTRIBUTED_CONTRACT_LANGUAGE` | Contract language without a named hotel |

## Persistence destinations

1. **Filesystem discovery inventory** — `data/sec-edgar/discovery-inventory/hotels.json` (unmatched hotels retained)  
2. **HI Graph Evidence** — field-level milestones (only when HPC matched + verified)  
3. **HI Hotel–Org Relationships** — Operator / Brand / Management Company edges  
4. **HI Organizations** — registrant CIK in `Registry / Identifier`  
5. **HI Ownership Research Runs** — run/cost ledger (reuse)  
6. **HPC** — allowlisted null-fill only — **no auto HPC create**, **no new HPC columns**

## Schema gaps (founder approval required to close)

| Gap | Mitigation in V1 |
|---|---|
| No HPC expiration/renewal/termination columns | Persist on HI Graph Evidence `Field Name` + `Value Observed` |
| No SEC Source Type enum on Partner Source Library | Use HI Graph Evidence `Source Type` = `SEC EDGAR Filing` |
| No dedicated CIK registry table | Pilot map + live `company_tickers` resolve |
| No Airtable table for unmatched discovered hotels | Filesystem inventory; do not auto-create HPC |

## Future HPC reconciliation

```bash
npm run sec-edgar-contract-intelligence:pilot -- --reconcile-only
```

Compares the stored discovery inventory against current HPC candidates **without** re-downloading SEC filings or repeating paid AI. Dry-run by default. When hotels become matchable, previously verified contracts can be planned onto existing HI Graph paths.

## Env

```dotenv
SEC_EDGAR_ENABLED=true
SEC_EDGAR_BASE_URL=https://data.sec.gov
SEC_EDGAR_USER_AGENT=Dealality Research joan@aohospitalityadvisors.com
SEC_EDGAR_RATE_LIMIT=2
SEC_EDGAR_TIMEOUT_MS=15000
# SEC_EDGAR_ALLOW_PAID_AI=0
# SEC_EDGAR_CACHE_DIR=data/sec-edgar/cache
```

## Commands

```bash
npm run test:sec-edgar-contract-intelligence-v1
npm run test:sec-edgar-marriott-hilton-historical-v2
npm run sec-edgar-contract-intelligence:pilot -- --offline-fixture
npm run sec-edgar-contract-intelligence:pilot
npm run sec-edgar-contract-intelligence:pilot -- --reconcile-only
# Historical Marriott/Hilton research (checkpointed batches):
npm run sec-edgar-marriott-hilton-historical:v2 -- --offline-fixture
npm run sec-edgar-marriott-hilton-historical:v2 -- --batch-size 12 --max-priority 2
npm run sec-edgar-marriott-hilton-historical:v2 -- --companies diamondrock-hospitality
# Airtable writes only with explicit apply + credentials:
npm run sec-edgar-contract-intelligence:pilot -- --apply
```

## Historical V2 (Marriott & Hilton)

- Brand families: `lib/hotel-intelligence/sec-edgar/brand-families-v2.js`
- Franchise/management expiration schedules: `franchise-schedule-extractor-v2.js` (DiamondRock test case)
- Historical applicability: `HISTORICALLY_VERIFIED` ≠ `CURRENTLY_VERIFIED` without later evidence
- Checkpoints: `data/sec-edgar/checkpoints/marriott-hilton-historical-v2.json`
- Inventory export: `reports/sec-edgar-marriott-hilton-historical-v2/`
- No arbitrary free-SEC document cap; use batch size + checkpoint resume
- Paid AI remains at $25 ceiling unless separately approved

## Search coverage & adaptive discovery V2

- Coverage matrix: `npm run sec-edgar-search-coverage:eval` → `reports/sec-edgar-search-coverage-v2/`
- Taxonomy: 14 versioned search families (`search-taxonomy-v2.js`); legacy 8 keywords retained for audit baseline
- Eligible filing inventory first: `--build-eligible` writes `data/sec-edgar/eligible-filings/`
- Forms ≠ exhibits: EX-10 nested under parent accessions (index.json follow-on gap documented)
- Adaptive queue: registrant↔hotel↔owner with depth/company caps; unverified entities blocked
- V1 pilot 2-year / 6–12 filing limits = smoke control only; production path is inventory + checkpoint batches

## Artifacts

| Path | Contents |
|---|---|
| `reports/sec-edgar-contract-intelligence-v1/HOTEL_INVENTORY.csv` | Full property-level inventory |
| `reports/sec-edgar-contract-intelligence-v1/HOTEL_INVENTORY.json` | Same as JSON |
| `reports/sec-edgar-contract-intelligence-v1/YIELD_PRIORITIZATION.json` | Per-company yield ranking |
| `reports/sec-edgar-contract-intelligence-v1/HPC_RECONCILIATION_DRY_RUN.json` | Future match dry-run |
| `data/sec-edgar/discovery-inventory/hotels.json` | Durable discovery inventory |

## Verification rules

- Portfolio-wide disclosures are **not** auto-attributed to every hotel.
- Estimated / speculative language → never `PERSISTED_VERIFIED`.
- Expiration is a **research trigger**, not proof of conversion intent.
- Every `--apply` write requires Airtable read-back before `PERSISTED_VERIFIED`.
- Named hotels without verified agreement dates remain useful discovery records.

## Yield guidance

Deprioritize broad brand-corporate filings that produce little property-level yield. Prefer REIT property schedules and EX-10 hotel agreements. See `YIELD_PRIORITIZATION.json` after each pilot.

## International readiness

Adapter contract in `lib/hotel-intelligence/sec-edgar/intl-readiness-v1.js` — no paid international sources in this phase.
