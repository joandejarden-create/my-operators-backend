# SEC EDGAR Contract Intelligence V1

**Contract:** `SEC_EDGAR_CONTRACT_INTELLIGENCE_V1`  
**Status:** Pilot implementation — internal persistence only  
**Customer publication:** BLOCKED without separate founder approval

## Purpose

Discover, verify, and persist hotel-level contractual intelligence from official SEC EDGAR filings into existing Dealality destinations:

1. **HI Graph Evidence** — field-level milestones (signing, effective, expiration, renewal, termination, amendment)
2. **HI Hotel–Org Relationships** — Operator / Brand / Management Company edges with Effective From/To when filing-backed
3. **HI Organizations** — registrant CIK in `Registry / Identifier`
4. **HI Ownership Research Runs** — run/cost ledger (reuse)
5. **HPC** — allowlisted null-fill only (`Management Model`, operator fields, affiliation dates) — **no new HPC columns**

## Schema gaps (founder approval required to close)

| Gap | Mitigation in V1 |
|---|---|
| No HPC expiration/renewal/termination columns | Persist on HI Graph Evidence `Field Name` + `Value Observed` |
| No SEC Source Type enum on Partner Source Library | Use HI Graph Evidence `Source Type` = `SEC EDGAR Filing` |
| No dedicated CIK registry table | Pilot map + live `company_tickers` resolve |

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
npm run sec-edgar-contract-intelligence:pilot -- --offline-fixture
npm run sec-edgar-contract-intelligence:pilot
# Airtable writes only with explicit apply + credentials:
npm run sec-edgar-contract-intelligence:pilot -- --apply
```

## Verification rules

- Portfolio-wide disclosures are **not** auto-attributed to every hotel.
- Estimated / speculative language → never `PERSISTED_VERIFIED`.
- Expiration is a **research trigger**, not proof of conversion intent.
- Every `--apply` write requires Airtable read-back before `PERSISTED_VERIFIED`.

## International readiness

Adapter contract in `lib/hotel-intelligence/sec-edgar/intl-readiness-v1.js` — no paid international sources in this phase.
