# PR2 — Pre-Deep Screening and Deduplication

## Delivered

- Screen **every** discovery candidate before fair queue + `maxCandidates`
- Traceable outcomes: `ELIGIBLE_FOR_DEEP_RESEARCH`, `GLOBAL_INTELLIGENCE_ONLY`, `DUPLICATE_NO_NEW_EVIDENCE`, `EXISTING_OPPORTUNITY_ENRICHMENT`, etc.
- Published CUSTOMER_PUBLISHED package inventory loader (Bethesda 67 fingerprint)
- Known org + new occurrence remains enrichment-eligible
- Screening ledger persisted as `pre-deep-candidate-screening.json`
- Wired into `original-full-e2e-research-chain-v1.js` before deep cap

## Modules

- `lib/.../research/gdi-published-inventory-dedupe-v1.js`
- `lib/.../research/gdi-pre-deep-candidate-screening-v1.js`
- `scripts/test-gdi-pre-deep-screening-dedupe-v1.mjs`

## Acceptance

| Criterion | Status |
|---|---|
| All candidates screened before deep cap | Yes |
| Published duplicates skip avoidable deep | Yes (Rock Creek Mansion) |
| New occurrence of known org eligible | Yes (AHIMA training fixture) |
| Out-of-market → GLOBAL_INTELLIGENCE_ONLY | Yes |
| Decision reasons recorded | Yes |
| CURRENT / fingerprint unchanged | Yes |
| No paid research | Yes |
