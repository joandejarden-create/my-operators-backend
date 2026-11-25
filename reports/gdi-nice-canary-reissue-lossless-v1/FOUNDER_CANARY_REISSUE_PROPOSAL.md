# Corrected NICE Canary Proposal (Lossless SoT)

**Status:** PROPOSED — NOT EXECUTED  
**Record:** `rec8QDHQEpGbi8kvW` / `gdi_opp_nice_2027`  
**Merge:** `gdi-research-sot-lossless-merge-v1`  
**Ownership:** `gdi-research-sot-field-ownership-v1`

## Guards

- Single existing Airtable record
- Single allowed opportunity ID
- Maximum one write
- Full live snapshot + rollback plan (from prior prewrite; refresh optional)
- Read-back + idempotency required on execute
- No GDO / STAGED / CURRENT changes

## Payload

| | Chars |
|---|---|
| Live SoT | 42314 |
| Prior rejected slim | 7235 |
| Proposed lossless merge | 44833 |

Classification: `VERIFIED_ENRICHMENT`

**STOP — founder authorization required before any mutation.**
