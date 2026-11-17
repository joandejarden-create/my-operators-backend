# Bethesda Opportunity Comparison — Live Exa vs 67 Published

## Baseline

- Package: `gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1`
- Fingerprint: `441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f`
- Count: **67** CUSTOMER_PUBLISHED (inventory unchanged by this research)

Baseline ID index exported to `baseline-67-inventory.json`.

## Evaluation labels (processed deep set = 20)

| Label | Count | Notes |
|---|---|---|
| NEW_UNIQUE | 3 | Not in 67 by org-key match; watchlist only |
| EXISTING_ENRICHED | 0 | No material enrichment of published rows |
| DUPLICATE | 1 | Rock Creek Mansion ↔ `gdi_plp_live_fa2063dc1195` |
| REJECTED | 16 | EXCLUDE after live deep / qualification |
| FURTHER_RESEARCH | 0 | — |

## NEW_UNIQUE detail

### Amentum Holdings, Inc. / Amentum
- Segment: `corporate_accounts_scaling`
- Opportunity ID (research): `disc_ff8f41b34c5b`
- Disposition: `WATCHLIST_RESEARCH`
- Provider run: `agent_run_9756fbe19b934785baed16957783fa6b`
- Contacts investigated: 2 · sales-ready: 1
- Baseline matches: none

### American Political Science Association
- Segment: `government_public_sector`
- Opportunity ID (research): `disc_ab0b19495c15`
- Disposition: `WATCHLIST_RESEARCH`
- Provider run: `agent_run_94904d70c1ec4548a86ba405fc9dd02d`
- Contacts investigated: 0 · sales-ready: 0
- Baseline matches: none

### SAME Foundation
- Segment: `social_special_events`
- Opportunity ID (research): `disc_2cac04452eec`
- Disposition: `WATCHLIST_RESEARCH`
- Provider run: `agent_run_6ffe131dd04640038b45e117b832b933`
- Contacts investigated: 0 · sales-ready: 0
- Baseline matches: none


## DUPLICATE detail

- **Rock Creek Mansion** (`disc_31a0771a161c`) ↔ baseline gdi_plp_live_fa2063dc1195

## Rejected sample (national / unfit)

- NASPA · `associations_conferences_events` · EXCLUDE
- American Economic Association and allied associations · `corporate_meetings_incentives` · EXCLUDE
- Big Time Hoops · `sports_entertainment` · EXCLUDE
- Society for Maternal-Fetal Medicine · `education_scientific` · EXCLUDE
- AlphaStruxure · `projects_infrastructure` · EXCLUDE
- AFCEA DC · `local_catering_day_meetings` · EXCLUDE
- Montgomery County Business Hall of Fame · `corporate_celebrations_private_events` · EXCLUDE
- American Association for Pediatric Ophthalmology and Strabismus · `travel_trade_group_intermediaries` · EXCLUDE
- Heven AeroTech · `relocation_extended_stay` · EXCLUDE
- TCON Meetings & Events · `professional_event_planners` · EXCLUDE
- Society for Industrial and Organizational Psychology · `associations_conferences_events` · EXCLUDE
- Enabled Intelligence, Inc. · `corporate_accounts_scaling` · EXCLUDE

## Deduplication findings

1. Org-key match caught Rock Creek Mansion against preferred-lodging published inventory.
2. Many EXCLUDE rows are **not** baseline duplicates — they are newly seen but commercially rejected (wrong geography, speculative, or weak demand thesis).
3. No research IDs were written into the customer-published package.
4. Novelty is judged against the **67 published** fingerprint only, per founder directive.
