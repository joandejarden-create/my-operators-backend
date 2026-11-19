# HE Sources & Evidence consolidated navigation (V19 / HE-QL-031)

**Date:** 2026-11-19  
**Stream:** Hotel Explorer (`ws-hotel-explorer`)  
**PR:** #463  

## Problem

Founder Preview showed oversized white cross-tab buttons (`Open Contact Intelligence`, `Open Sources & Evidence`) and a duplicate **Sources and Verification** subsection on Ownership Intelligence.

## Fix

1. Compact header-right `hiw-navlink` links: **View Contacts** / **View Sources**.
2. Removed Ownership `renderOwnershipSourcesStrip` / Sources and Verification table.
3. Unified Sources & Evidence register with category filters and distinct **Published** vs **Verified / accessed** columns.
4. Deep-link attrs `data-hiw-source-filter` / `data-hiw-source-focus` with row highlight.
5. Market Alerts citations emit `publication_date` (not stuffed into `observed_date`).
6. Guest reputation listing URLs merge into Sources under Guest Sentiment when loaded.
7. Permanent standard + registry: HE-QL-031 (`hotel-explorer-quality-standard-v1.20`).

## Gates

- `npm run test:he-product-quality-gates-v1` — includes `sources_evidence_consolidated_nav_and_dates`
- `npm run test:he-market-alerts-closeout-v1`
- `npm run test:he-hotel-news-from-market-alerts-v1`
- `npm run test:he-ownership-control-network-v1`

## Browser checklist (Founder Preview `:8091`)

Hotel: Bethesda Marriott (`recLuxvwwxID7U2B8`) · mode Proposed; also KGPV + one additional hotel.

1. View Contacts compact top-right on Relevant Decision-Makers
2. View Sources compact top-right on network / Relationship Detail
3. No large white Open Contact / Open Sources buttons
4. No Ownership Sources and Verification table
5. All Bethesda sources in Sources & Evidence (deduped)
6. Cross-tab links preserve hotel + preview mode
7. Source deep links highlight rows
8. Published vs verified/accessed distinguished
9. Same components in Radar / Founder Preview shell

## Airtable / research

No production Airtable writes. No paid research. No deployment.
