# Google Places hotel URL lookup (dry-run)

**Mode:** report-only (no Airtable writes)
**Version:** google-places-hotel-url-lookup-v1
**Generated:** 2026-08-07T09:01:15.739Z
**Batch / plan:** osm-dominican-republic-hotel-focused-2026-08-07

## Summary

| Metric | Count |
| --- | ---: |
| Input (missing URL) | 80 |
| Processed | 80 |
| API requests | 81 |
| Matched | 80 |
| No match | 0 |
| Proposed Official URL | 29 |
| High-confidence proposals | 23 |
| Skipped (budget) | 0 |

## Policy

- Source type: `google_places` (restricted_refresh_required)
- Store Place ID + websiteUri in reports only; no photos/reviews
- `googleMapsUri` never used as Official Property URL
- Apply to Census only after steward / known-chain corroboration merge

## Proposed sample

| Name | Brand | Confidence | Website host |
| --- | --- | --- | --- |
| Catalonia Punta Cana | Catalonia | High | cataloniahotels.com |
| Wyndham Alltra Punta Cana | Wyndham | High | puntacanawyndhamalltra.com |
| Hodelpa Novus Plaza | Hodelpa | High | hodelpa.com |
| Four Points by Sheraton | Four Points by Sheraton | High | marriott.com |
| Occidental Caribe (former Barcelo Punta Cana) | Barceló | High | barcelo.com |
| Occidental Caribe (former Barcelo Punta Cana) | Barceló | High | barcelo.com |
| Riu Palace Punta Cana | RIU | High | riu.com |
| Embassy Suites by Hilton | Hilton Hotels & Resorts | High | hilton.com |
| Amhsa Marina Grand Paradise Playa Dorada | Amhsa Marina Hotels | High | grandparadiseplayadorada.com |
| Hyatt Zilara Cap Cana | Hyatt Zilara | High | hyatt.com |
| Marriott Miches Beach, An All-Inclusive Resort | Marriott | High | marriott.com |
| Melia caribe beach resort | Meliá | High | melia.com |
| Holiday Inn Santo Domingo | Holiday Inn | High | ihg.com |
| Hodelpa Centro Plaza Hotel | Hodelpa | High | hodelpa.com |
| Crowne Plaza | Crowne Plaza | Medium | ihg.com |
