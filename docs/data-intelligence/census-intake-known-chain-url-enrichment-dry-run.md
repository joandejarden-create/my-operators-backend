# Known-chain Official URL enrichment (dry-run)

**Mode:** report-only (no Airtable writes)
**Version:** known-chain-official-url-enrichment-v1
**Generated:** 2026-08-07T10:49:33.895Z
**Plan batch:** osm-dominican-republic-hotel-focused-2026-08-07-url-enriched
**Google Places input:** reports/census-intake-google-places-url-dry-run-osm-dominican-republic-hotel-focused-2026-08-07.json

## Summary

| Metric | Count |
| --- | ---: |
| Input (missing URL) | 65 |
| Proposed Official URL | 25 |
| High confidence | 24 |
| Brand search leads | 50 |
| Brand hosts unmapped | 0 |
| Simulated auto_insert lift | 24 |

## Policy

- Brand homepage alone ≠ Official Property URL
- Search lead URLs are discovery aids only
- Google websiteUri only promoted when usable / non-denylist
- Brand-domain corroboration preferred for known chains

## Proposed sample

| Name | Brand | Source | Confidence | Re-gate |
| --- | --- | --- | --- | --- |
| Wyndham Alltra Punta Cana | Wyndham | google_places_website_on_brand_domain | high | auto_insert |
| Hotel Crowne Plaza | Crowne Plaza | google_places_website_on_brand_domain | high | auto_insert |
| Emotion By Hodelpa. | Hodelpa | google_places_website_on_brand_domain | high | auto_insert |
| Grand Bahia Principe Punta Cana | Bahía Príncipe | google_places_website_on_brand_domain | high | auto_insert |
| Grand Bahia Principe Aquamarine | Bahía Príncipe | google_places_website_on_brand_domain | high | auto_insert |
| Amhsa Marina Grand Paradise Playa Dorada | Amhsa Marina Hotels | google_places_website_on_brand_domain | high | auto_insert |
| Hotel Emotions by Hodelpa | Hodelpa | google_places_website_on_brand_domain | high | auto_insert |
| Crowne Plaza | Crowne Plaza | google_places_website_on_brand_domain | high | auto_insert |
| Renaissance Hotel | Renaissance Hotels | google_places_website_on_brand_domain | high | auto_insert |
| Hodelpa Gran Almirante | Hodelpa | google_places_website_on_brand_domain | high | auto_insert |
| Luxury Bahia Principe Samana | Bahía Príncipe | google_places_website_on_brand_domain | high | auto_insert |
| Hilton La Romana | Hilton Hotels & Resorts | google_places_website_on_brand_domain | high | steward_hold |
| Hard Rock Hotel | Hard Rock Hotels | google_places_website_on_brand_domain | medium | auto_insert |
| Excellence Punta Cana Hotel | Excellence Resorts | google_places_website_on_brand_domain | high | auto_insert |
| Occidental Grand Punta Cana Hotel | Occidental | google_places_website_on_brand_domain | high | auto_insert |
| Hotel Breathless Punta Cana | Breathless (Hyatt Inclusive Collection) | google_places_website_on_brand_domain | high | auto_insert |
| Holiday Inn | Holiday Inn | google_places_website_on_brand_domain | high | auto_insert |
| Grand Bahia Principe Turquesa | Bahía Príncipe | google_places_website_on_brand_domain | high | auto_insert |
| Dreams Dominicus La Romana | Dreams (Hyatt Inclusive Collection) | google_places_website_on_brand_domain | high | auto_insert |
| Hotel Casa Hemingway | Starfish Resorts | google_places_website_on_brand_domain | high | auto_insert |

## Simulated auto_insert lift sample

| Name | Prior | New | URL host |
| --- | --- | --- | --- |
| Wyndham Alltra Punta Cana | steward_hold | auto_insert | www.puntacanawyndhamalltra.com |
| Hotel Crowne Plaza | steward_hold | auto_insert | www.ihg.com |
| Emotion By Hodelpa. | steward_hold | auto_insert | www.hodelpa.com |
| Grand Bahia Principe Punta Cana | steward_hold | auto_insert | www.bahia-principe.com |
| Grand Bahia Principe Aquamarine | steward_hold | auto_insert | www.bahia-principe.com |
| Amhsa Marina Grand Paradise Playa Dorada | steward_hold | auto_insert | www.grandparadiseplayadorada.com |
| Hotel Emotions by Hodelpa | steward_hold | auto_insert | www.hodelpa.com |
| Crowne Plaza | steward_hold | auto_insert | www.ihg.com |
| Renaissance Hotel | steward_hold | auto_insert | www.marriott.com |
| Hodelpa Gran Almirante | steward_hold | auto_insert | www.hodelpa.com |
| Luxury Bahia Principe Samana | steward_hold | auto_insert | www.bahia-principe.com |
| Hard Rock Hotel | steward_hold | auto_insert | hotel.hardrock.com |
| Excellence Punta Cana Hotel | steward_hold | auto_insert | www.excellenceresorts.com |
| Occidental Grand Punta Cana Hotel | steward_hold | auto_insert | www.barcelo.com |
| Hotel Breathless Punta Cana | steward_hold | auto_insert | www.hyattinclusivecollection.com |
