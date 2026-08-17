# Known-chain Official URL enrichment (dry-run)

**Mode:** report-only (no Airtable writes)
**Version:** known-chain-official-url-enrichment-v1
**Generated:** 2026-08-07T11:17:14.939Z
**Plan batch:** osm-dominican-republic-hotel-focused-2026-08-07-url-enriched-v2
**Google Places input:** reports/census-intake-google-places-url-dry-run-osm-dominican-republic-hotel-focused-2026-08-07.json

## Summary

| Metric | Count |
| --- | ---: |
| Input (missing URL) | 65 |
| Proposed Official URL | 25 |
| High confidence | 25 |
| Brand search leads | 34 |
| Brand hosts unmapped | 22 |
| Simulated auto_insert lift | 26 |

## Policy

- Brand homepage alone ≠ Official Property URL
- Search lead URLs are discovery aids only
- Google websiteUri only promoted when usable / non-denylist
- Brand-domain corroboration preferred for known chains

## Proposed sample

| Name | Brand | Source | Confidence | Re-gate |
| --- | --- | --- | --- | --- |
| Breezes | Breezes (SuperClubs) | catalog_official_url | high | auto_insert |
| Dreams Palm Beach Resort | Dreams (Hyatt Inclusive Collection) | catalog_official_url | high | auto_insert |
| Be Live Grand Bavaro | Be Live | catalog_official_url | high | auto_insert |
| Be Live Grand Marien Hotel | Be Live | catalog_official_url | high | auto_insert |
| Sheraton | Sheraton | catalog_official_url | high | auto_insert |
| Hotel Occidental Allegro Playa Dorada | Occidental | catalog_official_url | high | auto_insert |
| Barceló Puerto Plata | Barceló | catalog_official_url | high | auto_insert |
| Real Intercontinental | InterContinental | catalog_official_url | high | auto_insert |
| Breathless Resort | Breathless (Hyatt Inclusive Collection) | catalog_official_url | high | auto_insert |
| Melia Caribe Tropical | Meliá | catalog_official_url | high | auto_insert |
| Grand Bahia Principe Bavaro | Bahía Príncipe | catalog_official_url | high | auto_insert |
| Catalonia Bavaro Royal | Catalonia | catalog_official_url | high | auto_insert |
| Hotel Occidental Allegro Playa Dorada | Occidental | catalog_official_url | high | auto_insert |
| Barcelo Punta Cana Hotel | Barceló | catalog_official_url | high | auto_insert |
| Gran Bahia Principe Cayacoa | Bahía Príncipe | catalog_official_url | high | auto_insert |
| Casa Marina Beach & Reef | Amhsa Marina Hotels | catalog_official_url | high | auto_insert |
| Quality Inn | Quality Inn | catalog_official_url | high | auto_insert |
| Hilton La Romana | Hilton Hotels & Resorts | catalog_official_url | high | auto_insert |
| Hard Rock Hotel | Hard Rock Hotels | catalog_official_url | high | auto_insert |
| Melia Santo Domingo Hotel | Meliá | catalog_official_url | high | auto_insert |

## Simulated auto_insert lift sample

| Name | Prior | New | URL host |
| --- | --- | --- | --- |
| Breezes | steward_hold | auto_insert | www.breezes.com |
| Dreams Palm Beach Resort | steward_hold | auto_insert | www.hyattinclusivecollection.com |
| Be Live Grand Bavaro | steward_hold | auto_insert | www.belivehotels.com |
| Be Live Grand Marien Hotel | steward_hold | auto_insert | www.belivehotels.com |
| Casa Bonita | steward_hold | auto_insert |  |
| Sheraton | steward_hold | auto_insert | www.marriott.com |
| Hotel Occidental Allegro Playa Dorada | steward_hold | auto_insert | www.barcelo.com |
| Barceló Puerto Plata | steward_hold | auto_insert | www.barcelo.com |
| Real Intercontinental | steward_hold | auto_insert | www.ihg.com |
| Breathless Resort | steward_hold | auto_insert | www.hyattinclusivecollection.com |
| Melia Caribe Tropical | steward_hold | auto_insert | www.melia.com |
| Grand Bahia Principe Bavaro | steward_hold | auto_insert | www.bahia-principe.com |
| Catalonia Bavaro Royal | steward_hold | auto_insert | www.cataloniahotels.com |
| Hotel Occidental Allegro Playa Dorada | steward_hold | auto_insert | www.barcelo.com |
| Barcelo Punta Cana Hotel | steward_hold | auto_insert | www.barcelo.com |
