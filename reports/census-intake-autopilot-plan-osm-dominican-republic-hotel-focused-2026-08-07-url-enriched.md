# Census Intake Autopilot — Plan

**Status:** `census_intake_autopilot_plan_ready`
**Gates version:** census-intake-autopilot-gates-v1
**Batch:** osm-dominican-republic-hotel-focused-2026-08-07-url-enriched
**Generated:** 2026-08-07T09:06:54.791Z
**Mode:** plan (no Airtable writes)
**Dedupe SoT:** Hotel Property Census only
**Legacy Hotel Census:** forbidden

## Decision counts

| Decision | Count |
| --- | ---: |
| auto_insert (total) | 76 |
| — no Human Review | 21 |
| — with Human Review | 55 |
| production_writable_insert | 76 |
| auto_enrich_only (already in HPC) | 0 |
| steward_hold | 90 |
| reject | 7 |
| input rows | 173 |

## Top gate reasons

| Reason | Count |
| --- | ---: |
| `missing_official_property_url` | 65 |
| `known_chain_backlog_gates_passed_hr` | 33 |
| `steward_unresolved_osm_brand_tag` | 23 |
| `known_chain_backlog_insert_city_unknown_hr` | 22 |
| `independent_gates_passed` | 16 |
| `official_url_denylisted_ota_or_social` | 7 |
| `active_or_soft_brand_gates_passed` | 5 |
| `known_brand_missing_city` | 2 |

## Auto-insert sample

| Name | Brand | City | HR | Class |
| --- | --- | --- | --- | --- |
| Hotel Europa | Independent | Sosua | false | independent_l1_promote |
| Bungalows of Las Galeras | Independent | Las Galeras | false | independent_l1_promote |
| Jarabacua River Club & Resort | Independent | Jarabacoa | false | independent_l1_promote |
| Bella Vista | Independent | Santo domingo | false | independent_l1_promote |
| El Pelicano Apart-Hotel | Independent | Las Galeras | false | independent_l1_promote |
| Santo Domingo Bed & Breakfast | Independent | Santo Domingo | false | independent_l1_promote |
| Hotel Tropicana Deluxe | Independent | Punta Cana | false | independent_l1_promote |
| Hotel Villa La Plantacion | Independent | Las Galeras | false | independent_l1_promote |
| Hotel Casa Coco | Independent | Boca Chica | false | independent_l1_promote |
| Hotel & Apartments Buchen | Independent | Samaná | false | independent_l1_promote |
| Casa Barbara Las Terrenas | Independent | Las Terrenas | false | independent_l1_promote |
| Hotel BQ;Vent W&P Santo Domingo | Independent | Santo Domingo | false | independent_l1_promote |
| Hostal Ganesh | Independent | El Valle, Samana | false | independent_l1_promote |
| Hotal San Francisco de Asis | Independent | Santo Domingo | false | independent_l1_promote |
| Villas CODEVI | Independent | Dajabon | false | independent_l1_promote |
| Hotel Anselmo | Independent | Boca Chica | false | independent_l1_promote |
| Natura Park Beach Eco Resort & Spa | Blau Hotels | Punta Cana | true | known_chain_census_backlog_not_active_setup |
| Catalonia Punta Cana | Catalonia | Unknown | true | known_chain_census_backlog_not_active_setup |
| Hotel RIU Mambo | RIU | Maimón | true | known_chain_census_backlog_not_active_setup |
| Hotel RIU Merengue | RIU | Maimón | true | known_chain_census_backlog_not_active_setup |
| Hotel RIU Bachata | RIU | Maimón | true | known_chain_census_backlog_not_active_setup |
| Barceló Bávaro Palace Deluxe | Barceló | Unknown | true | known_chain_census_backlog_not_active_setup |
| Hodelpa Novus Plaza | Hodelpa | Santo Domingo | true | known_chain_census_backlog_not_active_setup |
| Secrets Royal Beach Punta Cana | Hyatt | Punta Cana | true | known_chain_census_backlog_not_active_setup |
| Hard Rock Hotel & Casino Punta Cana - All inclusive | Hard Rock Hotels | Punta Cana | true | known_chain_census_backlog_not_active_setup |
| Dreams Royal Beach Punta Cana | Dreams (Hyatt Inclusive Collection) | Punta Cana | true | known_chain_census_backlog_not_active_setup |
| Four Points by Sheraton | Four Points by Sheraton | Punta Cana | false | active_or_soft_brand_census_plus_autopilot |
| Occidental Caribe (former Barcelo Punta Cana) | Barceló | Unknown | true | known_chain_census_backlog_not_active_setup |
| Occidental Caribe (former Barcelo Punta Cana) | Barceló | Bavaro | true | known_chain_census_backlog_not_active_setup |
| Riu Palace Punta Cana | RIU | Punta Cana | true | known_chain_census_backlog_not_active_setup |
| Excellence Punta Cana | Excellence Resorts | Punta Cana | true | known_chain_census_backlog_not_active_setup |
| Radisson Hotel Santo Domingo | Radisson by Choice | Santo Domingo | false | active_or_soft_brand_census_plus_autopilot |
| Viva Dominicus Palace by Wyndham | Wyndham | Unknown | true | known_chain_census_backlog_not_active_setup |
| Embassy Suites by Hilton | Hilton Hotels & Resorts | Santo Domingo | false | active_or_soft_brand_census_plus_autopilot |
| Excellence El Carmen | Excellence Resorts | Punta Cana | true | known_chain_census_backlog_not_active_setup |
| Villa Ibiscus | ibis | Unknown | true | known_chain_census_backlog_not_active_setup |
| Hotel Riu República | RIU | Unknown | true | known_chain_census_backlog_not_active_setup |
| Lopesan Costa Bávaro Resort, Spa & Casino | Lopesan | Punta Cana | true | known_chain_census_backlog_not_active_setup |
| Hyatt Zilara Cap Cana | Hyatt Zilara | Unknown | true | known_chain_census_backlog_not_active_setup |
| Majestic Mirage Punta Cana - All-Suite Resort | Majestic Resorts | Punta Cana | true | known_chain_census_backlog_not_active_setup |

## Steward hold sample

| Name | Reasons |
| --- | --- |
| Breezes | missing_official_property_url |
| Dreams Palm Beach Resort | missing_official_property_url |
| Be Live Grand Bavaro | missing_official_property_url |
| Wyndham Alltra Punta Cana | missing_official_property_url |
| Hotel Crowne Plaza | missing_official_property_url |
| Club Med | missing_official_property_url |
| Be Live Grand Marien Hotel | missing_official_property_url |
| Emotion By Hodelpa. | missing_official_property_url |
| Casa Bonita | known_brand_missing_city |
| Sheraton | missing_official_property_url |
| Sensimar Punta Cana Villas & Suites | missing_official_property_url |
| Hotel Occidental Allegro Playa Dorada | missing_official_property_url |
| Barceló Puerto Plata | missing_official_property_url |
| Grand Bahia Principe Punta Cana | missing_official_property_url |
| Real Intercontinental | missing_official_property_url |
| Breathless Resort | missing_official_property_url |
| Chaykovsky Boutique h | steward_unresolved_osm_brand_tag |
| Kasablanka | steward_unresolved_osm_brand_tag |
| Las Galeras Village | steward_unresolved_osm_brand_tag |
| Melia Caribe Tropical | missing_official_property_url |
| fata morgana | steward_unresolved_osm_brand_tag |
| Las Galeras Island Hostel | steward_unresolved_osm_brand_tag |
| Grand Bahia Principe Bavaro | missing_official_property_url |
| Grand Bahia Principe Aquamarine | missing_official_property_url |
| White Sands B&B | steward_unresolved_osm_brand_tag |
| Hostal Casa 51 | steward_unresolved_osm_brand_tag |
| Los Corales Village RECEPTION | steward_unresolved_osm_brand_tag |
| Amhsa Marina Grand Paradise Playa Dorada | missing_official_property_url |
| Villa los Gorgones | steward_unresolved_osm_brand_tag |
| Casa Cayuco | steward_unresolved_osm_brand_tag |
| Casa Gio Las Terrenas | steward_unresolved_osm_brand_tag |
| Catalonia Bavaro Royal | missing_official_property_url |
| Hotel Emotions by Hodelpa | missing_official_property_url |
| Cabrera Chalet | steward_unresolved_osm_brand_tag |
| Share Melvi Apartment | missing_official_property_url |
| Arena Oceanview Hotel & La Terraza Restaurant | steward_unresolved_osm_brand_tag |
| Casa Amelia | missing_official_property_url |
| Surfbreak Cabarete | steward_unresolved_osm_brand_tag |
| Hotel Naragua | missing_official_property_url |
| Los Corales Beach Village Suites | steward_unresolved_osm_brand_tag |

## Reject sample

| Name | Reasons |
| --- | --- |
| Leysi's Garden | official_url_denylisted_ota_or_social |
| Hostal Shivas | official_url_denylisted_ota_or_social |
| Hostal Rural Backpackers | official_url_denylisted_ota_or_social |
| The Circle by Meliá | official_url_denylisted_ota_or_social |
| Domescape Glamping Miches | official_url_denylisted_ota_or_social |
| Villa Mamanita | official_url_denylisted_ota_or_social |
| GAVA hostel | official_url_denylisted_ota_or_social |

## Next

1. Review steward_hold / reject reason distribution (tighten gates via fixtures)
2. Controlled mode: propose patches only for `production_writable_insert`
3. Apply only with Autopilot confirms (HPC only; no Brand Explorer; no legacy)
