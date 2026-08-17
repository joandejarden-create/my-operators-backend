# Census Intake Autopilot — Plan

**Status:** `census_intake_autopilot_plan_ready`
**Gates version:** census-intake-autopilot-gates-v1
**Batch:** osm-dominican-republic-hotel-focused-2026-08-07-url-enriched-v3
**Generated:** 2026-08-07T11:18:01.498Z
**Mode:** plan (no Airtable writes)
**Dedupe SoT:** Hotel Property Census only
**Legacy Hotel Census:** forbidden

## Decision counts

| Decision | Count |
| --- | ---: |
| auto_insert (total) | 123 |
| — no Human Review | 28 |
| — with Human Review | 95 |
| production_writable_insert | 123 |
| auto_enrich_only (already in HPC) | 0 |
| steward_hold | 39 |
| reject | 11 |
| input rows | 173 |

## Top gate reasons

| Reason | Count |
| --- | ---: |
| `known_chain_backlog_gates_passed_hr` | 57 |
| `known_chain_backlog_insert_city_unknown_hr` | 38 |
| `steward_unresolved_osm_brand_tag` | 21 |
| `missing_official_property_url` | 18 |
| `independent_gates_passed` | 14 |
| `active_or_soft_brand_gates_passed` | 14 |
| `hostel_or_hostal_out_of_scope` | 7 |
| `official_url_denylisted_ota_or_social` | 4 |

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
| Villas CODEVI | Independent | Dajabon | false | independent_l1_promote |
| Hotel Anselmo | Independent | Boca Chica | false | independent_l1_promote |
| Breezes | Breezes (SuperClubs) | Puerto Plata | true | known_chain_census_backlog_not_active_setup |
| Dreams Palm Beach Resort | Dreams (Hyatt Inclusive Collection) | Punta Cana | true | known_chain_census_backlog_not_active_setup |
| Natura Park Beach Eco Resort & Spa | Blau Hotels | Punta Cana | true | known_chain_census_backlog_not_active_setup |
| Catalonia Punta Cana | Catalonia | Unknown | true | known_chain_census_backlog_not_active_setup |
| Be Live Grand Bavaro | Be Live | Bávaro | true | known_chain_census_backlog_not_active_setup |
| Wyndham Alltra Punta Cana | Wyndham | Unknown | true | known_chain_census_backlog_not_active_setup |
| Hotel RIU Mambo | RIU | Maimón | true | known_chain_census_backlog_not_active_setup |
| Hotel RIU Merengue | RIU | Maimón | true | known_chain_census_backlog_not_active_setup |
| Hotel RIU Bachata | RIU | Maimón | true | known_chain_census_backlog_not_active_setup |
| Barceló Bávaro Palace Deluxe | Barceló | Unknown | true | known_chain_census_backlog_not_active_setup |
| Hotel Crowne Plaza | Crowne Plaza | Santo Domingo | false | active_or_soft_brand_census_plus_autopilot |
| Hodelpa Novus Plaza | Hodelpa | Santo Domingo | true | known_chain_census_backlog_not_active_setup |
| Be Live Grand Marien Hotel | Be Live | Puerto Plata | true | known_chain_census_backlog_not_active_setup |
| Emotion By Hodelpa. | Hodelpa | Unknown | true | known_chain_census_backlog_not_active_setup |
| Secrets Royal Beach Punta Cana | Hyatt | Punta Cana | true | known_chain_census_backlog_not_active_setup |
| Hard Rock Hotel & Casino Punta Cana - All inclusive | Hard Rock Hotels | Punta Cana | true | known_chain_census_backlog_not_active_setup |
| Casa Bonita | SLH | Barahona | false | active_or_soft_brand_census_plus_autopilot |
| Dreams Royal Beach Punta Cana | Dreams (Hyatt Inclusive Collection) | Punta Cana | true | known_chain_census_backlog_not_active_setup |
| Four Points by Sheraton | Four Points by Sheraton | Punta Cana | false | active_or_soft_brand_census_plus_autopilot |
| Sheraton | Sheraton | Santo Domingo | false | active_or_soft_brand_census_plus_autopilot |
| Hotel Occidental Allegro Playa Dorada | Occidental | Puerto Plata | true | known_chain_census_backlog_not_active_setup |
| Barceló Puerto Plata | Barceló | Puerto Plata | true | known_chain_census_backlog_not_active_setup |
| Occidental Caribe (former Barcelo Punta Cana) | Barceló | Unknown | true | known_chain_census_backlog_not_active_setup |
| Occidental Caribe (former Barcelo Punta Cana) | Barceló | Bavaro | true | known_chain_census_backlog_not_active_setup |
| Grand Bahia Principe Punta Cana | Bahía Príncipe | Unknown | true | known_chain_census_backlog_not_active_setup |
| Riu Palace Punta Cana | RIU | Punta Cana | true | known_chain_census_backlog_not_active_setup |

## Steward hold sample

| Name | Reasons |
| --- | --- |
| Club Med | missing_official_property_url |
| Sensimar Punta Cana Villas & Suites | missing_official_property_url |
| Chaykovsky Boutique h | steward_unresolved_osm_brand_tag |
| Kasablanka | steward_unresolved_osm_brand_tag |
| Las Galeras Village | steward_unresolved_osm_brand_tag |
| fata morgana | steward_unresolved_osm_brand_tag |
| White Sands B&B | steward_unresolved_osm_brand_tag |
| Los Corales Village RECEPTION | steward_unresolved_osm_brand_tag |
| Villa los Gorgones | steward_unresolved_osm_brand_tag |
| Casa Cayuco | steward_unresolved_osm_brand_tag |
| Casa Gio Las Terrenas | steward_unresolved_osm_brand_tag |
| Cabrera Chalet | steward_unresolved_osm_brand_tag |
| Share Melvi Apartment | missing_official_property_url |
| Arena Oceanview Hotel & La Terraza Restaurant | steward_unresolved_osm_brand_tag |
| Casa Amelia | missing_official_property_url |
| Surfbreak Cabarete | steward_unresolved_osm_brand_tag |
| Hotel Naragua | missing_official_property_url |
| Los Corales Beach Village Suites | steward_unresolved_osm_brand_tag |
| Las Terrazas Condo Apartment | steward_unresolved_osm_brand_tag |
| Flor del Mar Condo Apartment | steward_unresolved_osm_brand_tag |
| La Piazzetta PREMIUM Suites | steward_unresolved_osm_brand_tag |
| Hotel Dominican Fiesta | steward_unresolved_osm_brand_tag |
| Cabañas Monte Lola | steward_unresolved_osm_brand_tag |
| Dreams Resort Hotel | missing_official_property_url |
| Hotel Acuarium | missing_official_property_url |
| Iberostar | missing_official_property_url |
| Coop Marena Beach Resort | steward_unresolved_osm_brand_tag |
| Be Live | missing_official_property_url |
| Be Live Collection | missing_official_property_url |
| Be Live Collection | missing_official_property_url |
| Be Live Collection | missing_official_property_url |
| Be Live Collection | missing_official_property_url |
| Be Live | missing_official_property_url |
| Be Live | missing_official_property_url |
| Be Live | missing_official_property_url |
| Be Live | missing_official_property_url |
| Be Live | missing_official_property_url |
| Villa Scooby | steward_unresolved_osm_brand_tag |
| Hotel Diosi | steward_unresolved_osm_brand_tag |

## Reject sample

| Name | Reasons |
| --- | --- |
| Leysi's Garden | official_url_denylisted_ota_or_social |
| Hostal Shivas | hostel_or_hostal_out_of_scope |
| Hostal Ganesh | hostel_or_hostal_out_of_scope |
| Hotal San Francisco de Asis | hostel_or_hostal_out_of_scope |
| Las Galeras Island Hostel | hostel_or_hostal_out_of_scope |
| Hostal Casa 51 | hostel_or_hostal_out_of_scope |
| Hostal Rural Backpackers | hostel_or_hostal_out_of_scope |
| The Circle by Meliá | official_url_denylisted_ota_or_social |
| Domescape Glamping Miches | official_url_denylisted_ota_or_social |
| Villa Mamanita | official_url_denylisted_ota_or_social |
| GAVA hostel | hostel_or_hostal_out_of_scope |

## Next

1. Review steward_hold / reject reason distribution (tighten gates via fixtures)
2. Controlled mode: propose patches only for `production_writable_insert`
3. Apply only with Autopilot confirms (HPC only; no Brand Explorer; no legacy)
