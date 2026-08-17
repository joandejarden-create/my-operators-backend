# Dual-Lane Census Intake Plan — Dominican Republic

**Status:** `dual_lane_census_intake_plan_dry_run_ready`
**Batch:** osm-dominican-republic-hotel-focused-2026-08-07
**Generated:** 2026-08-07T08:18:44.592Z
**Write target (future apply only):** Deal Capture Platform → Hotel Property Census
**Dedupe SoT:** Hotel Property Census only
**Legacy Hotel Census:** forbidden / not used
**Airtable writes this run:** no

## Principle

Brand-exclusion routes hotels **away from the independent lane**, not out of Census.
Active/Live Brand Setup controls Autopilot **enrichment scope**, not whether a known-chain property may enter Hotel Property Census.
Duplicate prevention uses **Hotel Property Census only** — never legacy Hotel Census.

## Lane counts

| Lane / class | Count |
| --- | ---: |
| Independent L1 promote payloads | 18 |
| Known brand — Active/soft Autopilot-ready | 14 |
| Known brand — **census backlog (not Active Setup)** | 114 |
| Steward brand-tag review | 27 |
| Independent L1 validation pass | 18 |
| Known brand validation pass | 155 |
| Skipped HPC duplicate (`likely_existing`) | 4 |
| Held HPC possible duplicate (steward) | 1 |

## Field mapping (both lanes)

| Census field | Independent lane | Known-brand lane |
| --- | --- | --- |
| Affiliation Status | Independent | Branded / Soft-Branded / Brand-Unconfirmed |
| Current Brand | Independent | Matched chain / OSM brand |
| Independent Hotel Flag | true | false |
| Family / Source Family | independent_open_sources | chain / family label |
| VIC Freeze Hash | `independent_census_dr_osm_2026-08-07` | same batch freeze |
| Production Use Status | Census Only / Not Owner-Facing | same |
| Property Identity Key | `osm_do_<osm_id>` | `osm_do_<osm_id>` |

## Independent L1 sample

| Name | City | HPC action | Validation |
| --- | --- | --- | --- |
| Hotel Europa | Sosua | needs_research | pass |
| Bungalows of Las Galeras | Las Galeras | likely_new_candidate | pass |
| Jarabacua River Club & Resort | Jarabacoa | likely_new_candidate | pass |
| Bella Vista | Santo domingo | likely_new_candidate | pass |
| El Pelicano Apart-Hotel | Las Galeras | likely_new_candidate | pass |
| Santo Domingo Bed & Breakfast | Santo Domingo | likely_new_candidate | pass |
| Hotel Tropicana Deluxe | Punta Cana | likely_new_candidate | pass |
| Hotel Villa La Plantacion | Las Galeras | likely_new_candidate | pass |
| Hotel Casa Coco | Boca Chica | likely_new_candidate | pass |
| Hotel & Apartments Buchen | Samaná | likely_new_candidate | pass |
| Leysi's Garden | Samana | likely_new_candidate | pass |
| Hostal Shivas | El Valle, Samana | likely_new_candidate | pass |
| Casa Barbara Las Terrenas | Las Terrenas | likely_new_candidate | pass |
| Hotel BQ;Vent W&P Santo Domingo | Santo Domingo | likely_new_candidate | pass |
| Hostal Ganesh | El Valle, Samana | likely_new_candidate | pass |
| Hotal San Francisco de Asis | Santo Domingo | likely_new_candidate | pass |
| Villas CODEVI | Dajabon | likely_new_candidate | pass |
| Hotel Anselmo | Boca Chica | likely_new_candidate | pass |

## Known-chain backlog sample (keep for Census)

| Name | Current Brand | Class | HPC action |
| --- | --- | --- | --- |
| Breezes | Breezes (SuperClubs) | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Dreams Palm Beach Resort | Dreams (Hyatt Inclusive Collection) | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Natura Park Beach Eco Resort & Spa | Blau Hotels | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Catalonia Punta Cana | Catalonia | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Be Live Grand Bavaro | Be Live | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Wyndham Alltra Punta Cana | Wyndham | known_chain_census_backlog_not_active_setup | needs_research |
| Hotel RIU Mambo | RIU | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Hotel RIU Merengue | RIU | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Hotel RIU Bachata | RIU | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Barceló Bávaro Palace Deluxe | Barceló | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Hodelpa Novus Plaza | Hodelpa | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Club Med | Club Med | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Be Live Grand Marien Hotel | Be Live | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Emotion By Hodelpa. | Hodelpa | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Secrets Royal Beach Punta Cana | Hyatt | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Hard Rock Hotel & Casino Punta Cana - All inclusive | Hard Rock Hotels | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Dreams Royal Beach Punta Cana | Dreams (Hyatt Inclusive Collection) | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Sensimar Punta Cana Villas & Suites | RIU | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Hotel Occidental Allegro Playa Dorada | Occidental | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Barceló Puerto Plata | Barceló | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Occidental Caribe (former Barcelo Punta Cana) | Barceló | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Occidental Caribe (former Barcelo Punta Cana) | Barceló | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Grand Bahia Principe Punta Cana | Bahía Príncipe | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Riu Palace Punta Cana | RIU | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Excellence Punta Cana | Excellence Resorts | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Real Intercontinental | InterContinental | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Breathless Resort | Breathless (Hyatt Inclusive Collection) | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Viva Dominicus Palace by Wyndham | Wyndham | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Excellence El Carmen | Excellence Resorts | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Melia Caribe Tropical | Meliá | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Villa Ibiscus | ibis | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Grand Bahia Principe Bavaro | Bahía Príncipe | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Grand Bahia Principe Aquamarine | Bahía Príncipe | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Hotel Riu República | RIU | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Amhsa Marina Grand Paradise Playa Dorada | Amhsa Marina Hotels | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Lopesan Costa Bávaro Resort, Spa & Casino | Lopesan | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| The Circle by Meliá | Meliá | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Catalonia Bavaro Royal | Catalonia | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Hotel Emotions by Hodelpa | Hodelpa | known_chain_census_backlog_not_active_setup | likely_new_candidate |
| Share Melvi Apartment | Barceló | known_chain_census_backlog_not_active_setup | likely_new_candidate |

## Error handling path

- **Validation fail:** payload not sent; listed in failures
- **HPC likely_existing:** skip insert; link / enrich existing row instead
- **HPC possible_duplicate_review:** hold for steward; do not auto-insert
- **API / network error:** retry; no silent catch

## Next apply gates (explicit founder approval required)

1. Spot-check Independent L1 validation-pass rows
2. Spot-check known-chain backlog vs Hotel Property Census holds
3. Separate Autopilot coverage run for Active/Live brands only
4. Keep known-chain-not-Active as Census inventory + Human Review; do not require Brand Explorer activation
