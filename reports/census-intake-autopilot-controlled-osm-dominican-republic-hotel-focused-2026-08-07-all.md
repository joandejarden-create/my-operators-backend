# Census Intake Autopilot — Controlled Dry-Run

**Status:** `census_intake_controlled_dry_run_ready_for_apply_gate`
**Version:** census-intake-autopilot-controlled-v1
**Batch:** osm-dominican-republic-hotel-focused-2026-08-07
**Generated:** 2026-08-07T08:52:26.407Z
**Airtable writes:** no
**Write target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Legacy Hotel Census:** forbidden
**Cohort:** all

## Counts

| Metric | Count |
| --- | ---: |
| Proposals | 62 |
| Validation pass | 62 |
| Validation fail | 0 |
| No Human Review | 19 |
| With Human Review | 43 |
| Queue Autopilot enrichment | 3 |
| Approval bundle ready | true |

## Apply confirms (future — not run)

- `--confirm-safe-writes`
- `--confirm-write-to-production-census`
- `--confirm-no-brand-explorer-writes`
- `--confirm-no-owner-operator`
- `--confirm-no-date-writes`
- `--confirm-no-recent-momentum`
- `--confirm-no-company-validation`
- `--confirm-webhound-not-production-source`
- `--confirm-intake-inserts`
- `--confirm-no-legacy-hotel-census`

## Validation-pass sample

| Name | Brand | City | HR | Identity |
| --- | --- | --- | --- | --- |
| Hotel Europa | Independent | Sosua | false | High |
| Bungalows of Las Galeras | Independent | Las Galeras | false | High |
| Jarabacua River Club & Resort | Independent | Jarabacoa | false | High |
| Bella Vista | Independent | Santo domingo | false | High |
| El Pelicano Apart-Hotel | Independent | Las Galeras | false | High |
| Santo Domingo Bed & Breakfast | Independent | Santo Domingo | false | High |
| Hotel Tropicana Deluxe | Independent | Punta Cana | false | High |
| Hotel Villa La Plantacion | Independent | Las Galeras | false | High |
| Hotel Casa Coco | Independent | Boca Chica | false | High |
| Hotel & Apartments Buchen | Independent | Samaná | false | High |
| Casa Barbara Las Terrenas | Independent | Las Terrenas | false | High |
| Hotel BQ;Vent W&P Santo Domingo | Independent | Santo Domingo | false | High |
| Hostal Ganesh | Independent | El Valle, Samana | false | High |
| Hotal San Francisco de Asis | Independent | Santo Domingo | false | High |
| Villas CODEVI | Independent | Dajabon | false | High |
| Hotel Anselmo | Independent | Boca Chica | false | High |
| Natura Park Beach Eco Resort & Spa | Blau Hotels | Punta Cana | true | High |
| Hotel RIU Mambo | RIU | Maimón | true | High |
| Hotel RIU Merengue | RIU | Maimón | true | High |
| Hotel RIU Bachata | RIU | Maimón | true | High |
| Barceló Bávaro Palace Deluxe | Barceló | Unknown | true | Medium |
| Secrets Royal Beach Punta Cana | Hyatt | Punta Cana | true | High |
| Hard Rock Hotel & Casino Punta Cana - All inclusive | Hard Rock Hotels | Punta Cana | true | High |
| Dreams Royal Beach Punta Cana | Dreams (Hyatt Inclusive Collection) | Punta Cana | true | High |
| Excellence Punta Cana | Excellence Resorts | Punta Cana | true | High |

## Validation-fail sample

| Name | Failures |
| --- | --- |

## Data contract snapshot

- **Table:** Hotel Property Census
- **Field mapping:** intake dual-lane → `INTAKE_INSERT_ALLOWED_FIELDS`
- **Required:** Property Name, Property Identity Key, Country, City, Current Brand, Affiliation Status, Family / Source Family, Source URL, VIC Freeze Hash, Production Use Status, Enrichment Status, Human Review Required
- **Forbidden:** owner/operator/dates/momentum/Brand Explorer/Company Validated

## Change impact

- **Classification:** High (Census inserts)
- **Rollback:** delete by VIC Freeze Hash / Property Identity Key prefix `osm_do_` for this batch
- **Modules:** intake-autopilot-controlled, Hotel Property Census only

## Next

1. Spot-check validation-fail rows (if any)
2. Prefer first apply cohort `--cohort no_hr`
3. Explicit founder approval + apply script with all confirms
