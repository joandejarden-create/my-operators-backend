# Census Intake Autopilot — Controlled Dry-Run

**Status:** `census_intake_controlled_dry_run_validation_failures`
**Version:** census-intake-autopilot-controlled-v1
**Batch:** osm-dominican-republic-hotel-focused-2026-08-07-url-enriched-v3
**Generated:** 2026-08-07T11:18:17.318Z
**Airtable writes:** no
**Write target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Legacy Hotel Census:** forbidden
**Cohort:** no_hr

## Counts

| Metric | Count |
| --- | ---: |
| Proposals | 28 |
| Validation pass | 27 |
| Validation fail | 1 |
| No Human Review | 28 |
| With Human Review | 0 |
| Queue Autopilot enrichment | 14 |
| Approval bundle ready | false |

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
| Bella Vista | Independent | Santo Domingo | false | High |
| El Pelicano Apart-Hotel | Independent | Las Galeras | false | High |
| Santo Domingo Bed & Breakfast | Independent | Santo Domingo | false | High |
| Hotel Tropicana Deluxe | Independent | Punta Cana | false | High |
| Hotel Villa La Plantacion | Independent | Las Galeras | false | High |
| Hotel Casa Coco | Independent | Boca Chica | false | High |
| Hotel & Apartments Buchen | Independent | Samaná | false | High |
| Casa Barbara Las Terrenas | Independent | Las Terrenas | false | High |
| Hotel BQ;Vent W&P Santo Domingo | Independent | Santo Domingo | false | High |
| Villas CODEVI | Independent | Dajabon | false | High |
| Hotel Anselmo | Independent | Boca Chica | false | High |
| Hotel Crowne Plaza | Crowne Plaza | Santo Domingo | false | High |
| Four Points by Sheraton | Four Points by Sheraton | Punta Cana | false | High |
| Sheraton | Sheraton | Santo Domingo | false | High |
| Radisson Hotel Santo Domingo | Radisson by Choice | Santo Domingo | false | High |
| Embassy Suites by Hilton | Hilton Hotels & Resorts | Santo Domingo | false | High |
| Radisson Blu Resort & Residence, Punta Cana | Radisson Blu by Choice | Punta Cana | false | High |
| Crowne Plaza | Crowne Plaza | Santo Domingo | false | High |
| Quality Inn | Quality Inn | Santo Domingo | false | High |
| Hilton La Romana | Hilton Hotels & Resorts | La Romana | false | High |
| Westin | Westin | Punta Cana | false | High |
| The Westin Punta Cana - Resort & Club | Westin | Punta Cana | false | High |

## Validation-fail sample

| Name | Failures |
| --- | --- |
| Casa Bonita | missing_required:Family / Source Family |

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
