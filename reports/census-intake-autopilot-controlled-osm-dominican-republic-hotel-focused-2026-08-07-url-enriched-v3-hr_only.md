# Census Intake Autopilot — Controlled Dry-Run

**Status:** `census_intake_controlled_dry_run_ready_for_apply_gate`
**Version:** census-intake-autopilot-controlled-v1
**Batch:** osm-dominican-republic-hotel-focused-2026-08-07-url-enriched-v3
**Generated:** 2026-08-07T11:18:15.263Z
**Airtable writes:** no
**Write target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Legacy Hotel Census:** forbidden
**Cohort:** hr_only

## Counts

| Metric | Count |
| --- | ---: |
| Proposals | 95 |
| Validation pass | 95 |
| Validation fail | 0 |
| No Human Review | 0 |
| With Human Review | 95 |
| Queue Autopilot enrichment | 0 |
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
| Breezes | Breezes (SuperClubs) | Puerto Plata | true | High |
| Dreams Palm Beach Resort | Dreams (Hyatt Inclusive Collection) | Punta Cana | true | High |
| Natura Park Beach Eco Resort & Spa | Blau Hotels | Punta Cana | true | High |
| Catalonia Punta Cana | Catalonia | Punta Cana | true | Medium |
| Be Live Grand Bavaro | Be Live | Bávaro | true | High |
| Wyndham Alltra Punta Cana | Wyndham | Punta Cana | true | Medium |
| Hotel RIU Mambo | RIU | Maimón | true | High |
| Hotel RIU Merengue | RIU | Maimón | true | High |
| Hotel RIU Bachata | RIU | Maimón | true | High |
| Barceló Bávaro Palace Deluxe | Barceló | Bávaro | true | Medium |
| Hodelpa Novus Plaza | Hodelpa | Santo Domingo | true | High |
| Be Live Grand Marien Hotel | Be Live | Puerto Plata | true | High |
| Emotion By Hodelpa. | Hodelpa | Puerto Plata | true | Medium |
| Secrets Royal Beach Punta Cana | Hyatt | Punta Cana | true | High |
| Hard Rock Hotel & Casino Punta Cana - All inclusive | Hard Rock Hotels | Punta Cana | true | High |
| Dreams Royal Beach Punta Cana | Dreams (Hyatt Inclusive Collection) | Punta Cana | true | High |
| Hotel Occidental Allegro Playa Dorada | Occidental | Puerto Plata | true | High |
| Barceló Puerto Plata | Barceló | Puerto Plata | true | High |
| Occidental Caribe (former Barcelo Punta Cana) | Barceló | Punta Cana | true | Medium |
| Occidental Caribe (former Barcelo Punta Cana) | Barceló | Bavaro | true | High |
| Grand Bahia Principe Punta Cana | Bahía Príncipe | Punta Cana | true | Medium |
| Riu Palace Punta Cana | RIU | Punta Cana | true | High |
| Excellence Punta Cana | Excellence Resorts | Punta Cana | true | High |
| Real Intercontinental | InterContinental | Santo Domingo | true | High |
| Breathless Resort | Breathless (Hyatt Inclusive Collection) | Playas | true | High |

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
