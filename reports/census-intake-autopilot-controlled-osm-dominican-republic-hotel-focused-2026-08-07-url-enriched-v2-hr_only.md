# Census Intake Autopilot — Controlled Dry-Run

**Status:** `census_intake_controlled_dry_run_ready_for_apply_gate`
**Version:** census-intake-autopilot-controlled-v1
**Batch:** osm-dominican-republic-hotel-focused-2026-08-07-url-enriched-v2
**Generated:** 2026-08-07T10:53:39.380Z
**Airtable writes:** no
**Write target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Legacy Hotel Census:** forbidden
**Cohort:** hr_only

## Counts

| Metric | Count |
| --- | ---: |
| Proposals | 75 |
| Validation pass | 75 |
| Validation fail | 0 |
| No Human Review | 0 |
| With Human Review | 75 |
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
| Natura Park Beach Eco Resort & Spa | Blau Hotels | Punta Cana | true | High |
| Catalonia Punta Cana | Catalonia | Unknown | true | Medium |
| Wyndham Alltra Punta Cana | Wyndham | Unknown | true | Medium |
| Hotel RIU Mambo | RIU | Maimón | true | High |
| Hotel RIU Merengue | RIU | Maimón | true | High |
| Hotel RIU Bachata | RIU | Maimón | true | High |
| Barceló Bávaro Palace Deluxe | Barceló | Unknown | true | Medium |
| Hodelpa Novus Plaza | Hodelpa | Santo Domingo | true | High |
| Emotion By Hodelpa. | Hodelpa | Unknown | true | Medium |
| Secrets Royal Beach Punta Cana | Hyatt | Punta Cana | true | High |
| Hard Rock Hotel & Casino Punta Cana - All inclusive | Hard Rock Hotels | Punta Cana | true | High |
| Dreams Royal Beach Punta Cana | Dreams (Hyatt Inclusive Collection) | Punta Cana | true | High |
| Occidental Caribe (former Barcelo Punta Cana) | Barceló | Unknown | true | Medium |
| Occidental Caribe (former Barcelo Punta Cana) | Barceló | Bavaro | true | High |
| Grand Bahia Principe Punta Cana | Bahía Príncipe | Unknown | true | Medium |
| Riu Palace Punta Cana | RIU | Punta Cana | true | High |
| Excellence Punta Cana | Excellence Resorts | Punta Cana | true | High |
| Viva Dominicus Palace by Wyndham | Wyndham | Unknown | true | Medium |
| Excellence El Carmen | Excellence Resorts | Punta Cana | true | High |
| Villa Ibiscus | ibis | Unknown | true | Medium |
| Grand Bahia Principe Aquamarine | Bahía Príncipe | Unknown | true | Medium |
| Hotel Riu República | RIU | Unknown | true | Medium |
| Amhsa Marina Grand Paradise Playa Dorada | Amhsa Marina Hotels | Unknown | true | Medium |
| Lopesan Costa Bávaro Resort, Spa & Casino | Lopesan | Punta Cana | true | High |
| Hotel Emotions by Hodelpa | Hodelpa | Unknown | true | Medium |

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
