# Airtable Sandbox Validation — VIC → Brand Explorer Pilot

**Status:** `airtable_sandbox_validated_ready_for_vic_be_patch`
**Generated:** 2026-08-05T07:29:57.357Z
**Version:** airtable-sandbox-validation-v1

## Base IDs

| Role | Masked ID | Name |
|------|-----------|------|
| Production (`AIRTABLE_BASE_ID`) | `appvtn…INP6` | (production — do not write) |
| Sandbox (`AIRTABLE_BASE_ID_SANDBOX`) | `appRbW…2ch1` | Deal Capture MVP — Sandbox |

- IDs differ: **true**
- VIC sandbox patch may execute: **true**

## Environment

| Variable | Value |
|----------|-------|
| AIRTABLE_ENV | `sandbox` |
| BE_PILOT_SANDBOX_CONFIRMED | `1` |
| AIRTABLE_BASE_ID_SANDBOX present | true |

## Checks

- [PASS] **airtable_env_equals_sandbox** — AIRTABLE_ENV=sandbox
- [PASS] **be_pilot_sandbox_confirmed** — BE_PILOT_SANDBOX_CONFIRMED=1
- [PASS] **sandbox_base_id_present** — AIRTABLE_BASE_ID_SANDBOX=appRbW…2ch1
- [PASS] **sandbox_base_differs_from_production** — IDs differ (prod appvtn…INP6 vs sandbox appRbW…2ch1)
- [PASS] **production_write_client_not_initialized** — Validator does not construct Airtable(production) write client
- [PASS] **sandbox_token_can_access_base** — AIRTABLE_PAT can see sandbox base "Deal Capture MVP — Sandbox"
- [PASS] **sandbox_base_name_includes_sandbox_staging_or_test** — base name="Deal Capture MVP — Sandbox"
- [PASS] **table_present_brand_setup_brand_basics** — table present (81 fields)
- [PASS] **table_present_brand_setup_brand_explorer_presentation** — table present (28 fields)
- [PASS] **basics_field_brand_name** — Brand Basics.Brand Name present
- [PASS] **basics_field_brand_status** — Brand Basics.Brand Status present
- [PASS] **presentation_field_title** — Presentation.Title present
- [PASS] **presentation_field_body** — Presentation.Body present
- [PASS] **presentation_brand_link_field** — Presentation brand link field="Brand"
- [PASS] **target_record_hotel_indigo** — found hotel-indigo as recegXrqaPiSLGCIe (exact_record_id) name="Hotel Indigo"
- [PASS] **target_record_ascend** — found ascend as reclkgOzvAcBheUSo (exact_record_id) name="Ascend Hotel Collection"
- [PASS] **target_record_curio_collection** — found curio-collection as receQkxgjlezsc1xg (exact_record_id) name="Curio Collection by Hilton"
- [PASS] **target_record_holiday_inn_express** — found holiday-inn-express as recmGmiIqDtAsm01f (exact_record_id) name="Holiday Inn Express"
- [PASS] **patch_execution_gated_on_validation** — validation PASS — VIC sandbox patch may execute only via sandbox write adapter

## Tables

| Table | Present |
|-------|---------|
| Brand Setup - Brand Basics | true |
| Brand Setup - Brand Explorer Presentation | true |

## Target records

| Slug | Expected ID | Sandbox ID | Found | Resolution |
|------|-------------|------------|-------|------------|
| hotel-indigo | `recegXrqaPiSLGCIe` | `recegXrqaPiSLGCIe` | true | exact_record_id |
| ascend | `reclkgOzvAcBheUSo` | `reclkgOzvAcBheUSo` | true | exact_record_id |
| curio-collection | `receQkxgjlezsc1xg` | `receQkxgjlezsc1xg` | true | exact_record_id |
| holiday-inn-express | `recmGmiIqDtAsm01f` | `recmGmiIqDtAsm01f` | true | exact_record_id |

## Write safety

- Production write client initialized: **false**
- Production Airtable writes: **false**
- Sandbox Airtable writes: **false**
- Patch execution allowed: **true**

## Blockers

- (none)

## Manual setup (if not ready)

1. In Airtable, duplicate production Brand Explorer base (Deal Capture MVP) as a new base.
2. Name it clearly, e.g. "Deal Capture MVP — Sandbox" or "… Staging / Test".
3. Ensure Brand Setup - Brand Basics + Brand Explorer Presentation (and pilot brand records) are present.
4. Copy the new base ID into .env as AIRTABLE_BASE_ID_SANDBOX=app…
5. Set AIRTABLE_ENV=sandbox
6. Set BE_PILOT_SANDBOX_CONFIRMED=1
7. Keep AIRTABLE_BASE_ID pointed at production for read-only comparison only.
8. Re-run: npm run research-engine-v2:validate-airtable-sandbox
