# Mexico VIC → BE Small Pilot — Rebase Against Frozen 62

**Status:** `mexico_vic_be_small_pilot_sandbox_not_proven_do_not_execute`  
**Generated:** 2026-08-05T01:55:20.536Z

## Frozen 62

| Check | Result |
|-------|--------|
| Decision | `frozen_62_active_public_full_baseline_semantic_clean_flex_held` |
| Frozen | true |
| Active count | 62 |
| Confirmed | true |

## Brand reconfirm (under 62)

| Slug | Record ID | Status | Drifted |
|------|-----------|--------|---------|
| hotel-indigo | `recegXrqaPiSLGCIe` | Active | false |
| ascend | `reclkgOzvAcBheUSo` | Active | false |
| curio-collection | `receQkxgjlezsc1xg` | Active | false |
| holiday-inn-express | `recmGmiIqDtAsm01f` | Active | false |

## Property reconfirm (10)

| Property | Brand | OK |
|----------|-------|----|
| Hotel Indigo Guadalajara Expo | hotel-indigo | true |
| Hotel Indigo Playa del Carmen | hotel-indigo | true |
| Hotel Indigo Guanajuato | hotel-indigo | true |
| Amberes 64 | ascend | true |
| El Cid Castilla | ascend | true |
| El Cid La Ceiba | ascend | true |
| Amare Cancun | curio-collection | true |
| The Fives Downtown | curio-collection | true |
| MS Milenium Monterrey | curio-collection | true |
| Holiday Inn Express Queretaro | holiday-inn-express | true |

## Rulings preserved

- **el_cid_soft_brand_distribution_only:** true
- **no_choice_ownership_claim:** true
- **no_faranda_claim:** true
- **no_direct_management_claim:** true
- **no_recent_momentum_from_vic:** true
- **amberes_property_proof_only:** true
- **ms_milenium_city_san_pedro:** true
- **source_url_unchanged_note:** Official source URLs remain internal-only in staging proposal; not rewritten in this rebase
- **freeze_hash_preserved:** true

## Op rebase (16)

| # | Brand | Slug | Field | Proposed | Valid under 62? | Risk | Action |
|---|-------|------|-------|----------|-----------------|------|--------|
| 1 | Hotel Indigo | hotel-indigo | property_examples | 3 property example(s) | true | low | `keep_for_sandbox_patch` |
| 2 | Hotel Indigo | hotel-indigo | geographic_footprint_mexico | Mexico cities: Guadalajara, Playa Del Carmen, Guanajuato | true | low | `keep_for_sandbox_patch` |
| 3 | Hotel Indigo | hotel-indigo | portfolio_context | portfolio context (3 props) | true | low | `keep_for_sandbox_patch` |
| 4 | Hotel Indigo | hotel-indigo | owner_facing_copy | steward-approved owner-facing copy blocks | true | low | `keep_for_sandbox_patch` |
| 5 | Ascend Hotel Collection | ascend | property_examples | 3 property example(s) | true | low_after_steward | `keep_for_sandbox_patch` |
| 6 | Ascend Hotel Collection | ascend | geographic_footprint_mexico | Mexico cities: Mexico City, Mazatlan, Cozumel | true | low_after_steward | `keep_for_sandbox_patch` |
| 7 | Ascend Hotel Collection | ascend | portfolio_context | portfolio context (3 props) | true | low_after_steward | `keep_for_sandbox_patch` |
| 8 | Ascend Hotel Collection | ascend | owner_facing_copy | steward-approved owner-facing copy blocks | true | low_after_steward | `keep_for_sandbox_patch` |
| 9 | Curio Collection by Hilton | curio-collection | property_examples | 3 property example(s) | true | low | `keep_for_sandbox_patch` |
| 10 | Curio Collection by Hilton | curio-collection | geographic_footprint_mexico | Mexico cities: Cancun, Playa del Carmen, San Pedro Garza García | true | low | `keep_for_sandbox_patch` |
| 11 | Curio Collection by Hilton | curio-collection | portfolio_context | portfolio context (3 props) | true | low | `keep_for_sandbox_patch` |
| 12 | Curio Collection by Hilton | curio-collection | owner_facing_copy | steward-approved owner-facing copy blocks | true | low | `keep_for_sandbox_patch` |
| 13 | Holiday Inn Express | holiday-inn-express | property_examples | 1 property example(s) | true | low | `keep_for_sandbox_patch` |
| 14 | Holiday Inn Express | holiday-inn-express | geographic_footprint_mexico | Mexico cities: Queretaro | true | low | `keep_for_sandbox_patch` |
| 15 | Holiday Inn Express | holiday-inn-express | portfolio_context | portfolio context (1 props) | true | low | `keep_for_sandbox_patch` |
| 16 | Holiday Inn Express | holiday-inn-express | owner_facing_copy | steward-approved owner-facing copy blocks | true | low | `keep_for_sandbox_patch` |

### Action counts

- keep_for_sandbox_patch: **16**
- revise_before_sandbox_patch: **0**
- hold_do_not_patch: **0**

## Sandbox isolation

- Decision: `sandbox_not_proven_do_not_execute`
- Production base: `appvtn…INP6`
- Sandbox base: `(unset)`
- Execute allowed: **false**

- [FAIL] sandbox_base_env_present: no AIRTABLE_*_SANDBOX / STAGING base env set
- [FAIL] sandbox_base_differs_from_production: cannot compare — missing sandbox and/or production base
- [FAIL] explicit_sandbox_confirmation_flag: BE_PILOT_SANDBOX_CONFIRMED not set to 1
- [FAIL] airtable_env_sandbox_or_test: AIRTABLE_ENV unset
- [FAIL] write_target_not_production_base: write target must be sandbox/test base, not AIRTABLE_BASE_ID
- [PASS] alt_platform_base_not_used_as_sandbox_by_default: AIRTABLE_BASE_ID_ALT present (appCCU…foLk) — not treated as BE sandbox

## Production safety

- Status: `production_safety_ok_no_writes`
- Production patch blocked: **true**
- Sandbox execution happened: **false**
- Frozen 62 modified: **false**
- Frozen VIC modified: **false**

## Recommended next step

Provision a dedicated sandbox/test Airtable base, set AIRTABLE_BASE_ID_SANDBOX + BE_PILOT_SANDBOX_CONFIRMED=1 + AIRTABLE_ENV=sandbox, then re-run this command. Production patch remains blocked.

## Constraints

- No Webhound
- No production Airtable writes
- No Brand Explorer activation
- No Brand Status / release / CV / Brand Verified / Recent Momentum-from-VIC changes

## Production protected checks (post-rebase, read-only)

| Check | Result |
|-------|--------|
| Active universe SoT | PASS — 62 |
| Semantic audit (fresh) | PASS — C/H/M 0/0/0 |
| Quiet PVQL | Cited frozen 62 PASS 62/62 (no writes; not re-run) |
| Recent Momentum evidence | PASS |
| Mandatory release gates | PASS |
