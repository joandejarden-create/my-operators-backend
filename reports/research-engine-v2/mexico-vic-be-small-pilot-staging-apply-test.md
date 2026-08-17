# Mexico VIC → BE Small Pilot Staging-Only Apply Test

**Status:** `mexico_vic_be_small_pilot_staging_apply_test_ready_for_sandbox_patch_decision`  
**Generated:** 2026-08-04T23:35:43.255Z  
**Freeze hash:** `c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3`  
**Patch executed:** **NO**  
**Airtable writes:** **NO**

---

## Executive summary

Simulated staging-only Brand Explorer completion patches for **4 brands / 10 properties**. Proposed payload generated for review only — **not executed**.

| Metric | Value |
|--------|------:|
| Proposed patch ops | 16 |
| Brands | 4 |
| Pilot properties | 10 |
| Validation issues | 0 |
| Protected 54 regression | PASS |
| Semantic audit ok | no |
| Sandbox patch may proceed later | **NO** |

---

## Target mapping

| Slug | Record ID | Pilot props |
|------|-----------|------------:|
| `hotel-indigo` | recegXrqaPiSLGCIe | 3 |
| `ascend` | reclkgOzvAcBheUSo | 3 |
| `curio-collection` | receQkxgjlezsc1xg | 3 |
| `holiday-inn-express` | recmGmiIqDtAsm01f | 1 |

---

## Fields touched (simulation only)

- property_examples
- geographic_footprint_mexico
- portfolio_context
- property_proof
- owner_facing_copy

## Fields explicitly not touched

- Recent Momentum (unless separately dated — not from VIC)
- Brand Status
- release fields
- Company Validated
- Brand Verified
- Active Profile Approved
- Ready for Active Profile
- Founder Visual Review Pass
- rooms
- owner
- operator
- opening date
- affiliation start date
- production IDs
- Airtable mutation execution

---

## Before / after by brand

### Hotel Indigo (`hotel-indigo`)

**Before (read-only snapshot)**  
- Region basis: International Reference  
- Fixture rows: 0  
- Mexico-related fixture hits: 0  
- Recent Momentum cards observed: 0 (unchanged by this test)

**Proposed staging overlay**  
- Property examples: Hotel Indigo Guadalajara Expo; Hotel Indigo Playa del Carmen; Hotel Indigo Guanajuato  
- Mexico cities: Guadalajara, Playa Del Carmen, Guanajuato  
- Recent Momentum: **unchanged**

**Owner-facing preview**  
Mexico examples include Hotel Indigo Guadalajara Expo, Hotel Indigo Playa del Carmen, and Hotel Indigo Guanajuato — each reflecting the brand’s neighborhood-led positioning across distinct Mexican markets.

Hotel Indigo is present in Mexico across gateway and leisure markets, including Guadalajara, Playa del Carmen, and Guanajuato.

### Ascend Hotel Collection (`ascend`)

**Before (read-only snapshot)**  
- Region basis: CALA-specific  
- Fixture rows: 195  
- Mexico-related fixture hits: 2  
- Recent Momentum cards observed: 6 (unchanged by this test)

**Proposed staging overlay**  
- Property examples: Amberes 64, an Ascend Collection Hotel; El Cid Castilla Beach Hotel; El Cid La Ceiba Beach Hotel  
- Mexico cities: Mexico City, Mazatlan, Cozumel  
- Recent Momentum: **unchanged**

**Owner-facing preview**  
Mexico examples include Amberes 64 in Mexico City, El Cid Castilla Beach Hotel in Mazatlán, and El Cid La Ceiba Beach Hotel in Cozumel — illustrating Ascend Hotel Collection’s soft-brand distribution across urban boutique and beach resort formats.

Ascend Hotel Collection is present in Mexico across Mexico City, Mazatlán, and Cozumel.

### Curio Collection by Hilton (`curio-collection`)

**Before (read-only snapshot)**  
- Region basis: International Reference  
- Fixture rows: 231  
- Mexico-related fixture hits: 18  
- Recent Momentum cards observed: 3 (unchanged by this test)

**Proposed staging overlay**  
- Property examples: Amare Cancun Adults Only All-Inclusive Resort, Curio by Hilton; The Fives Downtown Hotel & Residences, Curio Collection by Hilton; MS Milenium Monterrey, Curio Collection by Hilton  
- Mexico cities: Cancun, Playa del Carmen, San Pedro Garza García  
- Recent Momentum: **unchanged**

**Owner-facing preview**  
Mexico examples include Amare Cancun, The Fives Downtown in Playa del Carmen, and MS Milenium in San Pedro Garza García — covering all-inclusive resort, lifestyle downtown, and Monterrey-metro urban Curio expressions.

Curio Collection by Hilton is present in Mexico across Cancún, Playa del Carmen, and the Monterrey metro (San Pedro Garza García).

### Holiday Inn Express (`holiday-inn-express`)

**Before (read-only snapshot)**  
- Region basis: CALA-informed  
- Fixture rows: 0  
- Mexico-related fixture hits: 0  
- Recent Momentum cards observed: 0 (unchanged by this test)

**Proposed staging overlay**  
- Property examples: Holiday Inn Express & Suites Queretaro  
- Mexico cities: Queretaro  
- Recent Momentum: **unchanged**

**Owner-facing preview**  
A Mexico example is Holiday Inn Express & Suites Querétaro — a practical midscale select-service reference in a major inland commercial market.

Holiday Inn Express is present in Mexico, including Querétaro.


---

## Diff summary (selected)

| Brand | Field | Proposed after (summary) | Risk | Steward ref |
|-------|-------|--------------------------|------|-------------|
| `hotel-indigo` | property_examples | 3 items | low | steward clean |
| `hotel-indigo` | geographic_footprint_mexico | object | low | steward clean |
| `hotel-indigo` | portfolio_context | object | low | steward clean |
| `hotel-indigo` | property_proof | 3 items | low | steward clean |
| `hotel-indigo` | owner_facing_copy | object | low | steward clean |
| `ascend` | property_examples | 3 items | low_after_steward | steward clean |
| `ascend` | geographic_footprint_mexico | object | low | steward clean |
| `ascend` | portfolio_context | object | low | steward clean |
| `ascend` | property_proof | 3 items | low | steward clean |
| `ascend` | owner_facing_copy | object | low | steward clean |
| `curio-collection` | property_examples | 3 items | low | steward clean |
| `curio-collection` | geographic_footprint_mexico | object | low | steward clean |
| `curio-collection` | portfolio_context | object | low | steward clean |
| `curio-collection` | property_proof | 3 items | low | steward clean |
| `curio-collection` | owner_facing_copy | object | low | steward clean |
| `holiday-inn-express` | property_examples | 1 items | low | steward clean |
| `holiday-inn-express` | geographic_footprint_mexico | object | low | steward clean |
| `holiday-inn-express` | portfolio_context | object | low | steward clean |
| `holiday-inn-express` | property_proof | 1 items | low | steward clean |
| `holiday-inn-express` | owner_facing_copy | object | low | steward clean |

Recent Momentum rows: all **UNCHANGED**.

---

## Steward rulings preserved

- El Cid: Ascend soft-brand distribution examples only  
- Amberes 64: property proof only (no VIC momentum)  
- MS Milenium city: **San Pedro Garza García**

---

## Validation

| Check | Result |
|-------|--------|
| Empty components | PASS |
| Forbidden terms | PASS |
| False momentum | PASS |
| Slug mapping | PASS |
| Momentum unchanged | PASS |
| El Cid soft-brand framing | PASS |
| Milenium city normalized | PASS |
| Airtable write detection | NONE |
| Production write detection | NONE |

Issues: _none_

---

## Protected baseline checks

### `test:brand-explorer-54-active-public-full-baseline`
- ok: **true** (exit 0)
- activeCount freeze artifact: **54**

### `brand-explorer-global-active-semantic-audit --dry-run --fresh`
- ok: **false** (exit 3)
- counts parsed: Critical=0 High=0 Medium=7

---

## Risks remaining

- Sandbox Airtable patch still requires explicit founder/steward approval
- Local fixture before-state is incomplete for Hotel Indigo / Holiday Inn Express (freeze metadata only)
- Semantic/PVQL live checks depend on environment credentials and freshness
- Live semantic audit is not clean vs protected 54 expectations (Active≠54 and/or Medium>0) — resolve live-universe drift before sandbox patch; this staging simulation did not write BE/Airtable

---

## Sandbox patch decision

**Controlled Airtable sandbox patch may proceed later:** **NO**

Next: `hold_sandbox_until_live_semantic_universe_matches_protected_54`

Artifacts:
- `be-small-pilot-staging-patch-proposal.json` (execute:false)
- `be-small-pilot-before-after-diff.json`
- `be-small-pilot-rendered-preview.json`

---

## Acceptance

- [x] Four brands mapped · ten properties included  
- [x] Patch payload generated, **not executed**  
- [x] Before/after + rendered preview generated  
- [x] No VIC Recent Momentum · El Cid / Amberes / Milenium rulings preserved  
- [x] Owner-facing preview clean of internal language / raw URLs  
- [x] No fake rooms/owners/open dates/start dates / CV / Brand Verified  
- [x] No Airtable / BE activation / production overwrite / Webhound  
- [x] Status: `mexico_vic_be_small_pilot_staging_apply_test_ready_for_sandbox_patch_decision`
