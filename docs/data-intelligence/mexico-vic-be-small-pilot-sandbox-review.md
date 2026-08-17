# Mexico VIC → Brand Explorer Small Pilot — Sandbox Review

**Status:** `vic_be_small_pilot_sandbox_review_approved_continue_expanded_sandbox`
**Recommendation:** `approve_sandbox_result_continue_to_expanded_sandbox_pilot`
**Generated:** 2026-08-05T07:37:10.978Z
**Expanded sandbox pilot may proceed:** **true**

## 1. What changed in sandbox

- Created **16** Presentation rows with Slot Key `vic.pilot.*`
- Fields: Title, Body, Slot Key, Brand (link)
- Brands: hotel-indigo, ascend, curio-collection, holiday-inn-express
- Freeze lineage: `c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3`

## 2. What did not change

- Production Airtable (`appvtn…INP6`) — **0 writes**
- Brand Status / Company Validated / Brand Verified
- Recent Momentum
- Release fields
- Frozen 62 + frozen VIC artifacts

## 3. Airtable review instructions

1. Open Airtable base: Deal Capture MVP — Sandbox
2. Go to table: Brand Setup - Brand Explorer Presentation
3. Filter: Slot Key contains vic.pilot.
4. Confirm exactly 16 rows across hotel-indigo, ascend, curio-collection, holiday-inn-express
5. Read Title + Body for each row as owner-facing copy
6. Do not edit Brand Status, Company Validated, Brand Verified, or Recent Momentum
7. Do not apply these rows to production yet

## 4. Brand-by-brand summary

### hotel-indigo

- Rows: **4**
- Properties: Hotel Indigo Guadalajara Expo; Hotel Indigo Playa del Carmen; Hotel Indigo Guanajuato
- Assessment: **pass**

### ascend

- Rows: **4**
- Properties: Amberes 64; El Cid Castilla; El Cid La Ceiba
- Assessment: **pass**

### curio-collection

- Rows: **4**
- Properties: Amare Cancun; The Fives Downtown; MS Milenium (San Pedro Garza García)
- Assessment: **pass**

### holiday-inn-express

- Rows: **4**
- Properties: Holiday Inn Express Querétaro
- Assessment: **pass**

## 5. Row-by-row review table

| Brand | Slot Key | Title | Body Summary | Review Status | Issue |
|-------|----------|-------|--------------|---------------|-------|
| Ascend Hotel Collection | `vic.pilot.geographic_footprint_mexico` | Mexico Geographic Footprint | Ascend Hotel Collection is present in Mexico across Mexico City, Mazatlán, and Cozumel. | pass | — |
| Holiday Inn Express | `vic.pilot.owner_facing_copy` | Mexico Owner-Facing Notes | A Mexico example is Holiday Inn Express & Suites Querétaro — a practical midscale select-service reference in a major inland commercial mar… | pass | audience_owner_address |
| Holiday Inn Express | `vic.pilot.geographic_footprint_mexico` | Mexico Geographic Footprint | Holiday Inn Express is present in Mexico, including Querétaro. | pass | — |
| Holiday Inn Express | `vic.pilot.portfolio_context` | Mexico Portfolio Context | This property supports Mexico midscale select-service context for owners reviewing Holiday Inn Express in secondary commercial cities. | pass | audience_owner_address |
| Holiday Inn Express | `vic.pilot.property_examples` | Mexico Property Examples | A Mexico example is Holiday Inn Express & Suites Querétaro — a practical midscale select-service reference in a major inland commercial mar… | pass | — |
| Hotel Indigo | `vic.pilot.owner_facing_copy` | Mexico Owner-Facing Notes | Mexico examples include Hotel Indigo Guadalajara Expo, Hotel Indigo Playa del Carmen, and Hotel Indigo Guanajuato — each reflecting the bra… | pass | audience_owner_address |
| Curio Collection by Hilton | `vic.pilot.portfolio_context` | Mexico Portfolio Context | These properties show Curio’s flexibility across leisure all-inclusive and urban/lifestyle Mexico markets. | pass | — |
| Curio Collection by Hilton | `vic.pilot.property_examples` | Mexico Property Examples | Mexico examples include Amare Cancun, The Fives Downtown in Playa del Carmen, and MS Milenium in San Pedro Garza García — covering all-incl… | pass | — |
| Ascend Hotel Collection | `vic.pilot.portfolio_context` | Mexico Portfolio Context | The Mexico examples show how independent and soft-brand hotels can participate in Ascend Hotel Collection across capital and coastal leisur… | pass | — |
| Hotel Indigo | `vic.pilot.portfolio_context` | Mexico Portfolio Context | These Mexico properties illustrate how Hotel Indigo can express local character in both urban and leisure settings. | pass | — |
| Ascend Hotel Collection | `vic.pilot.owner_facing_copy` | Mexico Owner-Facing Notes | Mexico examples include Amberes 64 in Mexico City, El Cid Castilla Beach Hotel in Mazatlán, and El Cid La Ceiba Beach Hotel in Cozumel — il… | pass | audience_owner_address |
| Hotel Indigo | `vic.pilot.geographic_footprint_mexico` | Mexico Geographic Footprint | Hotel Indigo is present in Mexico across gateway and leisure markets, including Guadalajara, Playa del Carmen, and Guanajuato. | pass | — |
| Hotel Indigo | `vic.pilot.property_examples` | Mexico Property Examples | Mexico examples include Hotel Indigo Guadalajara Expo, Hotel Indigo Playa del Carmen, and Hotel Indigo Guanajuato — each reflecting the bra… | pass | — |
| Curio Collection by Hilton | `vic.pilot.geographic_footprint_mexico` | Mexico Geographic Footprint | Curio Collection by Hilton is present in Mexico across Cancún, Playa del Carmen, and the Monterrey metro (San Pedro Garza García). | pass | — |
| Curio Collection by Hilton | `vic.pilot.owner_facing_copy` | Mexico Owner-Facing Notes | Mexico examples include Amare Cancun, The Fives Downtown in Playa del Carmen, and MS Milenium in San Pedro Garza García — covering all-incl… | pass | audience_owner_address |
| Ascend Hotel Collection | `vic.pilot.property_examples` | Mexico Property Examples | Mexico examples include Amberes 64 in Mexico City, El Cid Castilla Beach Hotel in Mazatlán, and El Cid La Ceiba Beach Hotel in Cozumel — il… | pass | — |

## 6. Copy quality assessment

- Reject count: **0**
- Audience address ('Owners evaluating…') is allowed; property-identity owner/operator/rooms/date claims are rejected.

- No forbidden-language rejects.

## 7. Property ruling assessment

- [PASS] ascend_amberes_mentioned — Amberes 64 present in Ascend copy
- [PASS] ascend_el_cid_castilla — El Cid Castilla present
- [PASS] ascend_el_cid_la_ceiba — El Cid La Ceiba present
- [PASS] ascend_soft_brand_framing — Soft-brand distribution framing present
- [PASS] ascend_no_choice_owns — No Choice ownership claim
- [PASS] ascend_denies_choice_assumption_ok — Steward denial language or no ownership claim
- [PASS] ascend_no_faranda — No Faranda claim
- [PASS] ascend_no_direct_mgmt_claim — No affirmative direct management claim (steward denial language allowed)
- [PASS] ascend_no_vic_momentum — No Recent Momentum from VIC in Ascend pilot rows
- [PASS] curio_amare — Amare Cancun present
- [PASS] curio_fives — The Fives Downtown present
- [PASS] curio_ms_milenium_san_pedro — MS Milenium city = San Pedro Garza García
- [PASS] curio_monterrey_metro_context — Monterrey metro context present
- [PASS] curio_no_hilton_owns — No Hilton ownership claim
- [PASS] curio_no_vic_momentum — No Recent Momentum from VIC in Curio pilot rows
- [PASS] indigo_three_properties — Indigo Guadalajara Expo, Playa del Carmen, Guanajuato present
- [PASS] indigo_no_false_momentum — No false momentum on Indigo
- [PASS] hie_queretaro — Holiday Inn Express Querétaro present
- [PASS] hie_no_false_momentum — No false momentum on HIE

## 8. Row inventory

| Metric | Value |
|--------|-------|
| Expected | 16 |
| Found | 16 |
| Missing | — |
| Duplicates | — |
| Unexpected | 0 |
| Wrong slot | 0 |
| Row count OK | true |

## 9. Risk assessment

- **production_overwrite:** none — read-only review; prior patch was sandbox-only
- **brand_status_drift:** none observed in this review (Basics Status not mutated)
- **momentum_pollution:** vic.pilot.* slots do not write Recent Momentum
- **copy_quality:** pass
- **ruling_integrity:** pass
- **expansion_risk:** low — small pilot pattern can expand in sandbox with same guardrails

## 10. Recommendation

`approve_sandbox_result_continue_to_expanded_sandbox_pilot`

Do **not** recommend production patch from this review.

## Production safety

- Production writes: **0**
- Sandbox isolated: **true**
- Expected production Active universe: **62**
- Expected production semantic: **C/H/M 0/0/0**


## Scope

Read-only founder review packet. No production recommendation unless requested separately.
