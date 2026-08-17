# Mexico VIC → BE Small Pilot Minor Steward Review

**Status:** `mexico_vic_be_small_pilot_minor_steward_review_clean_ready_for_staging_apply_test`  
**Generated:** 2026-08-04T23:27:45.935Z  
**Freeze hash:** `c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3`  
**Staging-only apply test may proceed:** **YES**

No Airtable · No Webhound · No BE activation · No BE record writes · No production overwrite · Freeze unmodified · No Recent Momentum from directory existence

---

## 1. Alias decisions

### Ascend Hotel Collection
| Field | Value |
|-------|-------|
| Requested | `ascend-hotel-collection` |
| Active BE | `ascend` (Ascend Hotel Collection) |
| Decision | **`approved_alias_mapping`** |
| Rationale | Requested `ascend-hotel-collection` is not an Active/Live BE54 slug. Live brand is `ascend` (Ascend Hotel Collection). Overlay-only alias approved; do not rename BE records. |

### Curio Collection by Hilton
| Field | Value |
|-------|-------|
| Requested | `curio-collection-by-hilton` |
| Active BE | `curio-collection` (Curio Collection by Hilton) |
| Decision | **`approved_alias_mapping`** |
| Rationale | Requested `curio-collection-by-hilton` is not an Active/Live BE54 slug. Live brand is `curio-collection` (Curio Collection by Hilton). Overlay-only alias approved; do not rename BE records. |

### Exact matches confirmed
- `hotel-indigo` → `hotel-indigo`
- `holiday-inn-express` → `holiday-inn-express`

---

## 2. Property-specific decisions

| Property | Decision | Framing / notes |
|----------|----------|-----------------|
| El Cid Castilla Beach Hotel | `approved_soft_brand_distribution_example` | Frame as an Ascend Hotel Collection soft-brand distribution example in Mazatlán. Independent resort identity remains El Cid; Ascend is the collection affiliation evidenced by the official Choice property page. |
| El Cid La Ceiba Beach Hotel | `approved_soft_brand_distribution_example` | Same handling as El Cid Castilla — Cozumel soft-brand Ascend distribution example only. |
| Amberes 64, an Ascend Collection Hotel | `approved_property_proof_only` | Amberes 64 is property proof / Mexico City example only in this pilot. |
| MS Milenium Monterrey, Curio Collection by Hilton | `approved_city_label_normalization` | Owner-facing and staging patch display must use San Pedro Garza García. Do not change the underlying Hilton source URL. Do not claim a broader location than the source supports. |

### El Cid (Castilla + La Ceiba)
- Soft-brand **distribution examples** under Ascend Hotel Collection only
- Do **not** imply Choice ownership, Faranda, or direct management
- Do **not** use as Recent Momentum without separate dated evidence

### Amberes 64
- **Property proof / property example only**
- Existing dated press may remain the momentum source if already in Ascend fixtures
- VIC / official page existence alone does **not** become Recent Momentum

### MS Milenium Monterrey
- Display city normalized to **San Pedro Garza García** (Nuevo León · Monterrey metro)
- Source URL unchanged
- No broader location claim than source supports

---

## 3. Owner-facing copy review

| Check | Result |
|-------|--------|
| Forbidden internal/source terms (original) | PASS (0) |
| Soft framing revisions needed | 2 |
| Forbidden terms (revised) | PASS (0) |
| Copy pack ready | **YES** |

### Soft issues found
- `ascend.owner_fit_note`: phrase_under_Ascend_could_imply_ownership_or_management
- `curio-collection.property_examples`: milenium_city_should_use_normalized_or_metro_label

### Revised safe copy (approved for staging apply test)

#### Hotel Indigo
**A.** Mexico examples include Hotel Indigo Guadalajara Expo, Hotel Indigo Playa del Carmen, and Hotel Indigo Guanajuato — each reflecting the brand’s neighborhood-led positioning across distinct Mexican markets.  
**B.** Hotel Indigo is present in Mexico across gateway and leisure markets, including Guadalajara, Playa del Carmen, and Guanajuato.  
**C.** These Mexico properties illustrate how Hotel Indigo can express local character in both urban and leisure settings.  
**D.** Owners evaluating Mexico can use these properties as reference points for urban expo-adjacent, coastal lifestyle, and colonial-city Indigo formats.

#### Ascend Hotel Collection
**A.** Mexico examples include Amberes 64 in Mexico City, El Cid Castilla Beach Hotel in Mazatlán, and El Cid La Ceiba Beach Hotel in Cozumel — illustrating Ascend Hotel Collection’s soft-brand distribution across urban boutique and beach resort formats.  
**B.** Ascend Hotel Collection is present in Mexico across Mexico City, Mazatlán, and Cozumel.  
**C.** The Mexico examples show how independent and soft-brand hotels can participate in Ascend Hotel Collection across capital and coastal leisure markets.  
**D.** Useful for owners comparing an urban Mexico City boutique example with established beach-resort soft-brand distribution formats in Ascend Hotel Collection — without assuming Choice ownership or direct management.

#### Curio Collection by Hilton
**A.** Mexico examples include Amare Cancun, The Fives Downtown in Playa del Carmen, and MS Milenium in San Pedro Garza García — covering all-inclusive resort, lifestyle downtown, and Monterrey-metro urban Curio expressions.  
**B.** Curio Collection by Hilton is present in Mexico across Cancún, Playa del Carmen, and the Monterrey metro (San Pedro Garza García).  
**C.** These properties show Curio’s flexibility across leisure all-inclusive and urban/lifestyle Mexico markets.  
**D.** Owners can compare a Cancún all-inclusive Curio against Playa del Carmen lifestyle and Monterrey-metro urban formats when underwriting Mexico.

#### Holiday Inn Express
**A.** A Mexico example is Holiday Inn Express & Suites Querétaro — a practical midscale select-service reference in a major inland commercial market.  
**B.** Holiday Inn Express is present in Mexico, including Querétaro.  
**C.** This property supports Mexico midscale select-service context for owners reviewing Holiday Inn Express in secondary commercial cities.  
**D.** Best used as a single-market Mexico reference for select-service midscale positioning — not as a full Mexico portfolio map.

---

## 4. Remaining holds

_None_

Post-steward risk: `safe_for_staging_overlay`

---

## 5. Staging-only apply recommendation

**May proceed:** **YES**  
**Next:** `mexico_vic_be_small_pilot_staging_only_apply_test`

Allowed later staging patch targets only:
- property examples
- Mexico / CALA footprint lines
- portfolio context (internal counts; owner-facing copy uses revised pack)
- property proof (as-of discovery)

Still forbidden:
- Airtable production writes
- BE activation / Active-Live record mutation in this step
- Recent Momentum from directory existence
- rooms / owners / operators / open dates / affiliation start dates / Company Validated

---

## Acceptance

- [x] Ascend alias approved
- [x] Curio alias approved
- [x] El Cid soft-brand framing documented
- [x] Amberes classified property proof only (no VIC momentum)
- [x] MS Milenium city normalized to San Pedro Garza García
- [x] Owner-facing copy passes internal-language review
- [x] Freeze unmodified · no Airtable · no BE writes · no production overwrite · no Webhound
- [x] Status: `mexico_vic_be_small_pilot_minor_steward_review_clean_ready_for_staging_apply_test`
