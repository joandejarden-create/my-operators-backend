# Mexico VIC → Brand Explorer Small Completion Pilot Overlay

**Status:** `mexico_vic_be_small_pilot_overlay_ready`  
**Generated:** 2026-08-04T23:24:54.001Z  
**Baseline:** `mexico_vic_4family_baseline_locked_staging_ready`  
**Freeze hash:** `c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3`

Read-only overlay. No Airtable · No Webhound · No BE activation · No BE record writes · No production overwrite · Frozen VIC baseline unmodified.

---

## 1. Executive summary

Overlay maps the **10-property / 4-brand** Mexico VIC small pilot to Active/Live Brand Explorer profiles as **staging completion evidence** for:

- property examples
- Mexico / CALA geographic grounding
- portfolio context (internal staging counts)
- property proof (current affiliation as-of discovery)

**Does not** create Recent Momentum, rooms, owners/operators, open dates, affiliation start dates, or Company Validated claims.

**Overall risk:** `safe_after_minor_steward_review`  
**Next:** minor_steward_review_then_staging_only_apply_test

---

## 2. Source authority and freeze hash

| Item | Value |
|------|-------|
| VIC baseline | `mexico_vic_4family_baseline_locked_staging_ready` |
| Freeze hash | `c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3` |
| Pilot candidates | `mexico_vic_be_completion_pilot_candidates_ready` |
| BE universe reference | Protected 54 Active/Live public-full (read-only snapshot) |

---

## 3. Pilot property list

| # | Family | Brand | Property | City |
|--:|--------|-------|----------|------|
| 1 | IHG | Hotel Indigo | Hotel Indigo Guadalajara Expo | Guadalajara |
| 2 | IHG | Hotel Indigo | Hotel Indigo Playa del Carmen | Playa Del Carmen |
| 3 | IHG | Hotel Indigo | Hotel Indigo Guanajuato | Guanajuato |
| 4 | Choice | Ascend Hotel Collection | Amberes 64, an Ascend Collection Hotel | Mexico City |
| 5 | Choice | Ascend Hotel Collection | El Cid Castilla Beach Hotel | Mazatlan |
| 6 | Choice | Ascend Hotel Collection | El Cid La Ceiba Beach Hotel | Cozumel |
| 7 | Hilton | Curio Collection by Hilton | Amare Cancun Adults Only All-Inclusive Resort, Curio by Hilton | Cancun |
| 8 | Hilton | Curio Collection by Hilton | The Fives Downtown Hotel & Residences, Curio Collection by Hilton | Playa del Carmen |
| 9 | Hilton | Curio Collection by Hilton | MS Milenium Monterrey, Curio Collection by Hilton | SanPedro Garza Garcia |
| 10 | IHG | Holiday Inn Express | Holiday Inn Express & Suites Queretaro | Queretaro |

---

## 4. Target BE brand mapping

| Requested slug | Active/Live BE54 slug | Mismatch? | Steward mapping |
|----------------|-----------------------|-----------|-----------------|
| `hotel-indigo` | `hotel-indigo` | no | Exact match |
| `ascend-hotel-collection` | `ascend` | YES | Use Active/Live BE54 slug `ascend` (brand name Ascend Hotel Collection). Do not invent `ascend-hotel-collection`. |
| `curio-collection-by-hilton` | `curio-collection` | YES | Use Active/Live BE54 slug `curio-collection` (brand name Curio Collection by Hilton). Do not invent `curio-collection-by-hilton`. |
| `holiday-inn-express` | `holiday-inn-express` | no | Exact match |

**Confirmed live brands:** Hotel Indigo (`hotel-indigo`), Ascend Hotel Collection (`ascend`), Curio Collection by Hilton (`curio-collection`), Holiday Inn Express (`holiday-inn-express`).

---

## 5. Per-brand overlay recommendations

| Brand | Current BE State | VIC Candidates | Proposed improvement | Risk | Recommendation |
|-------|------------------|----------------|----------------------|------|----------------|
| Hotel Indigo (`hotel-indigo`) | Active · International Reference · CV=false | Hotel Indigo Guadalajara Expo; Hotel Indigo Playa del Carmen; Hotel Indigo Guanajuato | Property examples + Mexico footprint + portfolio context; **no** Recent Momentum from VIC | `safe_for_staging_overlay` | proceed_to_staging_only_apply_test |
| Ascend Hotel Collection (`ascend`) | Active · CALA-specific · CV=false | Amberes 64, an Ascend Collection Hotel; El Cid Castilla Beach Hotel; El Cid La Ceiba Beach Hotel | Property examples + Mexico footprint + portfolio context; **no** Recent Momentum from VIC | `safe_after_minor_steward_review` | proceed_after_minor_steward_review |
| Curio Collection by Hilton (`curio-collection`) | Active · International Reference · CV=false | Amare Cancun Adults Only All-Inclusive Resort, Curio by Hilton; The Fives Downtown Hotel & Residences, Curio Collection by Hilton; MS Milenium Monterrey, Curio Collection by Hilton | Property examples + Mexico footprint + portfolio context; **no** Recent Momentum from VIC | `safe_after_minor_steward_review` | proceed_after_minor_steward_review |
| Holiday Inn Express (`holiday-inn-express`) | Active · CALA-informed · CV=false | Holiday Inn Express & Suites Queretaro | Property examples + Mexico footprint + portfolio context; **no** Recent Momentum from VIC | `safe_for_staging_overlay` | proceed_to_staging_only_apply_test |

### Per-brand answers

#### Hotel Indigo

1. Mexico/CALA grounding: **Yes**
2. Property examples: **Yes**
3. Portfolio/geographic evidence: **Yes**
4. Conflict with existing BE: **none_blocking**
5. Owner-facing useful: **Yes**
6. Safe for future controlled Airtable patch: **Yes (staging-controlled)**
7. Move to staging-only apply test: **Yes** (`proceed_to_staging_only_apply_test`)

#### Ascend Hotel Collection

1. Mexico/CALA grounding: **Yes**
2. Property examples: **Yes**
3. Portfolio/geographic evidence: **Yes**
4. Conflict with existing BE: **partial_note_only** — Amberes already appears in Ascend fixture momentum via dated Choice press — overlay adds property-example/footprint support; do not duplicate as new Recent Momentum from VIC
5. Owner-facing useful: **Yes**
6. Safe for future controlled Airtable patch: **Yes (staging-controlled)**
7. Move to staging-only apply test: **Yes** (`proceed_after_minor_steward_review`)

#### Curio Collection by Hilton

1. Mexico/CALA grounding: **Yes**
2. Property examples: **Yes**
3. Portfolio/geographic evidence: **Yes**
4. Conflict with existing BE: **partial_note_only** — Curio fixtures emphasize DR/Argentina CALA examples; Mexico Curio set is additive for Mexico grounding — no identity conflict detected
5. Owner-facing useful: **Yes**
6. Safe for future controlled Airtable patch: **Yes (staging-controlled)**
7. Move to staging-only apply test: **Yes** (`proceed_after_minor_steward_review`)

#### Holiday Inn Express

1. Mexico/CALA grounding: **Yes**
2. Property examples: **Yes**
3. Portfolio/geographic evidence: **Yes**
4. Conflict with existing BE: **none_blocking**
5. Owner-facing useful: **Yes**
6. Safe for future controlled Airtable patch: **Yes (staging-controlled)**
7. Move to staging-only apply test: **Yes** (`proceed_to_staging_only_apply_test`)


---

## 6. Per-property overlay table

| # | Family | BE slug | VIC brand | Property | City | State | Country | Official URL | Source type | Eligible | Identity | As-of | Proposed BE use | Safe fields | Unsafe | Risk | Steward note |
|--:|--------|---------|-----------|----------|------|-------|---------|--------------|-------------|----------|----------|-------|-----------------|-------------|--------|------|--------------|
| 1 | IHG | `hotel-indigo` | Hotel Indigo | Hotel Indigo Guadalajara Expo | Guadalajara | Jalisco | Mexico | https://www.ihg.com/hotelindigo/hotels/us/en/guadalajara/gdlal/hoteldetail | Official Parent Company Directory / property page | ELIGIBLE | High | 2026-08-04T22:21:26.945Z | property_example, geographic_footprint, portfolio_context, property_proof | identity/geo/URL | rooms/owner/open date/momentum | `safe_for_staging_overlay` | None |
| 2 | IHG | `hotel-indigo` | Hotel Indigo | Hotel Indigo Playa del Carmen | Playa Del Carmen | Quintana Roo | Mexico | https://www.ihg.com/hotelindigo/hotels/us/en/playa-del-carmen/pcmaa/hoteldetail | Official Parent Company Directory / property page | ELIGIBLE | High | 2026-08-04T22:21:26.945Z | property_example, geographic_footprint, portfolio_context, property_proof | identity/geo/URL | rooms/owner/open date/momentum | `safe_for_staging_overlay` | None |
| 3 | IHG | `hotel-indigo` | Hotel Indigo | Hotel Indigo Guanajuato | Guanajuato | Guanajuato | Mexico | https://www.ihg.com/hotelindigo/hotels/us/en/guanajuato/bjxgd/hoteldetail | Official Parent Company Directory / property page | ELIGIBLE | High | 2026-08-04T22:21:26.945Z | property_example, geographic_footprint, portfolio_context, property_proof | identity/geo/URL | rooms/owner/open date/momentum | `safe_for_staging_overlay` | None |
| 4 | Choice | `ascend` | Ascend Hotel Collection | Amberes 64, an Ascend Collection Hotel | Mexico City | CMX | Mexico | https://www.choicehotels.com/mexico/mexico-city/ascend-hotels/mx228 | Official Choice Mexico regional directory | ELIGIBLE | Exact | 2026-08-04T22:51:52.666Z | property_example, geographic_footprint, portfolio_context, property_proof | identity/geo/URL | rooms/owner/open date/momentum | `safe_for_staging_overlay` | None |
| 5 | Choice | `ascend` | Ascend Hotel Collection | El Cid Castilla Beach Hotel | Mazatlan | SIN | Mexico | https://www.choicehotels.com/sinaloa/mazatlan/ascend-hotels/mx164 | Official Choice Mexico regional directory | ELIGIBLE | Exact | 2026-08-04T22:51:52.666Z | property_example, geographic_footprint, portfolio_context, property_proof | identity/geo/URL | rooms/owner/open date/momentum | `safe_after_minor_steward_review` | El Cid soft-brand Ascend properties — confirm owner-facing soft-brand framing before patch; keep as Ascend Collection examples |
| 6 | Choice | `ascend` | Ascend Hotel Collection | El Cid La Ceiba Beach Hotel | Cozumel | ROO | Mexico | https://www.choicehotels.com/quintana-roo/cozumel/ascend-hotels/mx169 | Official Choice Mexico regional directory | ELIGIBLE | Exact | 2026-08-04T22:51:52.666Z | property_example, geographic_footprint, portfolio_context, property_proof | identity/geo/URL | rooms/owner/open date/momentum | `safe_after_minor_steward_review` | El Cid soft-brand Ascend properties — confirm owner-facing soft-brand framing before patch; keep as Ascend Collection examples |
| 7 | Hilton | `curio-collection` | Curio Collection by Hilton | Amare Cancun Adults Only All-Inclusive Resort, Curio by Hilton | Cancun | ROO | Mexico | https://www.hilton.com/en/hotels/cunaaqq-amare-cancun-adults-only-all-inclusive-resort/ | Official Hilton Mexico brand location page | ELIGIBLE | High | 2026-08-04T22:33:32.848Z | property_example, geographic_footprint, portfolio_context, property_proof | identity/geo/URL | rooms/owner/open date/momentum | `safe_for_staging_overlay` | All-inclusive Curio format — useful Mexico resort example; do not invent room counts |
| 8 | Hilton | `curio-collection` | Curio Collection by Hilton | The Fives Downtown Hotel & Residences, Curio Collection by Hilton | Playa del Carmen | ROO | Mexico | https://www.hilton.com/en/hotels/cuncuqq-the-fives-downtown-hotel-and-residences/ | Official Hilton Mexico brand location page | ELIGIBLE | High | 2026-08-04T22:33:32.848Z | property_example, geographic_footprint, portfolio_context, property_proof | identity/geo/URL | rooms/owner/open date/momentum | `safe_for_staging_overlay` | None |
| 9 | Hilton | `curio-collection` | Curio Collection by Hilton | MS Milenium Monterrey, Curio Collection by Hilton | SanPedro Garza Garcia | NLE | Mexico | https://www.hilton.com/en/hotels/mtymmqq-ms-milenium-monterrey/ | Official Hilton Mexico brand location page | ELIGIBLE | High | 2026-08-04T22:33:32.848Z | property_example, geographic_footprint, portfolio_context, property_proof | identity/geo/URL | rooms/owner/open date/momentum | `safe_after_minor_steward_review` | Normalize city label to San Pedro Garza García for owner-facing copy |
| 10 | IHG | `holiday-inn-express` | Holiday Inn Express | Holiday Inn Express & Suites Queretaro | Queretaro | Querétaro | Mexico | https://www.ihg.com/holidayinnexpress/hotels/us/en/queretaro/mqerg/hoteldetail | Official Parent Company Directory / property page | ELIGIBLE | High | 2026-08-04T22:21:26.945Z | property_example, geographic_footprint, portfolio_context, property_proof | identity/geo/URL | rooms/owner/open date/momentum | `safe_for_staging_overlay` | None |

---

## 7. Owner-facing copy test

### Hotel Indigo (`hotel-indigo`)

**A. Property examples**  
Mexico examples include Hotel Indigo Guadalajara Expo, Hotel Indigo Playa del Carmen, and Hotel Indigo Guanajuato — each reflecting the brand’s neighborhood-led positioning across distinct Mexican markets.

**B. Geographic footprint**  
Hotel Indigo is present in Mexico across gateway and leisure markets, including Guadalajara, Playa del Carmen, and Guanajuato.

**C. Portfolio context**  
These Mexico properties illustrate how Hotel Indigo can express local character in both urban and leisure settings.

**D. Owner-fit note**  
Owners evaluating Mexico can use these properties as reference points for urban expo-adjacent, coastal lifestyle, and colonial-city Indigo formats.

### Ascend Hotel Collection (`ascend`)

**A. Property examples**  
Mexico examples include Amberes 64 in Mexico City, El Cid Castilla Beach Hotel in Mazatlán, and El Cid La Ceiba Beach Hotel in Cozumel — showcasing Ascend’s range from urban boutique to beach resort soft brands.

**B. Geographic footprint**  
Ascend Hotel Collection is present in Mexico across Mexico City, Mazatlán, and Cozumel.

**C. Portfolio context**  
The Mexico set shows how Ascend can carry independent and soft-brand stories in capital and coastal leisure markets.

**D. Owner-fit note**  
Useful for owners comparing an urban Mexico City conversion play with established beach-resort soft-brand formats under Ascend.

### Curio Collection by Hilton (`curio-collection`)

**A. Property examples**  
Mexico examples include Amare Cancun, The Fives Downtown in Playa del Carmen, and MS Milenium Monterrey — covering all-inclusive resort, lifestyle downtown, and northern business-market Curio expressions.

**B. Geographic footprint**  
Curio Collection by Hilton is present in Mexico across Cancún, Playa del Carmen, and the Monterrey metro.

**C. Portfolio context**  
These properties show Curio’s flexibility across leisure all-inclusive and urban/lifestyle Mexico markets.

**D. Owner-fit note**  
Owners can compare a Cancún all-inclusive Curio against Playa del Carmen lifestyle and Monterrey urban formats when underwriting Mexico.

### Holiday Inn Express (`holiday-inn-express`)

**A. Property examples**  
A Mexico example is Holiday Inn Express & Suites Querétaro — a practical midscale select-service reference in a major inland commercial market.

**B. Geographic footprint**  
Holiday Inn Express is present in Mexico, including Querétaro.

**C. Portfolio context**  
This property supports Mexico midscale select-service context for owners reviewing Holiday Inn Express in secondary commercial cities.

**D. Owner-fit note**  
Best used as a single-market Mexico reference for select-service midscale positioning — not as a full Mexico portfolio map.


Copy rules honored: no VIC / census / source pack / staging / directory / raw URLs / Company Validated / Brand Verified / false Recent Momentum / unsourced rooms-owners-operators.

---

## 8. Recent Momentum exclusion review

| Check | Result |
|-------|--------|
| VIC directory existence mapped to Recent Momentum | **No** |
| Properties checked | 10 |
| All classified as property proof, not momentum | **Yes** |

Amberes may already have dated Choice press in Ascend fixtures — VIC still contributes **property example / footprint**, not a new momentum event.

---

## 9. Risk scoring

**Overall:** `safe_after_minor_steward_review`

| # | Property | Classification |
|--:|----------|----------------|
| 1 | Hotel Indigo Guadalajara Expo | `safe_for_staging_overlay` |
| 2 | Hotel Indigo Playa del Carmen | `safe_for_staging_overlay` |
| 3 | Hotel Indigo Guanajuato | `safe_for_staging_overlay` |
| 4 | Amberes 64, an Ascend Collection Hotel | `safe_for_staging_overlay` |
| 5 | El Cid Castilla Beach Hotel | `safe_after_minor_steward_review` |
| 6 | El Cid La Ceiba Beach Hotel | `safe_after_minor_steward_review` |
| 7 | Amare Cancun Adults Only All-Inclusive Resort, Curio by Hilton | `safe_for_staging_overlay` |
| 8 | The Fives Downtown Hotel & Residences, Curio Collection by Hilton | `safe_for_staging_overlay` |
| 9 | MS Milenium Monterrey, Curio Collection by Hilton | `safe_after_minor_steward_review` |
| 10 | Holiday Inn Express & Suites Queretaro | `safe_for_staging_overlay` |

Dimensions scored: brand identity · property identity · temporal affiliation · source strength · owner-facing usefulness · production migration risk.

---

## 10. Staging apply recommendation

- **Now:** no Airtable / no BE writes  
- **Next:** `minor_steward_review_then_staging_only_apply_test`  
- After minor steward notes (El Cid soft-brand framing; Monterrey city label; slug aliases)

---

## 11. Fields safe to patch later

- brand
- property name
- city
- state / region (when present)
- country
- official property URL (internal lineage only — not owner-facing raw URLs)
- Mexico / CALA presence (supported by listed properties only)
- property example candidate
- property proof of current affiliation (as-of discovery)
- portfolio context as staging evidence count (internal)

---

## 12. Fields unsafe to patch

- rooms (Unknown in this pilot set)
- owner
- operator / management company
- open date / opening history
- affiliation start date
- Recent Momentum from directory existence alone
- Company Validated
- Brand Verified
- production Hotel Census overwrite fields
- legacy Dealality-only evidence

---

## 13. Recommended next step

1. Accept slug steward mappings (`ascend`, `curio-collection`).
2. Minor steward review on El Cid Ascend examples + Monterrey city label.
3. Run a **staging-only apply test** (separate task) that patches property examples / Mexico footprint only.
4. Do **not** activate Brand Explorer, write production, or invent Recent Momentum.

---

## Acceptance

- [x] All 10 properties mapped to correct Active/Live BE slug (or steward mapping for requested aliases)
- [x] All four brands reviewed
- [x] Trace to freeze `c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3`
- [x] Freeze unmodified · no Airtable · no BE writes · no production overwrite · no Webhound
- [x] Recent Momentum not created from directory existence
- [x] Safe vs unsafe fields separated
- [x] Owner-facing copy clean of internal language
- [x] Overlay classifications assigned
- [x] Status: `mexico_vic_be_small_pilot_overlay_ready`
