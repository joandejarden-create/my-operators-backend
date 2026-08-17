# Brand Explorer 62 — Background Validation Plan

**Status:** `brand_explorer_62_background_validation_patch_plan_ready`
**Generated:** 2026-08-05T14:17:52.275Z
**Mode:** dry-run / patch proposals only (no Census writes, no production BE patches)

## 1. Executive summary

- Active universe: **62** · public-full: **62** · Four Points Flex held: **true**
- Census: **666** records / **101** fields · held: **4**
- Brand↔Census mapped (exact/alias/soft): **32** · no census: **30**
- Patch proposals: **175** (actionable **139**) · founder decisions: **106**
- Semantic C/H/M: {"critical":0,"high":0,"medium":0,"low":0} · PVQL: true (62) · momentum: true · gates: true · footnote: true
- Quality quiet: `freeze_after_minor_cleanup_pass` (61 approve · 1 minor: `mgallery-collection`)

## 2. Active 62 validation

- OK: **true**
- Freeze: `frozen_62_active_public_full_baseline_semantic_clean_flex_held`
- Flex in active: **false** (must be false)

## 3. New Census field contract

- Contract OK: **true** · fields 101/101 · records 666/666
- Held (Human Review Required): 4/4
- Duplicate identity keys: 0
- Missing required fields: none
- Radar fields present: **true**

## 4. Brand-to-Census mapping

| Mapping class | Count |
| --- | ---: |
| `exact_brand_match` | 22 |
| `soft_brand_collection_match` | 7 |
| `no_census_records_found` | 30 |
| `alias_brand_match` | 3 |

## 5. Property example validation

| Classification | Count |
| --- | ---: |
| `confirmed_in_census` | 31 |
| `missing_from_census` | 180 |
| `confirmed_but_needs_text_update` | 7 |
| `census_has_better_example` | 1 |

**Note:** `missing_from_census` is expected for most International Reference / non-Mexico openings. Hotel Property Census is Mexico-scoped (666). Missing is **not** automatic removal unless the BE example claims Mexico/CALA proof without Census support.

## 6. Hotel count / footprint validation

Mexico Census is used as property proof / regional crosscheck only — not global portfolio SoT. Blank enrichment fields are not negative evidence.

| Claim class | Brands |
| --- | ---: |
| `no_action` | 14 |
| `Census_may_be_incomplete` | 48 |

## 7–8. Owner-facing text & forbidden language

- Brands with forbidden hits: **33**
- PVQL hits: 1 · Extra process-term hits: 62
- Brands: aloft-hotels, ascend, comfort-inn-suites, country-inn-suites, design-hotels, doubletree-by-hilton, everhome-suites, fairmont, hampton-by-hilton, hilton-garden-inn, hilton-hotels-and-resorts, home2-suites-by-hilton, homewood-suites-by-hilton, ibis, mama-shelter, marriott-hotels, mercure, novotel, pullman, quality-inn, radisson-individuals-by-choice, radisson-red, residence-inn-by-marriott, sheraton, spark-by-hilton, springhill-suites-by-marriott, studiores, suburban-studios, tapestry-collection-by-hilton, towneplace-suites-by-marriott, trademark-collection-by-wyndham, tru-by-hilton, westin

## 9. Webflow / product field readiness

See `brand-explorer-62-webflow-field-review.md` for per-brand field matrix.

## 10. Recent Momentum review

| Classification | Count |
| --- | ---: |
| `remove` | 36 |
| `founder decision needed` | 18 |
| `keep` | 161 |
| `needs source` | 51 |

## 11. BE vs Census mismatches

Count: **187** (see census crosscheck JSON for full list).

## 12. Census correction flags

- `be_property_missing_from_census` · bunkhouse-hotels · Hotel San Cristóbal Bunkhouse Hotels — Todos Santos — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · bunkhouse-hotels · Hotel San Fernando Bunkhouse Hotels — Mexico City — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · curio-collection · Anselmo Buenos Aires Curio Collection by Hilton — Buenos Aires — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · design-hotels · Condesa DF — U.S. Property Example — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · fairmont · Fairmont Mayakoba — CALA — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · handwritten-collection · Marival Distinct Luxury Residences Handwritten Collection — Nuevo Vallarta — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · ibis · ibis Mexico Alameda — CALA — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · ibis · ibis Mexico Alameda — Mexico City — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · kimpton · Kimpton Virgilio Kimpton Hotels — Mexico City — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · mama-shelter · Mama Shelter Mexico City — CALA — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · novotel · Novotel Mexico City World Trade Center — Mexico City — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · novotel · Novotel Mexico City Centro Histórico — CALA — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · novotel · Novotel Mexico City World Trade Center — CALA — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · preferred-hotels-and-resorts · NIZUC Resort & Spa Preferred Hotels & Resorts — Cancun — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · preferred-hotels-and-resorts · NIZUC Resort & Spa Preferred Hotels & Resorts — Cancun — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · preferred-hotels-and-resorts · NIZUC Resort & Spa Preferred Hotels & Resorts — Cancun — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · quality-inn · Quality Inn — Resville, Mexico — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · sheraton · Sheraton Buganvilias Resort & Convention Center — CALA — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · sheraton · Sheraton Buganvilias Resort & Convention Center — CALA — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.
- `be_property_missing_from_census` · sheraton · Sheraton Cancun Resort & Spa — CALA — BE example not found in Mexico Hotel Property Census — may be outside Mexico scope or Census incomplete. Do not overwrite Census.

## 13. Brand Explorer patch proposal table

| Brand | Field | Category | Risk | Founder OK? | Reason |
| --- | --- | --- | --- | --- | --- |
| aloft-hotels | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| ascend | materials.caseStudy | `safe_text_cleanup` | high | true | forbidden_language:consumer_site |
| comfort-inn-suites | loyalty.proof | `safe_text_cleanup` | high | true | forbidden_language:consumer_site |
| comfort-inn-suites | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:census_url |
| comfort-inn-suites | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:listed_on_choice |
| comfort-inn-suites | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:census |
| comfort-inn-suites | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:census_url |
| comfort-inn-suites | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:listed_on_choice |
| comfort-inn-suites | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:census |
| country-inn-suites | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:item_19 |
| country-inn-suites | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:listed_on_choice |
| country-inn-suites | footprint.geo_intro | `safe_text_cleanup` | high | true | forbidden_language:census |
| country-inn-suites | materials.caseStudy | `safe_text_cleanup` | high | true | forbidden_language:listed_on_choice |
| country-inn-suites | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:chd |
| country-inn-suites | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:listed_on_choice |
| country-inn-suites | footprint.editorial_bullets | `safe_text_cleanup` | high | true | forbidden_language:census |
| country-inn-suites | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:listed_on_choice |
| country-inn-suites | footprint.region.cala | `safe_text_cleanup` | high | true | forbidden_language:census |
| country-inn-suites | footprint.region.apac | `safe_text_cleanup` | high | true | forbidden_language:census |
| country-inn-suites | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:listed_on_choice |
| country-inn-suites | loyalty.proof | `safe_text_cleanup` | high | true | forbidden_language:consumer_site |
| country-inn-suites | footprint.editorial_bullets | `safe_text_cleanup` | high | true | forbidden_language:census |
| courtyard-by-marriott | footprint.openings | `property_example_update` | low | true | census_match_ok_location_text_may_need_refresh |
| curio-collection | footprint.openings | `property_example_update` | low | true | census_match_ok_location_text_may_need_refresh |
| design-hotels | footprint.editorial | `safe_text_cleanup` | high | true | forbidden_language:census |
| design-hotels | footprint.editorial_bullets | `safe_text_cleanup` | high | true | forbidden_language:census |
| doubletree-by-hilton | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| everhome-suites | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:chd |
| everhome-suites | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:listed_on_choice |
| everhome-suites | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:chd |
| everhome-suites | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| everhome-suites | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:listed_on_choice |
| fairmont | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| hampton-by-hilton | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| hilton-garden-inn | footprint.openings | `property_example_update` | low | true | census_match_ok_location_text_may_need_refresh |
| hilton-garden-inn | footprint.openings | `property_example_update` | low | true | census_match_ok_location_text_may_need_refresh |
| hilton-garden-inn | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| hilton-hotels-and-resorts | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| holiday-inn-express | footprint.openings | `property_example_update` | low | true | census_match_ok_location_text_may_need_refresh |
| home2-suites-by-hilton | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| homewood-suites-by-hilton | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| ibis | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| kimpton | footprint.openings | `property_example_update` | low | true | census_match_ok_location_text_may_need_refresh |
| mama-shelter | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| marriott-hotels | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| mercure | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| motto-by-hilton | footprint.openings | `property_example_update` | low | true | census_match_ok_location_text_may_need_refresh |
| novotel | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| pullman | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| quality-inn | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:census_url |
| quality-inn | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:listed_on_choice |
| quality-inn | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:census |
| quality-inn | materials.caseStudy | `safe_text_cleanup` | high | true | forbidden_language:consumer_site |
| quality-inn | loyalty.proof | `safe_text_cleanup` | high | true | forbidden_language:consumer_site |
| quality-inn | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:listed_on_choice |
| radisson-individuals-by-choice | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:listed_on_choice |
| radisson-red | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:census |
| radisson-red | loyalty.proof | `safe_text_cleanup` | high | true | forbidden_language:consumer_site |
| radisson-red | footprint.openings | `safe_text_cleanup` | high | true | forbidden_language:listed_on_choice |
| residence-inn-by-marriott | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| sheraton | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| spark-by-hilton | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| springhill-suites-by-marriott | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| studiores | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| suburban-studios | loyalty.proof | `safe_text_cleanup` | high | true | forbidden_language:consumer_site |
| towneplace-suites-by-marriott | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| tru-by-hilton | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| westin | footprint.portfolio_mix | `safe_text_cleanup` | high | true | forbidden_language:census |
| so | footprint.openings | `property_example_update` | low | true | BE openings thin; Census has eligible unused property |
| ac-hotels-by-marriott | footprint.momentum | `do_not_patch` | high | true | property_existence_not_momentum |
| ac-hotels-by-marriott | footprint.momentum | `founder_decision_needed` | high | true | review_momentum_vs_property_example_boundary |
| ac-hotels-by-marriott | footprint.momentum | `do_not_patch` | high | true | property_existence_not_momentum |
| aloft-hotels | footprint.momentum | `founder_decision_needed` | high | true | review_momentum_vs_property_example_boundary |
| autograph-collection | footprint.momentum | `founder_decision_needed` | high | true | missing_date |
| autograph-collection | footprint.momentum | `founder_decision_needed` | high | true | missing_date |
| autograph-collection | footprint.momentum | `founder_decision_needed` | high | true | missing_date |
| autograph-collection | footprint.momentum | `founder_decision_needed` | high | true | missing_date |
| autograph-collection | footprint.momentum | `founder_decision_needed` | high | true | missing_date |
| autograph-collection | footprint.momentum | `founder_decision_needed` | high | true | missing_date |
| avid-hotels | footprint.momentum | `do_not_patch` | high | true | property_existence_not_momentum |
| … | … | … | … | … | +95 more in JSON |

## 14. Founder decisions needed

- **ac-hotels-by-marriott** · recent_momentum · remove — property_existence_not_momentum
- **ac-hotels-by-marriott** · recent_momentum · founder decision needed — review_momentum_vs_property_example_boundary
- **ac-hotels-by-marriott** · recent_momentum · remove — property_existence_not_momentum
- **aloft-hotels** · recent_momentum · founder decision needed — review_momentum_vs_property_example_boundary
- **autograph-collection** · recent_momentum · needs source — missing_date
- **autograph-collection** · recent_momentum · needs source — missing_date
- **autograph-collection** · recent_momentum · needs source — missing_date
- **autograph-collection** · recent_momentum · needs source — missing_date
- **autograph-collection** · recent_momentum · needs source — missing_date
- **autograph-collection** · recent_momentum · needs source — missing_date
- **avid-hotels** · recent_momentum · remove — property_existence_not_momentum
- **bunkhouse-hotels** · recent_momentum · remove — property_existence_not_momentum
- **bunkhouse-hotels** · recent_momentum · remove — property_existence_not_momentum
- **canopy-by-hilton** · recent_momentum · founder decision needed — review_momentum_vs_property_example_boundary
- **canopy-by-hilton** · recent_momentum · remove — property_existence_not_momentum
- **canopy-by-hilton** · recent_momentum · remove — property_existence_not_momentum
- **city-express-by-marriott** · recent_momentum · founder decision needed — review_momentum_vs_property_example_boundary
- **city-express-by-marriott** · recent_momentum · remove — property_existence_not_momentum
- **city-express-by-marriott** · recent_momentum · remove — property_existence_not_momentum
- **comfort-inn-suites** · recent_momentum · needs source — missing_announcement_url
- **country-inn-suites** · recent_momentum · remove — empty_title_or_body
- **country-inn-suites** · recent_momentum · remove — empty_title_or_body
- **country-inn-suites** · recent_momentum · needs source — missing_announcement_url
- **country-inn-suites** · recent_momentum · remove — empty_title_or_body
- **courtyard-by-marriott** · recent_momentum · founder decision needed — review_momentum_vs_property_example_boundary
- **dazzler-by-wyndham** · recent_momentum · remove — property_existence_not_momentum
- **dazzler-by-wyndham** · recent_momentum · needs source — missing_date
- **dazzler-by-wyndham** · recent_momentum · needs source — missing_date
- **dazzler-by-wyndham** · recent_momentum · remove — property_existence_not_momentum
- **dazzler-by-wyndham** · recent_momentum · remove — property_existence_not_momentum
- **dazzler-by-wyndham** · recent_momentum · remove — property_existence_not_momentum
- **dazzler-by-wyndham** · recent_momentum · needs source — missing_date
- **even-hotels** · recent_momentum · remove — property_existence_not_momentum
- **even-hotels** · recent_momentum · remove — property_existence_not_momentum
- **fairmont** · recent_momentum · needs source — missing_announcement_url
- **fairmont** · recent_momentum · needs source — missing_announcement_url
- **fairmont** · recent_momentum · founder decision needed — review_momentum_vs_property_example_boundary
- **handwritten-collection** · recent_momentum · needs source — missing_date
- **handwritten-collection** · recent_momentum · needs source — missing_date
- **handwritten-collection** · recent_momentum · needs source — missing_date

## 15. Brand Explorer safety result

- noCensusWrites: **true**
- noProductionBePatchesApplied: **true**
- noCompanyValidatedWrites: **true**
- noBrandVerifiedWrites: **true**
- noBrandStatusWrites: **true**
- noRecentMomentumApplied: **true**
- mexicoCensusNotUsedAsGlobalPortfolioSoT: **true**
- blankCensusFieldsNotNegativeEvidence: **true**

## 16. Recommended first patch batch

| Brand | Field | Category | Proposed |
| --- | --- | --- | --- |
| aloft-hotels | footprint.portfolio_mix | `safe_text_cleanup` | Rewrite without internal/process/forbidden language |
| ascend | materials.caseStudy | `safe_text_cleanup` | Rewrite without internal/process/forbidden language |
| comfort-inn-suites | loyalty.proof | `safe_text_cleanup` | Rewrite without internal/process/forbidden language |
| comfort-inn-suites | footprint.openings | `safe_text_cleanup` | Rewrite without internal/process/forbidden language |
| comfort-inn-suites | footprint.openings | `safe_text_cleanup` | Rewrite without internal/process/forbidden language |
| comfort-inn-suites | footprint.openings | `safe_text_cleanup` | Rewrite without internal/process/forbidden language |
| comfort-inn-suites | footprint.openings | `safe_text_cleanup` | Rewrite without internal/process/forbidden language |
| comfort-inn-suites | footprint.openings | `safe_text_cleanup` | Rewrite without internal/process/forbidden language |
| comfort-inn-suites | footprint.openings | `safe_text_cleanup` | Rewrite without internal/process/forbidden language |
| country-inn-suites | footprint.openings | `safe_text_cleanup` | Rewrite without internal/process/forbidden language |
| country-inn-suites | footprint.openings | `safe_text_cleanup` | Rewrite without internal/process/forbidden language |
| country-inn-suites | footprint.geo_intro | `safe_text_cleanup` | Rewrite without internal/process/forbidden language |
| country-inn-suites | materials.caseStudy | `safe_text_cleanup` | Rewrite without internal/process/forbidden language |
| country-inn-suites | footprint.openings | `safe_text_cleanup` | Rewrite without internal/process/forbidden language |
| country-inn-suites | footprint.openings | `safe_text_cleanup` | Rewrite without internal/process/forbidden language |
| courtyard-by-marriott | footprint.openings | `property_example_update` | Refresh location framing using Census city/country for Courtyard by Marriott Monterrey Airport |
| curio-collection | footprint.openings | `property_example_update` | Refresh location framing using Census city/country for MS Milenium Monterrey, Curio Collection by Hilton |
| hilton-garden-inn | footprint.openings | `property_example_update` | Refresh location framing using Census city/country for Hilton Garden Inn Guadalajara Airport |
| hilton-garden-inn | footprint.openings | `property_example_update` | Refresh location framing using Census city/country for Hilton Garden Inn Guadalajara Airport |
| holiday-inn-express | footprint.openings | `property_example_update` | Refresh location framing using Census city/country for Holiday Inn Express Tapachula |
| mgallery-collection | quality_audit_minor_cleanup | `Webflow_render_fix` | Review quiet quality audit findings for MGallery and apply founder-approved minor cleanup only |

---

**Final status:** `brand_explorer_62_background_validation_patch_plan_ready`
