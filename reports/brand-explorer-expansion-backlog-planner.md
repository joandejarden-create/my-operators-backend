# Brand Explorer Expansion Backlog + Wave Planner v28B

- Generated: 2026-07-10T03:20:34.325Z
- Mode: **dry-run**
- Backlog total: **56** brands
- Existing Brand Basics matches: **56**
- New Brand Basics needed: **0**
- Brands with Explorer presentation rows: **4**
- Airtable modified: **no**
- Company Validated untouched: **yes**

## Orchestrator integration (v28C)
Wave commands use `proposedSlug` inputs. `brand-explorer-complete-build` resolves them via `lib/partner-intelligence/brand-explorer-brand-target-resolver.js` (`expansion_backlog` source).

## Factory batch policy
Factory stages continue across brands when image approval is pending; image-dependent activation remains blocked per brand until images are approved.

## Proposed waves
### Wave 1 — Soft-collection pilots (closest to active six) (4 brands)
`npm run brand-explorer-complete-build -- --brands radisson-individuals-by-choice,tapestry-collection-by-hilton,autograph-collection,design-hotels --dry-run --target-quality active-profile`

| Brand | Slug | Priority | In Brand Setup |
| --- | --- | ---: | --- |
| Radisson Individuals by Choice | `radisson-individuals-by-choice` | 115 | yes |
| Tapestry Collection by Hilton | `tapestry-collection-by-hilton` | 108 | yes |
| Autograph Collection | `autograph-collection` | 108 | yes |
| Design Hotels | `design-hotels` | 100 | yes |

### Wave 2 — Marriott ladder (select, extended, lifestyle) (13 brands)
`npm run brand-explorer-complete-build -- --brands courtyard-by-marriott,residence-inn-by-marriott,springhill-suites-by-marriott,towneplace-suites-by-marriott,ac-hotels-by-marriott,aloft-hotels,moxy-hotels,studiores,marriott-hotels,sheraton,westin,four-points-flex-by-sheraton,city-express-by-marriott --dry-run --target-quality active-profile`

| Brand | Slug | Priority | In Brand Setup |
| --- | --- | ---: | --- |
| Courtyard by Marriott | `courtyard-by-marriott` | 94 | yes |
| Residence Inn by Marriott | `residence-inn-by-marriott` | 94 | yes |
| SpringHill Suites by Marriott | `springhill-suites-by-marriott` | 94 | yes |
| TownePlace Suites by Marriott | `towneplace-suites-by-marriott` | 94 | yes |
| AC Hotels by Marriott | `ac-hotels-by-marriott` | 92 | yes |
| Aloft Hotels | `aloft-hotels` | 92 | yes |
| Moxy Hotels | `moxy-hotels` | 92 | yes |
| StudioRes | `studiores` | 92 | yes |
| Marriott Hotels | `marriott-hotels` | 88 | yes |
| Sheraton | `sheraton` | 88 | yes |
| Westin | `westin` | 88 | yes |
| Four Points Flex by Sheraton | `four-points-flex-by-sheraton` | 88 | yes |
| City Express by Marriott | `city-express-by-marriott` | 86 | yes |

### Wave 3 — Hilton select / extended stay (8 brands)
`npm run brand-explorer-complete-build -- --brands home2-suites-by-hilton,tru-by-hilton,hampton-by-hilton,hilton-garden-inn,spark-by-hilton,canopy-by-hilton,motto-by-hilton,tempo-by-hilton --dry-run --target-quality active-profile`

| Brand | Slug | Priority | In Brand Setup |
| --- | --- | ---: | --- |
| Home2 Suites by Hilton | `home2-suites-by-hilton` | 94 | yes |
| Tru by Hilton | `tru-by-hilton` | 94 | yes |
| Hampton by Hilton | `hampton-by-hilton` | 94 | yes |
| Hilton Garden Inn | `hilton-garden-inn` | 94 | yes |
| Spark by Hilton | `spark-by-hilton` | 94 | yes |
| Canopy by Hilton | `canopy-by-hilton` | 92 | yes |
| Motto by Hilton | `motto-by-hilton` | 90 | yes |
| Tempo by Hilton | `tempo-by-hilton` | 90 | yes |

### Wave 4 — Hyatt + IHG lifestyle & select (8 brands)
`npm run brand-explorer-complete-build -- --brands voco-hotels,unbound-collection-by-hyatt,vignette-collection,handwritten-collection,hyatt-place,avid-hotels,even-hotels,holiday-inn-express --dry-run --target-quality active-profile`

| Brand | Slug | Priority | In Brand Setup |
| --- | --- | ---: | --- |
| Voco Hotels | `voco-hotels` | 100 | yes |
| Unbound Collection by Hyatt | `unbound-collection-by-hyatt` | 100 | yes |
| Vignette Collection | `vignette-collection` | 100 | yes |
| Handwritten Collection | `handwritten-collection` | 100 | yes |
| Hyatt Place | `hyatt-place` | 94 | yes |
| avid hotels | `avid-hotels` | 94 | yes |
| Even Hotels | `even-hotels` | 94 | yes |
| Holiday Inn Express | `holiday-inn-express` | 92 | yes |

### Wave 5 — Accor / NH midscale ladder (6 brands)
`npm run brand-explorer-complete-build -- --brands mgallery-collection,ibis,novotel,mercure,nh-hotels,pullman --dry-run --target-quality active-profile`

| Brand | Slug | Priority | In Brand Setup |
| --- | --- | ---: | --- |
| MGallery Collection | `mgallery-collection` | 100 | yes |
| ibis | `ibis` | 94 | yes |
| Novotel | `novotel` | 94 | yes |
| Mercure | `mercure` | 90 | yes |
| NH Hotels | `nh-hotels` | 88 | yes |
| Pullman | `pullman` | 88 | yes |

### Wave 6 — Wyndham + Choice extended stay (8 brands)
`npm run brand-explorer-complete-build -- --brands suburban-studios,woodspring-suites,everhome-suites,esplendor-by-wyndham,dazzler-by-wyndham,trademark-collection-by-wyndham,travelodge-by-wyndham,wyndham --dry-run --target-quality active-profile`

| Brand | Slug | Priority | In Brand Setup |
| --- | --- | ---: | --- |
| Suburban Studios | `suburban-studios` | 101 | yes |
| WoodSpring Suites | `woodspring-suites` | 101 | yes |
| Everhome Suites | `everhome-suites` | 101 | yes |
| Esplendor by Wyndham | `esplendor-by-wyndham` | 100 | yes |
| Dazzler by Wyndham | `dazzler-by-wyndham` | 100 | yes |
| Trademark Collection by Wyndham | `trademark-collection-by-wyndham` | 100 | yes |
| Travelodge by Wyndham | `travelodge-by-wyndham` | 94 | yes |
| Wyndham | `wyndham` | 84 | yes |

### Wave 7 — Resort / all-inclusive / lifestyle independents (6 brands)
`npm run brand-explorer-complete-build -- --brands iberostar-selection,bunkhouse-hotels,hyatt-zilara,hyatt-ziva,secrets-resorts-and-spas,sunscape-resorts-and-spas --dry-run --target-quality active-profile`

| Brand | Slug | Priority | In Brand Setup |
| --- | --- | ---: | --- |
| Iberostar Selection | `iberostar-selection` | 78 | yes |
| Bunkhouse Hotels | `bunkhouse-hotels` | 74 | yes |
| Hyatt Zilara | `hyatt-zilara` | 70 | yes |
| Hyatt Ziva | `hyatt-ziva` | 70 | yes |
| Secrets Resorts & Spas | `secrets-resorts-and-spas` | 70 | yes |
| Sunscape Resorts & Spas | `sunscape-resorts-and-spas` | 70 | yes |

### Wave 8 — Independent luxury collections (highest governance) (3 brands)
`npm run brand-explorer-complete-build -- --brands mr-and-mrs-smith,small-luxury-hotels-of-the-world,the-leading-hotels-of-the-world --dry-run --target-quality active-profile`

| Brand | Slug | Priority | In Brand Setup |
| --- | --- | ---: | --- |
| Mr & Mrs Smith | `mr-and-mrs-smith` | 72 | yes |
| Small Luxury Hotels of the World | `small-luxury-hotels-of-the-world` | 70 | yes |
| The Leading Hotels of the World | `the-leading-hotels-of-the-world` | 66 | yes |

## Recommended first 10 brands
| Rank | Brand | Slug | Priority | Brand Setup | Presentation rows |
| ---: | --- | --- | ---: | --- | ---: |
| 1 | Radisson Individuals by Choice | `radisson-individuals-by-choice` | 115 | yes | 212 |
| 2 | Tapestry Collection by Hilton | `tapestry-collection-by-hilton` | 108 | yes | 0 |
| 3 | Autograph Collection | `autograph-collection` | 108 | yes | 0 |
| 4 | Suburban Studios | `suburban-studios` | 101 | yes | 196 |
| 5 | WoodSpring Suites | `woodspring-suites` | 101 | yes | 195 |
| 6 | Everhome Suites | `everhome-suites` | 101 | yes | 205 |
| 7 | Voco Hotels | `voco-hotels` | 100 | yes | 0 |
| 8 | Unbound Collection by Hyatt | `unbound-collection-by-hyatt` | 100 | yes | 0 |
| 9 | Vignette Collection | `vignette-collection` | 100 | yes | 0 |
| 10 | Esplendor by Wyndham | `esplendor-by-wyndham` | 100 | yes | 0 |

## Highest complexity brands
| Brand | Slug | Overall | Source | Image | Fact |
| --- | --- | ---: | ---: | ---: | ---: |
| Hyatt Zilara | `hyatt-zilara` | 14 | 4 | 5 | 5 |
| Hyatt Ziva | `hyatt-ziva` | 14 | 4 | 5 | 5 |
| Secrets Resorts & Spas | `secrets-resorts-and-spas` | 14 | 4 | 5 | 5 |
| Sunscape Resorts & Spas | `sunscape-resorts-and-spas` | 14 | 4 | 5 | 5 |
| Small Luxury Hotels of the World | `small-luxury-hotels-of-the-world` | 14 | 5 | 4 | 5 |
| The Leading Hotels of the World | `the-leading-hotels-of-the-world` | 14 | 5 | 4 | 5 |
| Mr & Mrs Smith | `mr-and-mrs-smith` | 13 | 4 | 4 | 5 |
| Iberostar Selection | `iberostar-selection` | 12 | 4 | 4 | 4 |
| Bunkhouse Hotels | `bunkhouse-hotels` | 12 | 4 | 4 | 4 |
| Voco Hotels | `voco-hotels` | 9 | 3 | 3 | 3 |

## Brands needing special handling
- **Radisson Individuals by Choice** (`radisson-individuals-by-choice`): sibling_active_radisson
- **Tapestry Collection by Hilton** (`tapestry-collection-by-hilton`): sibling_active_curio
- **Autograph Collection** (`autograph-collection`): sibling_active_tribute
- **Design Hotels** (`design-hotels`): sibling_active_tribute, marriott_affiliate_not_flag
- **Four Points Flex by Sheraton** (`four-points-flex-by-sheraton`): sheraton_sub_brand
- **City Express by Marriott** (`city-express-by-marriott`): cala_regional_focus
- **Wyndham** (`wyndham`): parent_brand_name_collision_risk
- **Iberostar Selection** (`iberostar-selection`): resort_governance
- **Bunkhouse Hotels** (`bunkhouse-hotels`): independent_portfolio, limited_franchise_fdd
- **Mr & Mrs Smith** (`mr-and-mrs-smith`): hyatt_acquisition, legal_sensitivity
- **Hyatt Zilara** (`hyatt-zilara`): adults_only_resort, legal_sensitivity
- **Hyatt Ziva** (`hyatt-ziva`): family_resort, legal_sensitivity
- **Secrets Resorts & Spas** (`secrets-resorts-and-spas`): adults_only_resort, legal_sensitivity
- **Sunscape Resorts & Spas** (`sunscape-resorts-and-spas`): family_resort, legal_sensitivity
- **Small Luxury Hotels of the World** (`small-luxury-hotels-of-the-world`): independent_consortium, legal_sensitivity
- **The Leading Hotels of the World** (`the-leading-hotels-of-the-world`): independent_consortium, legal_sensitivity, no_single_parent_fdd

## Review queue summary
- `pending_source_review`: 52
- `pending_fact_review`: 4
- `pending_image_review`: 0
- `pending_founder_copy_review`: 0
- `pending_legal_sensitivity_review`: 0
- `ready_for_apply`: 0
- `active_profile_ready`: 0

## Suggested commands by wave
- Wave 1: `npm run brand-explorer-complete-build -- --brands radisson-individuals-by-choice,tapestry-collection-by-hilton,autograph-collection,design-hotels --dry-run --target-quality active-profile`
- Wave 2: `npm run brand-explorer-complete-build -- --brands courtyard-by-marriott,residence-inn-by-marriott,springhill-suites-by-marriott,towneplace-suites-by-marriott,ac-hotels-by-marriott,aloft-hotels,moxy-hotels,studiores,marriott-hotels,sheraton,westin,four-points-flex-by-sheraton,city-express-by-marriott --dry-run --target-quality active-profile`
- Wave 3: `npm run brand-explorer-complete-build -- --brands home2-suites-by-hilton,tru-by-hilton,hampton-by-hilton,hilton-garden-inn,spark-by-hilton,canopy-by-hilton,motto-by-hilton,tempo-by-hilton --dry-run --target-quality active-profile`
- Wave 4: `npm run brand-explorer-complete-build -- --brands voco-hotels,unbound-collection-by-hyatt,vignette-collection,handwritten-collection,hyatt-place,avid-hotels,even-hotels,holiday-inn-express --dry-run --target-quality active-profile`
- Wave 5: `npm run brand-explorer-complete-build -- --brands mgallery-collection,ibis,novotel,mercure,nh-hotels,pullman --dry-run --target-quality active-profile`
- Wave 6: `npm run brand-explorer-complete-build -- --brands suburban-studios,woodspring-suites,everhome-suites,esplendor-by-wyndham,dazzler-by-wyndham,trademark-collection-by-wyndham,travelodge-by-wyndham,wyndham --dry-run --target-quality active-profile`
- Wave 7: `npm run brand-explorer-complete-build -- --brands iberostar-selection,bunkhouse-hotels,hyatt-zilara,hyatt-ziva,secrets-resorts-and-spas,sunscape-resorts-and-spas --dry-run --target-quality active-profile`
- Wave 8: `npm run brand-explorer-complete-build -- --brands mr-and-mrs-smith,small-luxury-hotels-of-the-world,the-leading-hotels-of-the-world --dry-run --target-quality active-profile`

## Guardrails
- Dry-run default — no Airtable writes
- Brand Basics create gated: `--apply-create-backlog`
- No automatic image or fact approval
- Company Validated never modified by this planner