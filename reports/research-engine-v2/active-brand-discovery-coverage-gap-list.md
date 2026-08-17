# Active Brand Discovery Coverage Gap List

Generated: 2026-08-05T23:57:35.743Z

**Production writes:** false · Brand Setup/Explorer untouched · Hotel Property Census not written

## Summary

- Active/Live brands in scope: **62**
- Excluded from Webhound (already supported): Marriott, IHG, Hilton, Choice
- Gap parents for Webhound: **6** (Bunkhouse, BWH Hotels, Wyndham, Accor, Preferred Hotels & Resorts, SLH)

## Parent matrix

| Parent | Brands | Adapter status | Module | Webhound? |
| --- | ---: | --- | --- | --- |
| Accor | 9 | partial | lib/accor-brand-directory-extract.js; lib/accor-continent-directory-extract.js | yes |
| Bunkhouse | 1 | missing | — | yes |
| BWH Hotels | 2 | partial | lib/bwh-brand-directory-extract.js | yes |
| Choice | 9 | supported | lib/research-engine-v2/census-autopilot-choice-cala-discovery-adapter.js | no |
| Hilton | 13 | supported | lib/research-engine-v2/census-autopilot-hilton-cala-discovery-adapter.js | no |
| IHG | 8 | supported | lib/research-engine-v2/census-autopilot-ihg-cala-discovery-adapter.js | no |
| Marriott | 14 | supported | lib/research-engine-v2/census-autopilot-marriott-discovery-adapter.js | no |
| Preferred Hotels & Resorts | 1 | missing | — | yes |
| SLH | 1 | missing | — | yes |
| Wyndham | 4 | partial | lib/wyndham-brand-directory-extract.js | yes |

## Webhound gap parents

### Bunkhouse
- Brands (1): bunkhouse-hotels
- Representative sample: bunkhouse-hotels
- Status: missing
- Module: none

### BWH Hotels
- Brands (2): bw-premier-collection, bw-signature-collection
- Representative sample: bw-premier-collection, bw-signature-collection
- Status: partial
- Module: lib/bwh-brand-directory-extract.js

### Wyndham
- Brands (4): dazzler-by-wyndham, everhome-suites, trademark-collection-by-wyndham, woodspring-suites
- Representative sample: dazzler-by-wyndham, everhome-suites, trademark-collection-by-wyndham
- Status: partial
- Module: lib/wyndham-brand-directory-extract.js

### Accor
- Brands (9): design-hotels, fairmont-hotels-and-resorts, ibis, mama-shelter, mercure, mgallery-collection, novotel, pullman, so-hotels-and-resorts
- Representative sample: design-hotels, fairmont-hotels-and-resorts, ibis
- Status: partial
- Module: lib/accor-brand-directory-extract.js; lib/accor-continent-directory-extract.js

### Preferred Hotels & Resorts
- Brands (1): preferred-hotels-and-resorts
- Representative sample: preferred-hotels-and-resorts
- Status: missing
- Module: none

### SLH
- Brands (1): small-luxury-hotels-of-the-world
- Representative sample: small-luxury-hotels-of-the-world
- Status: missing
- Module: none

## Brand rows (compact)

| Brand | Slug | Parent (inferred) | Status | WH |
| --- | --- | --- | --- | --- |
| AC Hotels by Marriott | ac-hotels-by-marriott | Marriott | supported | N |
| Aloft Hotels | aloft-hotels | Marriott | supported | N |
| Ascend Hotel Collection | ascend | Choice | supported | N |
| Autograph Collection | autograph-collection | Marriott | supported | N |
| avid hotels | avid-hotels | IHG | supported | N |
| Bunkhouse Hotels | bunkhouse-hotels | Bunkhouse | missing | Y |
| BW Premier Collection | bw-premier-collection | BWH Hotels | partial | Y |
| BW Signature Collection | bw-signature-collection | BWH Hotels | partial | Y |
| Canopy by Hilton | canopy-by-hilton | Hilton | supported | N |
| City Express by Marriott | city-express-by-marriott | Marriott | supported | N |
| Comfort Inn & Suites | comfort-inn-suites | Choice | supported | N |
| Country Inn & Suites by Choice | country-inn-suites | Choice | supported | N |
| Courtyard by Marriott | courtyard-by-marriott | Marriott | supported | N |
| Curio Collection by Hilton | curio-collection | Hilton | supported | N |
| Dazzler by Wyndham | dazzler-by-wyndham | Wyndham | partial | Y |
| Design Hotels | design-hotels | Accor | partial | Y |
| DoubleTree by Hilton | doubletree-by-hilton | Hilton | supported | N |
| Even Hotels | even-hotels | IHG | supported | N |
| Everhome Suites | everhome-suites | Wyndham | partial | Y |
| Fairmont | fairmont-hotels-and-resorts | Accor | partial | Y |
| Hampton by Hilton | hampton-by-hilton | Hilton | supported | N |
| Handwritten Collection | handwritten-collection | IHG | supported | N |
| Hilton Garden Inn | hilton-garden-inn | Hilton | supported | N |
| Hilton Hotels & Resorts | hilton-hotels-and-resorts | Hilton | supported | N |
| Holiday Inn Express | holiday-inn-express | IHG | supported | N |
| Home2 Suites by Hilton | home2-suites-by-hilton | Hilton | supported | N |
| Homewood Suites by Hilton | homewood-suites-by-hilton | Hilton | supported | N |
| Hotel Indigo | hotel-indigo | IHG | supported | N |
| ibis | ibis | Accor | partial | Y |
| Kimpton Hotels | kimpton | IHG | supported | N |
| Mama Shelter | mama-shelter | Accor | partial | Y |
| Marriott Hotels | marriott-hotels | Marriott | supported | N |
| Mercure | mercure | Accor | partial | Y |
| MGallery Collection | mgallery-collection | Accor | partial | Y |
| Motto by Hilton | motto-by-hilton | Hilton | supported | N |
| Moxy Hotels | moxy-hotels | Marriott | supported | N |
| Novotel | novotel | Accor | partial | Y |
| Preferred Hotels & Resorts | preferred-hotels-and-resorts | Preferred Hotels & Resorts | missing | Y |
| Pullman | pullman | Accor | partial | Y |
| Quality Inn | quality-inn | Choice | supported | N |
| Radisson by Choice | radisson | Choice | supported | N |
| Radisson Blu by Choice | radisson-blu | Choice | supported | N |
| Radisson Individuals by Choice | radisson-individuals-by-choice | Choice | supported | N |
| Radisson RED by Choice | radisson-red | Choice | supported | N |
| Residence Inn by Marriott | residence-inn-by-marriott | Marriott | supported | N |
| Sheraton | sheraton | Marriott | supported | N |
| Small Luxury Hotels of the World | small-luxury-hotels-of-the-world | SLH | missing | Y |
| SO/ | so-hotels-and-resorts | Accor | partial | Y |
| Spark by Hilton | spark-by-hilton | Hilton | supported | N |
| SpringHill Suites by Marriott | springhill-suites-by-marriott | Marriott | supported | N |
| StudioRes | studiores | Marriott | supported | N |
| Suburban Studios | suburban-studios | Choice | supported | N |
| Tapestry Collection by Hilton | tapestry-collection-by-hilton | Hilton | supported | N |
| Tempo by Hilton | tempo-by-hilton | Hilton | supported | N |
| TownePlace Suites by Marriott | towneplace-suites-by-marriott | Marriott | supported | N |
| Trademark Collection by Wyndham | trademark-collection-by-wyndham | Wyndham | partial | Y |
| Tribute Portfolio | tribute-portfolio | Marriott | supported | N |
| Tru by Hilton | tru-by-hilton | Hilton | supported | N |
| Vignette Collection | vignette-collection | IHG | supported | N |
| Voco Hotels | voco-hotels | IHG | supported | N |
| Westin | westin | Marriott | supported | N |
| WoodSpring Suites | woodspring-suites | Wyndham | partial | Y |