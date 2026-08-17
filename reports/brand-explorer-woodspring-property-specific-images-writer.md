# Brand Explorer WoodSpring Property-Specific Hotel Images v33C-R2

- Generated: 2026-07-13T19:34:13.954Z
- Mode: **dry-run**
- Dry-run clean: **yes**
- Visible property cards after apply: **3**
- Section partially complete: **no**

## Current visible property example audit
- `recI3cbO8mOhEpo1W` **WoodSpring Suites Orlando — U.S. Property Example** — image: https://www.choicehotels.com/hoteldam/fl/flf21/images/1280/FLF21Exterior1_1.jpg; property-specific: true; hotel photo: true; action: replace
- `recpNB0KoPq6y3Mhs` **WoodSpring Suites Charlotte — U.S. Property Example** — image: https://www.choicehotels.com/hoteldam/nc/ncb10/images/1280/NCB10Exterior1.jpg; property-specific: true; hotel photo: true; action: replace
- `rec4Eqp9lwXSP7UQE` **WoodSpring Suites Raleigh — U.S. Property Example** — image: https://www.choicehotels.com/hoteldam/nc/nc936/images/480/NC936Exterior01_1.jpg; property-specific: true; hotel photo: true; action: replace

## Property-specific image discovery
- **WoodSpring Suites Orlando** (flf21): https://www.choicehotels.com/hoteldam/fl/flf21/images/1280/FLF21Exterior1_1.jpg
- **WoodSpring Suites Charlotte** (ncb10): https://www.choicehotels.com/hoteldam/nc/ncb10/images/1280/NCB10Exterior1.jpg
- **WoodSpring Suites Raleigh** (nc936): https://www.choicehotels.com/hoteldam/nc/nc936/images/480/NC936Exterior01_1.jpg

## Cards preserved: 0

## Cards replaced: 3
- WoodSpring Suites Orlando (`recI3cbO8mOhEpo1W`) → https://www.choicehotels.com/hoteldam/fl/flf21/images/1280/FLF21Exterior1_1.jpg
- WoodSpring Suites Charlotte (`recpNB0KoPq6y3Mhs`) → https://www.choicehotels.com/hoteldam/nc/ncb10/images/1280/NCB10Exterior1.jpg
- WoodSpring Suites Raleigh (`rec4Eqp9lwXSP7UQE`) → https://www.choicehotels.com/hoteldam/nc/nc936/images/480/NC936Exterior01_1.jpg

## Cards hidden: 0

## Gallery corrections

## Images rejected

## Registry creates: 3; patches: 0

## Logo/lifestyle/generic remain visible: **no**

## Expected QA
- Final QA: projected_ready_after_property_specific_hotel_images (3/3 cards)
- Complete Build: projected_active_profile_ready_with_3_property_hotel_images
- Visual defects: projected_reduction_in_property_example_generic_image_defects

```bash
npm run brand-explorer-woodspring-property-specific-images-writer -- --brand woodspring-suites --apply --approve-brand-explorer-v33C-R2-woodspring-property-specific-images --founder-approved-woodspring-property-specific-hotel-images --confirm-official-source-images-only --confirm-no-company-validation-claim --confirm-no-summary-url-field --confirm-no-momentum-proof-standard-changes --confirm-woodspring-only
```
