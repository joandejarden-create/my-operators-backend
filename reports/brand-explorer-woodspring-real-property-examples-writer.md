# Brand Explorer WoodSpring Real Property Examples v33C-R1

- Generated: 2026-07-13T18:40:46.549Z
- Mode: **dry-run**
- Dry-run clean: **yes**
- Company Validated untouched: **yes**

## CALA availability
- No CALA WoodSpring property URLs in approved extract; U.S. property examples required.

## Current openings audit
- `rec4Eqp9lwXSP7UQE` **WoodSpring Suites Raleigh — U.S. Property Example** — type: property_example; image: temporary_airtable; action: rewrite_as_property_example
- `recI3cbO8mOhEpo1W` **WoodSpring Suites Orlando — U.S. Property Example** — type: property_example; image: temporary_airtable; action: rewrite_as_property_example
- `recdC5lflCPCtjbkr` **Choice Extended-Stay Platform Example** — type: generic_platform_example; image: temporary_airtable; action: hide
- `recpNB0KoPq6y3Mhs` **WoodSpring Suites Charlotte — U.S. Property Example** — type: property_example; image: temporary_airtable; action: rewrite_as_property_example

## Selected property examples
- **WoodSpring Suites Orlando** — Orlando, Florida; source: https://www.choicehotels.com/florida/orlando/woodspring-hotels/flf21; image: official_woodspring_brand_image
- **WoodSpring Suites Charlotte** — Charlotte, North Carolina; source: https://www.choicehotels.com/north-carolina/charlotte/woodspring-hotels/ncb10; image: official_woodspring_brand_image
- **WoodSpring Suites Raleigh** — Raleigh, North Carolina; source: https://www.choicehotels.com/north-carolina/raleigh/woodspring-hotels/nc936; image: official_woodspring_brand_image

## Before / after
- `recI3cbO8mOhEpo1W`: WoodSpring Suites Orlando — U.S. Property Example → WoodSpring Suites Orlando — U.S. Property Example
- `recpNB0KoPq6y3Mhs`: WoodSpring Suites Charlotte — U.S. Property Example → WoodSpring Suites Charlotte — U.S. Property Example
- `rec4Eqp9lwXSP7UQE`: WoodSpring Suites Raleigh — U.S. Property Example → WoodSpring Suites Raleigh — U.S. Property Example
- `recdC5lflCPCtjbkr`: Choice Extended-Stay Platform Example → (hidden)

## Registry
- Patches: **0**
- Creates: **3**
- Hidden rows: **0**
- Choice-logo property image used: **no**
- Generic cards remain visible: **no**

## Expected QA
- Final QA: projected_ready_from_unknown_after_property_example_rebuild
- Complete Build: projected_improvement; standards may still block until v33F
- Visual defects: projected_reduction_from_unknown_after_distinct_property_images

```bash
npm run brand-explorer-woodspring-real-property-examples-writer -- --brand woodspring-suites --apply --approve-brand-explorer-v33C-R1-woodspring-real-property-examples --founder-approved-woodspring-property-example-images --confirm-official-source-images-only --confirm-no-company-validation-claim --confirm-no-summary-url-field --confirm-no-momentum-gallery-proof-standard-changes --confirm-woodspring-only
```
