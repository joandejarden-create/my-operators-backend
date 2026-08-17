# Wave 14 Tab Factory Build

Stage 4 builds owner-facing Presentation content for **nine approved Wave 14 brands** from source packs.

## Approved Stage 4 scope

- `marriott-hotels`
- `sheraton`
- `westin`
- `residence-inn-by-marriott`
- `springhill-suites-by-marriott`
- `towneplace-suites-by-marriott`
- `aloft-hotels`
- `four-points-flex-by-sheraton`
- `studiores`

## Exclusions

- `the-house-of-originals`
- `morgans-originals`
- `radisson-collection`
- protected 46 Active/Live brands (read-only validation only)

## Allowed writes

- Presentation Title / Body / Case Summary / chips
- Brand Basics: Brand Positioning, Guest Psychographics Description, Brand Value Proposition
- Brand Basics: Target Guest Segments only when validated

## Forbidden

- Brand Status, release fields, Company Validated, Source Library, Registry
- Images and non-target brands
- Section labels as scenario titles; owner-fit diligence / process language

## Stage 4 acceptance posture

- All nine remain **Under Review** / factory preview only
- No images written (Stage 5)
- Tab-factory audit: golden + no-empty PASS; only remaining fails are `overview.scenario.*` missing images (accepted Stage 5 gap)
- Ready token: `wave14_stage4_content_clean_ready_for_image_materialization`

## Steward gaps before image materialization

| Brand | Gap | Hold? |
|-------|-----|-------|
| SpringHill Suites | CALA weak — International Reference openings/market archetypes | Prefer steward-matched property URLs before gallery harden |
| TownePlace Suites | CALA weak — International Reference openings/market archetypes | Prefer steward-matched property URLs before gallery harden |
| Four Points Flex | No CALA; do not use Four Points by Sheraton properties | Hold named property cards until Flex URL match |
| StudioRes | No CALA; Fort Myers needs property overview URL confirm | Hold gallery until URL steward match |

CALA-first brands (Marriott Hotels, Sheraton, Westin, Residence Inn, Aloft) may proceed to Stage 5 image materialization with CALA property preference.

## Ready for images

`wave14_stage4_content_clean_ready_for_image_materialization` after apply + post-validation.
