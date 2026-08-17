# Wave 14 Dated Momentum Cleanup — Residence Inn by Marriott

Slug: `residence-inn-by-marriott` · Record: `rec9Ufbpa0GxJGzt8`

## Before

| Title | DateLine | Section dated | URL |
| --- | --- | --- | --- |
| Residence Inn Merida Confirms CALA Upscale Extended-Stay Presence | 2026 | true | https://www.marriott.com/en-us/hotels/midri-residence-inn-merida/overview/ |
| Residence Inn On Marriott Longer Stays Family Positioning | 2026 | true | https://www.hotel-development.marriott.com/brands/residence-inn |

## After (planned)

| Title | DateLine | Region | Source |
| --- | --- | --- | --- |
| Residence Inn Merida Confirms Residence Inn by Marriott CALA Presence | **Directory** | CALA | https://www.marriott.com/en-us/hotels/midri-residence-inn-merida/overview/ |
| Residence Inn by Marriott Development Page Frames Upscale Extended-Stay | **2026** | International Reference | https://www.hotel-development.marriott.com/brands/residence-inn |
| Residence Inn by Marriott On Marriott Longer Stays Family Positioning | **2026** | International Reference | https://www.hotel-development.marriott.com/brands/extended-stay-brands |

Date rationale: Property overview cards use Directory (evidence rule). Development/brand pages use steward-year 2026 for section-pattern dated minimum (Wave 13 living-page convention). URLs from Wave 14 source packs.

## Patches (3)

- `PATCH` footprint.momentum — repair_dated_momentum_card (`recrsjVlquS8A7EKP`)
- `PATCH` footprint.momentum — repair_dated_momentum_card (`recHR4bVxODjLk1KK`)
- `POST` footprint.momentum — create_dated_momentum_card

