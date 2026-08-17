# Wave 14 Dated Momentum Cleanup — Marriott Hotels

Slug: `marriott-hotels` · Record: `recn59UtkyyoYwzSz`

## Before

| Title | DateLine | Section dated | URL |
| --- | --- | --- | --- |
| Marriott Cancun Resort Confirms CALA Full-Service Presence | 2026 | true | https://www.marriott.com/en-us/hotels/cunmc-marriott-cancun-resort/overview/ |
| Marriott Hotels Development Page Frames Flagship Full-Service Lane | 2026 | true | https://www.hotel-development.marriott.com/brands/marriott |

## After (planned)

| Title | DateLine | Region | Source |
| --- | --- | --- | --- |
| Marriott Cancun Resort Confirms Marriott Hotels CALA Presence | **Directory** | CALA | https://www.marriott.com/en-us/hotels/cunmc-marriott-cancun-resort/overview/ |
| Marriott Hotels Development Page Frames Flagship Full-Service Lane | **2026** | International Reference | https://www.hotel-development.marriott.com/brands/marriott |
| Marriott Hotels Brand Site Confirms Flagship Full-Service Product | **2026** | International Reference | https://marriott-hotels.marriott.com/ |

Date rationale: Property overview cards use Directory (evidence rule). Development/brand pages use steward-year 2026 for section-pattern dated minimum (Wave 13 living-page convention). URLs from Wave 14 source packs.

## Patches (3)

- `PATCH` footprint.momentum — repair_dated_momentum_card (`recJgI22FKQDDSIQD`)
- `PATCH` footprint.momentum — repair_dated_momentum_card (`rec1V75Z0JGBSuasG`)
- `POST` footprint.momentum — create_dated_momentum_card

