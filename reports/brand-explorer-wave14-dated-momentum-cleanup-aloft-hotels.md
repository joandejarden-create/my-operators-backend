# Wave 14 Dated Momentum Cleanup — Aloft Hotels

Slug: `aloft-hotels` · Record: `recJ1GZQpttX7qHgw`

## Before

| Title | DateLine | Section dated | URL |
| --- | --- | --- | --- |
| Aloft Cancun Confirms CALA Lifestyle Select-Service Presence | 2026 | true | https://www.marriott.com/en-us/hotels/cunal-aloft-cancun/overview/ |
| Aloft Development Page Frames Lifestyle Select-Service Lane | 2026 | true | https://www.hotel-development.marriott.com/brands/aloft |

## After (planned)

| Title | DateLine | Region | Source |
| --- | --- | --- | --- |
| Aloft Cancun Confirms Aloft Hotels CALA Lifestyle Select-Service Presence | **Directory** | CALA | https://www.marriott.com/en-us/hotels/cunal-aloft-cancun/overview/ |
| Aloft Hotels Development Page Frames Lifestyle Select-Service Lane | **2026** | International Reference | https://www.hotel-development.marriott.com/brands/aloft |
| Aloft Hotels Brand Presence Confirms Lifestyle Select-Service Product | **2026** | International Reference | https://aloft-hotels.marriott.com/ |

Date rationale: Property overview cards use Directory (evidence rule). Development/brand pages use steward-year 2026 for section-pattern dated minimum (Wave 13 living-page convention). URLs from Wave 14 source packs.

## Patches (3)

- `PATCH` footprint.momentum — repair_dated_momentum_card (`recExHSOmvBr9mbfg`)
- `PATCH` footprint.momentum — repair_dated_momentum_card (`recQlhTPqQFT6KfQt`)
- `POST` footprint.momentum — create_dated_momentum_card

