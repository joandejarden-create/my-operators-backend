# Wave 13 Public Six Geo + Momentum Cleanup — Fairmont

Slug: `fairmont-hotels-and-resorts` · Record: `recJhPaDVU3YUDQUt`

## Before

- Geo intro words: 73
- Regions: footprint.region.cala (24w), footprint.region.am (empty), footprint.region.eu (empty)
- Momentum cards: 2

## After

- Geo intro words: 48

- **CALA** (`footprint.region.cala`, 38w) — CALA
- **Americas** (`footprint.region.am`, 39w) — International Reference · Americas
- **Europe** (`footprint.region.eu`, 43w) — International Reference · Europe

### Recent Momentum

- **Accor Group Extends Fairmont Make Special Happen Experiences** · Nov 2025 · International Reference
- **Fairmont Make Special Happen Campaign Marks Global Brand Momentum** · May 2025 · International Reference
- **Fairmont Mayakoba Anchors CALA Resort Luxury Proof** · Directory · CALA

## Patches

- `PATCH` `footprint.geo_intro` — geo_intro_owner_facing
- `PATCH` `footprint.region.cala` — region_card_source_supported
- `POST` `footprint.region.am` — region_card_create
- `POST` `footprint.region.eu` — region_card_create
- `PATCH` `footprint.momentum` — hide_unstructured_momentum_card
- `PATCH` `footprint.momentum` — hide_unstructured_momentum_card
- `PATCH` `footprint.momentum_label` — momentum_label_contract
- `POST` `footprint.momentum` — create_structured_momentum_card
- `POST` `footprint.momentum` — create_structured_momentum_card
- `POST` `footprint.momentum` — create_structured_momentum_card
