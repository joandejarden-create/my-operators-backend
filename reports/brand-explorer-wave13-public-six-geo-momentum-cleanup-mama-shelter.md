# Wave 13 Public Six Geo + Momentum Cleanup — Mama Shelter

Slug: `mama-shelter` · Record: `recXCZCK05XXYX7Q8`

## Before

- Geo intro words: 50
- Regions: footprint.region.eu (empty), footprint.region.cala (33w), footprint.region.am (empty)
- Momentum cards: 2

## After

- Geo intro words: 65

- **Europe** (`footprint.region.eu`, 49w) — International Reference · Europe
- **CALA** (`footprint.region.cala`, 42w) — CALA · Pipeline
- **Americas** (`footprint.region.am`, 47w) — CALA-linked Americas diligence · Pipeline

### Recent Momentum

- **Mama Shelter Brand Page Frames Affordable Urban Lifestyle** · 2026 · International Reference
- **Mama Shelter Paris East Anchors European Lifestyle Proof** · Directory · International Reference
- **Mama Shelter Mexico City Pipeline Listed On Accor ALL** · Pipeline · CALA

## Patches

- `PATCH` `footprint.geo_intro` — geo_intro_owner_facing
- `POST` `footprint.region.eu` — region_card_create
- `PATCH` `footprint.region.cala` — region_card_source_supported
- `POST` `footprint.region.am` — region_card_create
- `PATCH` `footprint.momentum` — hide_unstructured_momentum_card
- `PATCH` `footprint.momentum` — hide_unstructured_momentum_card
- `PATCH` `footprint.momentum_label` — momentum_label_contract
- `POST` `footprint.momentum` — create_structured_momentum_card
- `POST` `footprint.momentum` — create_structured_momentum_card
- `POST` `footprint.momentum` — create_structured_momentum_card
