# Wave 13 Public Six Geo + Momentum Cleanup — Mercure

Slug: `mercure` · Record: `recevrLJ3m6rIug3S`

## Before

- Geo intro words: 77
- Regions: footprint.region.cala (34w), footprint.region.apac (empty), footprint.region.eu (empty)
- Momentum cards: 3

## After

- Geo intro words: 57

- **CALA** (`footprint.region.cala`, 44w) — CALA
- **APAC** (`footprint.region.apac`, 39w) — International Reference · APAC
- **Europe** (`footprint.region.eu`, 40w) — International Reference · Europe

### Recent Momentum

- **Accor Press Highlights Mercure In Midscale Conversion Growth** · 2024 · International Reference
- **Mercure Bogotá BH Zona Financiera Shows CALA Midscale Fit** · Directory · CALA
- **Mercure Bangkok Sukhumvit 11 Anchors APAC Local Immersion** · Directory · International Reference

## Patches

- `PATCH` `footprint.geo_intro` — geo_intro_owner_facing
- `PATCH` `footprint.region.cala` — region_card_source_supported
- `POST` `footprint.region.apac` — region_card_create
- `POST` `footprint.region.eu` — region_card_create
- `PATCH` `footprint.momentum` — hide_unstructured_momentum_card
- `PATCH` `footprint.momentum` — hide_unstructured_momentum_card
- `PATCH` `footprint.momentum` — hide_unstructured_momentum_card
- `PATCH` `footprint.momentum_label` — momentum_label_contract
- `POST` `footprint.momentum` — create_structured_momentum_card
- `POST` `footprint.momentum` — create_structured_momentum_card
- `POST` `footprint.momentum` — create_structured_momentum_card
