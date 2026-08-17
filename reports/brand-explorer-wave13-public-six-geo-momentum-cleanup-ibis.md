# Wave 13 Public Six Geo + Momentum Cleanup — ibis

Slug: `ibis` · Record: `reclFXbpZ5XzLWbGP`

## Before

- Geo intro words: 44
- Regions: footprint.region.cala (31w), footprint.region.eu (empty), footprint.region.am (empty)
- Momentum cards: 2

## After

- Geo intro words: 53

- **CALA** (`footprint.region.cala`, 42w) — CALA · Master ibis
- **Europe** (`footprint.region.eu`, 40w) — International Reference · Europe
- **Americas** (`footprint.region.am`, 39w) — CALA-linked Americas diligence

### Recent Momentum

- **Accor Press Highlights ibis Economy Network Growth** · 2025 · International Reference
- **ibis Mexico Alameda Confirms Master Brand CALA Presence** · Directory · CALA
- **ibis Lima Larco Miraflores Extends CALA Essential-Stay Proof** · Directory · CALA

## Patches

- `PATCH` `footprint.geo_intro` — geo_intro_owner_facing
- `PATCH` `footprint.region.cala` — region_card_source_supported
- `POST` `footprint.region.eu` — region_card_create
- `POST` `footprint.region.am` — region_card_create
- `PATCH` `footprint.momentum` — hide_unstructured_momentum_card
- `PATCH` `footprint.momentum` — hide_unstructured_momentum_card
- `PATCH` `footprint.momentum_label` — momentum_label_contract
- `POST` `footprint.momentum` — create_structured_momentum_card
- `POST` `footprint.momentum` — create_structured_momentum_card
- `POST` `footprint.momentum` — create_structured_momentum_card
