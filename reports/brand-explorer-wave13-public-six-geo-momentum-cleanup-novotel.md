# Wave 13 Public Six Geo + Momentum Cleanup — Novotel

Slug: `novotel` · Record: `recQE2lSSSSyuUrMQ`

## Before

- Geo intro words: 73
- Regions: footprint.region.cala (36w), footprint.region.eu (empty), footprint.region.am (empty)
- Momentum cards: 2

## After

- Geo intro words: 47

- **CALA** (`footprint.region.cala`, 45w) — CALA
- **Europe** (`footprint.region.eu`, 39w) — International Reference · Europe
- **Americas** (`footprint.region.am`, 38w) — CALA-linked Americas diligence

### Recent Momentum

- **Why Invest In Novotel 2026 Frames Wellbeing Midscale Platform** · 2026 · International Reference
- **Accor 2025 Openings Line-Up Includes Novotel Conversions** · 2025 · International Reference
- **Novotel Mexico City World Trade Center Shows CALA Midscale Mix** · Directory · CALA

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
