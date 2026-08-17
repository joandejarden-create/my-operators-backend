# Wave 13 — SO/ Hold Remediation

Version: `wave13-so-hold-remediation-v1` · Packages: `wave13-so-hold-remediation-packages-v1`
Generated: 2026-07-28T06:49:59.992Z
Mode: **APPLY**

Ready: `so_hold_remediation_applied_ready_for_validation`

## Scope

- Target: `so-hotels-and-resorts` only
- Brand Status: remains **Under Review** (no write)
- Active 45 / House of Originals / Morgans / Radisson: **untouched**

## Summary

- Planned patches: 13
- Applied writes: 13
- Image writes: **0**
- Brand Status / release / CV / Source Library / Registry writes: **false**
- Steward snapshot invent-fills: **false**

## After plan

- Body rewrites: 36
- Regions: footprint.region.eu, footprint.region.apac, footprint.region.am, footprint.region.cala
- Momentum: SO/ Paris Featured as Fashion and Art Lifestyle Flagship; SO/ Fashion-Rooted Luxury Lifestyle Collection — Accor Brand Materials
- Openings: SO/ Paris SO/ Hotels & Resorts — Paris; SO/ Maldives SO/ Hotels & Resorts — Maldives; SO/ Berlin Das Stue B1Y6 SO/ Hotels & Resorts — Berlin

## Patches

- `PATCH` `valueOwners.lifecycle.5` — remove_process_language_residue (`recGGgTeTFZzE4b0A`)
- `PATCH` `footprint.region.eu` — region_card_source_supported (`recuJ1Ddx3EE2zJZ0`)
- `PATCH` `footprint.region.apac` — region_card_source_supported (`recHUDwiFhS1KnwUd`)
- `PATCH` `footprint.region.am` — region_card_source_supported (`recsOrFpgdqddcFv7`)
- `PATCH` `footprint.region.cala` — region_card_source_supported (`recHvd9yiePKd36A2`)
- `PATCH` `footprint.momentum` — hide_unstructured_momentum_card (`rec285NhrvNO9iL7d`)
- `PATCH` `footprint.momentum` — hide_unstructured_momentum_card (`recLZwISKuyZIYCPx`)
- `PATCH` `footprint.momentum_label` — momentum_label_contract (`recoTUJqIWXzk9mAP`)
- `POST` `footprint.momentum` — create_structured_momentum_card
- `POST` `footprint.momentum` — create_structured_momentum_card
- `PATCH` `footprint.openings` — openings_contract_rebuild (`rec4JfDQwMTWhKtx7`)
- `PATCH` `footprint.openings` — openings_contract_rebuild (`rec0ItBYcXq3miZ4i`)
- `PATCH` `footprint.openings` — openings_contract_rebuild (`reczkBnzUciUjCRmT`)

