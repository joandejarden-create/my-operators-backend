# Production Cycle Plan

- Mode: production-cycle
- Region: CALA
- Scope: active-brand-setup
- Batch size: 100
- Max passes: 3
- Writes enabled: true
- Census before: 741
- Queue order: source_discovery → address_confirmation → property_name_cleanup → description_extraction → amenities_extraction → property_type_asset_context → rooms_keys → radar_public_readiness → coordinate_resolution
- Per-bundle ChatGPT approval: **false** (founder CLI is approval)
