# Discovery Source Report

- **Families used:** Hilton, Choice, VIC_evidence
- **Blocked source families:** Marriott
- **Discovered (directory):** 122
- **VIC evidence rows:** 666

## Region plan

- Status: `cala_discovery_region_partial_ready`
- Ready countries: Mexico
- Needs adapter: 38

Autopilot discovery executes only where directory adapters are ready (currently Mexico Hilton + Choice). Other CALA countries require explicit adapter work — no silent guessing.

## Adapter errors

- **Marriott:** listing_adapter_not_ready — Per-property HQV/official page enrichment exists; CALA listing discovery not wired
