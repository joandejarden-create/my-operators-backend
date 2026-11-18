# Research vs Staged vs Published contract

| Mode | Query | Source | Filter policy |
|---|---|---|---|
| **Research Preview** | `inventory=research` + `live=1` | Hotel research store (`opportunities.json`) + Bethesda internal union when applicable | Salesperson hygiene only; **no** customer-facing publication filter |
| **Staged Preview** | `inventory=staged` + `preview=staged` | Explicit STAGED package only | Fail-closed if STAGED missing; no CURRENT / live fallback |
| **Published Inventory** | `inventory=published` / default prod | CURRENT publication package | Customer-approved package only |

Every view must surface:

- Inventory source / listScope
- Version or package fingerprint when packaged
- Opportunity count
- Research vs publication state
- Hotel identity

Research Preview must not require STAGED or CUSTOMER_PUBLISHED.
Published must not widen to research.
Staged must not silently become CURRENT or live research.
