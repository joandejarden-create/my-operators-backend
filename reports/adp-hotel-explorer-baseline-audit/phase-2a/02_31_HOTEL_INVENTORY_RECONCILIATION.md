# Phase 2A — 31-hotel inventory reconciliation

**Canonical freeze:** `lib/ai-demand-positioning/governance/canonical-adp-31-property-scope-v1.js`  
**Gate:** `npm run test:adp-canonical-31-scope-v1` → `{"ok":true,"hotels":31,"bethesda":true}`  
**Posture:** `npm run test:adp-canonical-31-certification-posture-v1` → identityCount 31, **certifiedCount 0**, `privateMatrixPresent: false`

Founder previously authorized **all 31 discovered ADP hotels including Bethesda** in Full E2E scope. This reconciliation does **not** drop hotels for incomplete evidence.

## Counts

| Universe | Count |
|----------|------:|
| Canonical ADP 31 | 31 |
| FS published (`data/ai-demand-positioning/published/`) | 27 |
| In canonical 31 but **not** published locally | 5 |
| Published locally but **not** in canonical 31 | 1 (`adp_radisson_blu_toronto_downtown`) |

## In scope, not in local published FS

| Property ID | Next action (no silent drop) |
|-------------|------------------------------|
| `adp_charlotte_marriott_city_center` | Locate remote published/durable package or schedule authorized Full E2E |
| `adp_falls_church_marriott_fairview_park` | Same |
| `adp_nobu_london_portman` | Same (may exist in other worktrees / Railway volume) |
| `adp_renaissance_arlington_capital_view` | Same |
| `adp_tampa_airport_marriott` | Same |

## Published but outside freeze

| Property ID | Note |
|-------------|------|
| `adp_radisson_blu_toronto_downtown` | Separate Toronto closeout track — keep out of “certified 31” claims unless freeze revised |

## Bethesda (in freeze + published)

| Field | Value |
|-------|-------|
| ADP | `adp_bethesda_marriott` |
| HPC | `recLuxvwwxID7U2B8` |
| Publish status | `Live` |
| Latest period | `adp_period_adp_bethesda_marriott_20261002142154_62428a` |
| Evidence file | Present (~1.3MB; not empty) |
| Ledger posture | `COMPATIBILITY_ONLY` / `force_internal` (local Phase 1.3 identity work) |

## Discrepancy rule for Phase 2A

- **Inventory authority** = canonical 31 list (membership).  
- **Publication authority** = published manifests + founder publication auth (not implied by membership).  
- **Certification authority** ≠ publication; private certification matrix currently **absent** → certifiedCount 0.  
- Missing published FS rows for 5 hotels = **coverage gap**, not removal from scope.
