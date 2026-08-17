# Operator Explorer source provenance by tab

Validates that each Operator Explorer publishable tab uses an acceptable source mix.

## Hierarchy

1. Operator official website (entity / CALA division)  
2. Operator official regional / services / platforms pages  
3. Operator decks / materials (company-controlled)  
4. Parent / enterprise pages (labeled enterprise context only)  
5. Third-party (supplementary only)

## Canonical rules (quality baselines)

| Operator | Required | Parent context | Allowed third-party examples |
| --- | --- | --- | --- |
| `arbor-lodging-cala` | `arborlodging.com` | (same official domain; label enterprise in copy) | `hotelinvestmenttoday.com` |
| `hotel-equities-cala` | `hotelequities.com` | (same official domain) | — |

Parent/enterprise pages may support platform scale and brand approvals when clearly labeled. They may **not** be the only evidence for CALA positioning, differentiators, leadership, or owner-facing fit.

## Operator-specific tabs

These tabs fail if evidence is third-party-only or missing canonical operator domains:

- Profile & Positioning  
- Operating Platform  
- Brand & Relationships  
- Markets & Footprint  
- Leadership  
- Project Fit & Deal Profile  
- Proof & Track Record  

## Commands

```bash
npm run operator-explorer-source-provenance-by-tab -- --source=fixtures --dry-run
npm run operator-explorer-source-provenance-by-tab -- --source=merged --dry-run
npm run test:operator-explorer-source-provenance-by-tab
```

`--source`:
- `fixtures` — fixture `_meta` URLs + pilot candidates + canonical homepage  
- `live` — Partner Intelligence Source Library rows linked to Master  
- `merged` — fixtures + live PI rows  

Reports: `reports/operator-explorer-source-provenance-by-tab.{json,md}`

## Modules

- `lib/partner-intelligence/operator-explorer-source-provenance-by-tab.js`
- `lib/partner-intelligence/operator-explorer-source-provenance-by-tab-audit.js`

## Related

- `docs/data-intelligence/operator-explorer-mandatory-release-gates.md`
- `docs/data-intelligence/operator-explorer-tab-factory-build-operation.md`
- Brand parallel: `docs/data-intelligence/brand-explorer-source-provenance-by-tab.md`
