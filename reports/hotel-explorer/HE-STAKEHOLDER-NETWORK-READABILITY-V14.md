# HE Stakeholder Network Readability & Progressive Disclosure — v14

**Date:** 2026-11-19  
**PR:** #463  
**Lesson:** HE-QL-026 · Quality Standard v1.15 §9a  
**Airtable / paid / deploy:** none

## Verdict

The Ownership graph is now a hotel-centered **Stakeholder & Deal Relationship Network** with readable wrapped nodes, ownership-above / ops-brand-below layout, progressive Explore Key People / Related Hotels (bounded), short edge tags, and an organization detail panel. Unbounded “Expand sponsor hotels” is removed.

## Default view (Bethesda)

- Nodes: Bethesda Marriott · Marbeth Acquisition · Mullen Real Estate Capital · Marriott International · Marriott Hotels  
- People / portfolio: **not** auto-loaded  
- Controls: Fit · Reset · Brand Development view · Explore Key People · Explore Related Hotels  

## Progressive disclosure

| Action | Result |
|---|---|
| Explore Key People | Jeff Mullen, Katherine Button, Patrick Reece (capped) |
| Select Mullen | Detail panel with type, status, expand actions, connections |
| Explore Related Hotels | Bounded related-hotel cluster (≤8) under ops lane |
| Brand Development view | Filters to brand-dev-relevant edge set |

Capital / financing / advisor nodes remain **unresolved** unless evidence-backed parties exist — none invented for Bethesda.

## Evidence

`reports/hotel-explorer/phase4-quality/phase4-founder-preview-screenshots/`

- `quality-v14-ocn-default.png`
- `quality-v14-ocn-people.png`
- `quality-v14-ocn-detail.png`
- `quality-v14-ocn-hotels.png`

## Gates

```bash
npm run test:he-ownership-control-network-v1
npm run test:he-product-quality-gates-v1
```

## Persistence

Presentation/layout only over existing HI ownership payload — no Airtable apply.
