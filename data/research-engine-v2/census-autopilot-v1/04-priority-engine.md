# Priority Engine

Score = **BUSINESS_RELEVANCE × MATERIAL_INCOMPLETENESS × STALENESS × CROSS_TABLE_RISK × RESEARCHABILITY**

## Factors

- Mexico / CALA importance
- Active Dealality opportunity relevance (ctx flag)
- Brand Explorer activation value (ctx flag)
- Pipeline / opening status
- Missing critical / material fields
- Unresolved cross-table contradiction
- Source availability (page_source_state)
- Last verified date

## Bands

| Band | Meaning |
|------|---------|
| P0 Critical | Highest product + gap + researchability |
| P1 High | Strong candidates for next research |
| P2 Medium | Normal backlog |
| P3 Low | Defer |

Not alphabetical. Implementation: `lib/research-engine-v2/census-autopilot-v1/priority-engine.js`.
