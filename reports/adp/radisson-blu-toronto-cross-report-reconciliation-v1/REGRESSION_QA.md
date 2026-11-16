# REGRESSION QA

Shared calculation / UI changes:

1. **Web Reality Gap** — prefer published `gapScore` (do not recompute binary `gaps.length/total`).
2. **Executive Read** — prefer published `realityGap.coveragePercent` over live profile recompute.
3. **Performance Review KPIs** — include Top-3 + Tracked Attributes from published metrics; do not truncate territories.

Control hotels (published Reality Gap unchanged; no history rewrite):

| Hotel | Published gapScore | Notes |
|-------|--------------------|-------|
| Bethesda Marriott | 80 | stored value unchanged |
| Waterstone Resort & Marina | 61.1 | unchanged |
| Wyndham San Diego Bayside | 91.7 | unchanged |
| Mayflower Hotel | no published snapshot in this checkout | N/A |

Raw observations / Core periods: **not rewritten**.
