# Brand Explorer Portfolio Mix + Portfolio Context Normalization Writer v25C-4C

- Generated: 2026-07-09T22:15:48.650Z
- Mode: **dry-run**
- Brand: **Tribute Portfolio**

## Portfolio Mix audit

| Brand | Classification | Rows |
|-------|----------------|------|
| Tribute Portfolio | radisson_style_complete | 5 |
| Curio Collection by Hilton | radisson_style_complete | 5 |
| Kimpton Hotels | radisson_style_complete | 5 |
| Radisson Blu by Choice | radisson_style_complete | 5 |
| Radisson by Choice | radisson_style_complete | 5 |
| Ascend Hotel Collection | radisson_style_complete | 5 |

## Tribute Portfolio Context root cause

- **frontend_mapping_issue**

overview.portfolio_context row exists (v25C-4A) with tier Title and Body, but Portfolio Context on Overview tab only renders buildPortfolioLadderCellsHtml() — Body is not displayed in that section. Marriott brands also lack static sibling ladder mapping (unlike Choice/Hilton), so inactive ladder steps show generic labels only.

## Tribute proposed Portfolio Mix chips

- **Urban Lifestyle** — High
- **Resort / Leisure-Adjacent** — Moderate
- **Conversion / Repositioning** — High
- **Secondary Market** — Selective
- **New Build Prototype-Led** — Low

## All-brand proposed Portfolio Mix chips

### Tribute Portfolio

- **Urban Lifestyle** — High
- **Resort / Leisure-Adjacent** — Moderate
- **Conversion / Repositioning** — High
- **Secondary Market** — Selective
- **New Build Prototype-Led** — Low

### Curio Collection by Hilton

- **Urban** — High
- **Leisure / Resort-Adjacent** — High
- **Secondary Market** — Moderate
- **New Build Prototype-Led** — Low
- **Conversion / Repositioning** — High

### Kimpton Hotels

- **Urban** — High
- **Leisure / Resort-Adjacent** — Moderate
- **Secondary Market** — Selective
- **New Build Prototype-Led** — Low
- **Conversion / Repositioning** — High

### Ascend Hotel Collection

- **Urban** — Moderate
- **Leisure / Resort-Adjacent** — Moderate
- **Secondary Market** — High
- **New Build Prototype-Led** — Low
- **Conversion / Repositioning** — High


## Summary

| Rows would create | 0 |
| Rows would update | 0 |
| Unsupported % removed | yes |
| Airtable modified | no |

## Exact apply command

```bash
npm run brand-explorer-portfolio-mix-context-normalization-writer -- --all-active --apply --approve-brand-explorer-v25C-4C-all-active-portfolio-mix-normalization --founder-reviewed-all-active-portfolio-mix-copy --confirm-no-unsupported-portfolio-statistics
```
