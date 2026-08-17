# Brand Explorer Portfolio Context Ladder Mapping Repair v25C-4D

- Generated: 2026-07-10T11:06:25.393Z
- Mode: **dry-run**
- Brand: **Tribute Portfolio**

## Root cause

**v25C-4C populated overview.portfolio_context and added partial Marriott frontend mapping; v25C-4D audit still flagged Marriott because visual-display-defect-audit only treated Hilton/Choice as static-ladder parents and Marriott tier-2 peers were not soft-collection focused.**

Data row overview.portfolio_context exists with tier Title=2 and founder-reviewed Body. API exposes it via brand.brandExplorer.blocks. Remaining defect was display-mapping: audit omitted Marriott from usesParentStaticLadder and frontend tier-2 peer list needed soft-collection owner-planning labels (Autograph Collection, Design Hotels, etc.).

## Diagnosis

| Data exists | yes |
| API exposes portfolio context | yes |
| Narrative renders | yes |
| Frontend Marriott mapping | yes |
| Tribute highlighted | yes |
| Marriott sibling labels | yes |
| Generic scale labels | no |

## Ladder simulation

- Tier 0: Fairfield by Marriott, Courtyard by Marriott, Residence Inn, TownePlace Suites
- Tier 1: SpringHill Suites, Four Points, Aloft, AC Hotels
- Tier 2 **(active)**: Tribute Portfolio
- Tier 3: The Ritz-Carlton, St. Regis, W Hotels, The Luxury Collection, Edition

## Summary

| Rows would update | 0 |
| Airtable modified | no |
| Company Validated untouched | yes |

## Exact apply command

```bash
npm run brand-explorer-portfolio-context-ladder-mapping-repair -- --brand tribute-portfolio --apply --approve-brand-explorer-v25C-4D-portfolio-context-ladder-mapping --confirm-marriott-context-is-owner-planning-not-company-validated
```
