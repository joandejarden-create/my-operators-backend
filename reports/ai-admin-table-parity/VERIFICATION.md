# AI Admin table presentation — verification

## Defect reproduced

Legacy fixture `before-open-menu-obscured.png`: Hotel 5 More is expanded (`<details open>`) but menu items are not visible — neighboring sticky Actions cells paint over the in-cell absolute menu.

## After fix

| Artifact | Proof |
|----------|-------|
| `after-open-menu-unobscured.png` | Portal menu fully visible with icons + disabled why-line |
| `after-open-menu-last-row.png` | Bottom-row flip stays in viewport |
| `after-open-menu-narrow.png` | Narrow viewport |
| `after-open-menu-zoom200.png` | Scaled layout |
| `more-menu-layering-proof.json` | `elementFromPoint` hits menu; archive click fires `data-act` |

Commands:

```bash
node scripts/test-admin-ai-demand-table-v1.mjs
node scripts/playwright-ai-admin-more-menu-layering-v1.mjs
```

## Tabs covered by shared `actionsHtml`

- AI Demand Reviews
- GDI Reports
- Report Archive

ADP Action Plan: overflow shell only (no row More).

## Non-goals confirmed

- No research runs, Airtable mutations, or count formula changes during QA
