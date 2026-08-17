# Brand Explorer Scenario Display Parity Audit v31H

Compares how `overview.scenario.1` is stored, exposed by the API, and rendered for two brands.

**Command**

```bash
npm run brand-explorer-scenario-display-parity-audit -- --left radisson --right radisson-individuals-by-choice --slot overview.scenario.1 --dry-run
```

**Reports**

- `reports/brand-explorer-scenario-display-parity-audit.md`
- `reports/brand-explorer-scenario-display-parity-audit.json`

## Scope

| Side | Brand | Slug | Record ID |
|------|-------|------|-----------|
| Left | Radisson by Choice | `radisson` | `recywbx1YQSTCPqW1` |
| Right | Radisson Individuals by Choice | `radisson-individuals-by-choice` | `recRyvM8OmLlDj9G7` |

Audit only — no Airtable writes, no image approvals, no materialization, no Company Validated changes.

## v31H finding (2026-07-10)

**Primary root cause:** Radisson Individuals `overview.scenario.1` (`recpe1vIxIsaKq1XX`) has **External Display Status = Do Not Display**. The API excludes quarantined rows from `brand.brandExplorer.blocks[]`, so the slot is absent from the API payload. The atelier scenario grid then falls back to the blank `scenario-card__visual--empty` placeholder.

**Not the cause:** Missing image attachment (both brands have an Image attachment in Airtable), brand-specific frontend logic, or expansion slug/record ID mismatch.

**Secondary gaps:** No Brand Asset Registry row linked to `overview.scenario.1` on either brand; expansion-brand image governance requires registry approval before active-profile evidence.

## Recommended sequence

1. **P1 — Clear quarantine** if copy/image are founder-ready: set External Display Status to visible (not Do Not Display).
2. **P2 — Registry + approval:** create/link registry asset for `overview.scenario.1`, founder-approve, then materialize if needed.
3. **Optional UI:** hide empty scenario visual shell when slot is text-only eligible (uniform patch, not Radisson-specific).
