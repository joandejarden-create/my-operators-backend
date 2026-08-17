# Active Brand Completion Reconciliation

Read-only audit reconciling all historically active Brand Explorer brands against PVQL / Tab Factory.

## Run

```bash
npm run brand-explorer-active-brand-completion-reconciliation -- --dry-run
```

## Outputs

- `reports/brand-explorer-active-brand-completion-reconciliation.json`
- `reports/brand-explorer-active-brand-completion-reconciliation.md`
- `reports/brand-explorer-active-brand-inventory.md`
- `reports/brand-explorer-active-brand-restore-candidates.md`
- `reports/brand-explorer-active-brand-remediation-plan.md`

## Rules

- No Airtable writes
- No Company Validated / Source Library / Registry / release field changes
- No content rewrites during audit
- Historically done brands must not be silently treated as incomplete

Latest run: see reports (generated 2026-07-22T21:05:49.718Z).

Inventory size: **23** · public-full clean: **11** · ready to restore: **0**