# Brand Explorer v36D Action Router

Generic action router + remediation executor (dry-run by default).

## Modules

| Module | Path |
|--------|------|
| Action router | `lib/partner-intelligence/brand-explorer-action-router.js` |
| Remediation executor | `lib/partner-intelligence/brand-explorer-remediation-executor.js` |
| Patch builder | `lib/partner-intelligence/brand-explorer-remediation-patch-builder.js` |
| Apply gate enforcer | `lib/partner-intelligence/brand-explorer-apply-gate-enforcer.js` |

## Run

```bash
npm run brand-explorer-v36d-action-router -- --brands design-hotels,small-luxury-hotels-of-the-world,tribute-portfolio,woodspring-suites,everhome-suites --dry-run
```

Requires prior: `reports/brand-explorer-v36c-remediation-planner.json`

See `reports/brand-explorer-v36d-action-router.md`.
