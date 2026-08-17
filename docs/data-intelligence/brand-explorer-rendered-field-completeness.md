# Brand Explorer rendered field completeness

Every visible owner-facing field must resolve to one of:

- complete  
- intentionally suppressed  
- cleanly marked unavailable  
- included in an unapplied patch plan  

`auditPass` requires `failFindings === 0`. See Tab Factory build operation for the permanent process.

```bash
npm run brand-explorer-rendered-field-completeness-audit -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --dry-run
npm run test:brand-explorer-rendered-field-completeness -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world
```

Related: `docs/data-intelligence/brand-explorer-tab-factory-build-operation.md`, `docs/data-intelligence/brand-explorer-rendered-field-completeness-audit.md`.
