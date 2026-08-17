# Remaining Brands — Lane 2 (Full Tab Factory Build)

- Generated: 2026-07-23T00:10:09.509Z
- Brands: autograph-collection, handwritten-collection, radisson-collection, tapestry-collection-by-hilton, vignette-collection
- Mode: **APPLY**
- Applied: **true**

## Summary

- Planned presentation writes: 418
- Blocked brands: —
- Release fields written: **false** (confirm-no-release-field-writes)
- Active release: **not performed** (requires separate founder restore/release)

## Per brand

- **autograph-collection**: rows=84 planned=83 blocked=false — see `reports/brand-explorer-full-build-autograph.md`
- **handwritten-collection**: rows=84 planned=84 blocked=false — see `reports/brand-explorer-full-build-handwritten.md`
- **radisson-collection**: rows=84 planned=84 blocked=false — see `reports/brand-explorer-full-build-radisson-collection.md`
- **tapestry-collection-by-hilton**: rows=84 planned=83 blocked=false — see `reports/brand-explorer-full-build-tapestry.md`
- **vignette-collection**: rows=84 planned=84 blocked=false — see `reports/brand-explorer-full-build-vignette.md`

## Validation commands (after apply)

```bash
npm run brand-explorer-tab-factory-audit -- --brands autograph-collection,handwritten-collection,radisson-collection,tapestry-collection-by-hilton,vignette-collection --dry-run
npm run test:brand-explorer-rendered-field-completeness -- --brands autograph-collection,handwritten-collection,radisson-collection,tapestry-collection-by-hilton,vignette-collection
npm run test:brand-explorer-no-empty-rendered-components -- --brands autograph-collection,handwritten-collection,radisson-collection,tapestry-collection-by-hilton,vignette-collection
npm run brand-explorer-source-provenance-by-tab -- --brands autograph-collection,handwritten-collection,radisson-collection,tapestry-collection-by-hilton,vignette-collection --dry-run
npm run brand-explorer-image-uniqueness-audit -- --brands autograph-collection,handwritten-collection,radisson-collection,tapestry-collection-by-hilton,vignette-collection --dry-run
npm run brand-explorer-image-role-match-audit -- --brands autograph-collection,handwritten-collection,radisson-collection,tapestry-collection-by-hilton,vignette-collection --dry-run
npm run test:brand-explorer-section-pattern-parity -- --brands autograph-collection,handwritten-collection,radisson-collection,tapestry-collection-by-hilton,vignette-collection
npm run brand-explorer-os -- --brands autograph-collection,handwritten-collection,radisson-collection,tapestry-collection-by-hilton,vignette-collection --stage release-readiness --dry-run --skip-regression
```

## Apply flags

- `--approve-full-tab-factory-build`
- `--confirm-no-company-validation-changes`
- `--confirm-no-source-library-status-changes`
- `--confirm-no-registry-approval-changes`
- `--confirm-no-release-field-writes`
- `--confirm-tab-factory-contracts`
- `--confirm-source-provenance-by-tab`
- `--confirm-image-uniqueness`
- `--confirm-image-role-match`
- `--confirm-section-pattern-parity`

