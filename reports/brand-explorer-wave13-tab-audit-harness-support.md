# Wave 13 — Tab Audit Harness Support

Generated: 2026-07-27

## Change

Added Wave 13 Stage 4 approved slugs to the tab-factory-audit allowlist in
`lib/partner-intelligence/brand-explorer-tab-factory-audit.js`.

## Supported Wave 13 slugs

- `mama-shelter`
- `mercure`
- `ibis`
- `novotel`
- `pullman`
- `so-hotels-and-resorts`
- `fairmont-hotels-and-resorts`

## Implementation

- Imported `WAVE13_STAGE4_APPROVED_SLUGS` from `brand-explorer-wave13-factory-plan.js`
- Created `WAVE13_AUDIT_ALLOWLIST = new Set(WAVE13_STAGE4_APPROVED_SLUGS)`
- Added `!WAVE13_AUDIT_ALLOWLIST.has(b)` to the brand validation check in `runTabFactoryAudit`

## Audit standards

- No audit standards were weakened
- No required sections were skipped
- Factory-preview brands are not auto-passed
- Same quality bar as public-full profiles applies
- Protected 39 baseline logic remains intact

## Other harnesses checked

- **Rendered field completeness** (`auditBrandRenderedFieldCompleteness`): Only gates on `PROTECTED_BRANDS`, not an allowlist. Wave 13 brands work when passed via `--brands`. No change needed.
- **No-empty rendered components** (`test-brand-explorer-no-empty-rendered-components.mjs`): No allowlist gate. Accepts any brand via `--brands`. No change needed.
- **Golden content quality** (`test-brand-explorer-golden-content-quality.mjs`): No allowlist gate. Accepts any brand via `--brands`. No change needed.
