# Wave 12 Founder Review

Stage 8 of the Wave 12 factory produces **read-only** founder review packets for the 12 Under Review / factory-preview brands.

## Command

```bash
npm run brand-explorer-wave12-factory -- --stage founder-review --dry-run
```

## Outputs

- `reports/brand-explorer-founder-review-{slug}.md` (×12)
- `reports/brand-explorer-wave12-founder-review-summary.md`

## Recommendations

- `approve_for_status_promotion_and_public_release`
- `approve_after_minor_cleanup`
- `remediation_required`

## Guardrails

- dry-run only — **no Airtable writes**
- no Brand Status / release / Company Validated / Source Library / Registry writes
- no content or image patches
- protected 27 baseline untouched

Last generated: 2026-07-24T22:16:57.332Z

