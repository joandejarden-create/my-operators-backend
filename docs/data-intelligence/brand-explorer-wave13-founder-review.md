# Wave 13 Founder Review

Stage 8 of the Wave 13 factory produces **read-only** founder review packets for the seven Accor factory-preview brands.

## Command

```bash
npm run brand-explorer-wave13-factory -- --stage founder-review --dry-run
```

**No `--apply`.** If `--apply` is passed, the stage throws — founder-review must remain read-only.

## Outputs

- `reports/brand-explorer-founder-review-{slug}.md` (×7)
- `reports/brand-explorer-wave13-founder-review-summary.md`
- `reports/brand-explorer-wave13-founder-review-summary.json`

## Target brands

- `mama-shelter`
- `mercure`
- `ibis`
- `novotel`
- `pullman`
- `so-hotels-and-resorts`
- `fairmont-hotels-and-resorts`

## Recommendations

- `approve_for_status_promotion_and_public_release`
- `approve_after_minor_cleanup`
- `remediation_required`

## Exclusions

- The House of Originals — excluded (Stage 3.5 option C)
- Morgans Originals — not created / not modified
- Radisson Collection — non-target
- Protected 39 — untouched

## Guardrails

- dry-run only — **no Airtable writes**
- no Brand Status / release / Company Validated / Source Library / Registry writes
- no content or image patches
- protected 39 baseline untouched

## Promotion readiness (last run)

- May proceed: **partial_yes_with_holds**
- Held back: so-hotels-and-resorts

Last generated: 2026-07-27T18:09:19.829Z

