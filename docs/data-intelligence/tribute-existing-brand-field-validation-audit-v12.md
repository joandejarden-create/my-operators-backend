# Tribute Existing Brand Field Validation Audit v12

Read-only validation audit for live Tribute Portfolio Brand Setup / Explorer-facing fields.

## Purpose

- Validate existing live field values (not newly staged values).
- Compare against approved Partner Intelligence facts and approved Source Library records.
- Flag weak/generic/wrong-brand/stale/placeholder values.
- Propose safe corrections for a future gated writer (v13), with explicit source basis.
- Keep all Airtable data unchanged in this module.

## Scope

- `Brand Setup - Brand Basics` live Tribute record
- `Brand Setup - Brand Explorer Presentation` rows (with `materials.file` focus)
- `Partner Intelligence - Source Library` approved records for Tribute
- `Partner Intelligence - Extracted Facts` approved facts for Tribute
- Reference-pattern comparison against completed-brand fixtures

## Out Of Scope

- No Airtable writes
- No image changes
- No presentation-row changes
- No Company Validated / Company Validation Date changes
- No implication of Marriott validation

## Command

```bash
npm run tribute-existing-brand-field-validation-audit -- --dry-run
```

## Required Output

- Field-by-field audit table with classification and correction recommendation.
- Explicit Brand Website current vs proposed value.
- Source basis and review status for each proposed correction.
- Separation of source-backed corrections vs human-review-required corrections.
- Confirmation that Airtable remains unchanged.

## Classification Labels

- `correct/source-backed`
- `missing`
- `generic`
- `stale`
- `placeholder/demo`
- `wrong-brand`
- `too broad`
- `not brand-specific enough`
- `source-backed but weakly written`
- `AI-drafted and needing review`

## Future Apply Writer

This module should feed a future gated writer (v13), for example:

```bash
# placeholder for future v13 writer
npm run tribute-existing-brand-field-correction-writer -- --apply --approve-tribute-existing-brand-field-corrections
```

