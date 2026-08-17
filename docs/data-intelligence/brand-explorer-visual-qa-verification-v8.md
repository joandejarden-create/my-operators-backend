# Brand Explorer Visual QA Verification v8

**Status:** Read-only verification module for Explorer visual readiness  
**Module:** `lib/partner-intelligence/brand-explorer-visual-qa-verification.js`  
**Script:** `npm run brand-explorer-visual-qa-verification -- --brand tribute-portfolio --dry-run`

## Purpose

Verify end-to-end that promoted Tribute Portfolio media is readable by the Brand Explorer pipeline after:

- v6.1 attachment materialization repair
- v7 presentation image patch

This verifier is read-only and never writes Airtable.

## Command

```bash
npm run brand-explorer-visual-qa-verification -- --brand tribute-portfolio --dry-run
```

## Checks performed

1. Reads Tribute `Brand Setup - Brand Basics` record.
2. Reads Tribute `Brand Setup - Brand Explorer Presentation` rows.
3. Confirms expected promoted slots exist.
4. Confirms each expected slot has `Image` attachment count > 0.
5. Confirms each expected slot has readable `imageUrl`.
6. Confirms slot image URL maps to approved Brand Asset Registry records.
7. Flags unapproved/unexpected promoted slots.
8. Confirms Brand Setup logo was not overwritten by this workflow.
9. Confirms Company Validated fields are untouched by this workflow.
10. Confirms Tribute remains text/governance platform-ready in reporting context.
11. Confirms normalized API output shape includes promoted slot images.
12. Confirms frontend slot/imageUrl consumption expectations from explorer JS.

## Outputs

- `reports/brand-explorer-visual-qa-verification.md`
- `reports/brand-explorer-visual-qa-verification.json`

## Guardrails

- No Airtable writes.
- No Brand Setup edits.
- No presentation record edits.
- No logo overwrite.
- No image downloads.
- No approvals or governance status changes.

## Relationship to copy parity

v8 verifies image readiness and slot visibility only. Copy parity and copy writes are handled separately by:

- v8.5 audit: `brand-explorer-presentation-copy-parity-audit`
- v9 writer: `brand-explorer-presentation-copy-promotion-writer`
