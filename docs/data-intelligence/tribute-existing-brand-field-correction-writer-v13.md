# Tribute Existing Brand Field Correction Writer v13

Narrow correction writer for source-backed updates to **existing** live Tribute Brand Setup fields.

## Purpose

- Apply high-confidence, source-backed corrections only.
- Use `v12` validation audit as source of truth.
- Keep scope intentionally narrow (no profile rewrite).

## Current Scope (v13)

- `Brand Setup - Brand Basics`:
  - `Brand Website` only

## Explicitly Out Of Scope

- No image changes
- No `Brand Setup - Brand Explorer Presentation` row edits
- No `materials.file` / sourceLinks changes
- No hero/gallery/value-driver/recent-openings changes
- No AI-drafted field writes
- No writes to:
  - `Brand Profile Analysis`
  - `Brand Standards`
  - `Questions Owners Should Ask`
  - `Company Validated`
  - `Company Validation Date`

## Commands

Dry-run:

```bash
npm run tribute-existing-brand-field-correction-writer -- --dry-run
```

Apply (gated):

```bash
npm run tribute-existing-brand-field-correction-writer -- --apply --approve-tribute-existing-field-corrections
```

## Required Evidence Checks

Before proposing/writing `Brand Website`, v13 verifies:

- URL appears in approved Tribute Source Library records, and
- URL aligns with `v12` correction recommendation, and
- URL is present in `materials.file` source links context.

