# Tribute Brand Explorer Content Promotion Writer v11

**Status:** Targeted content promotion writer for Tribute Portfolio  
**Module:** `lib/partner-intelligence/tribute-brand-explorer-content-promotion-writer.js`  
**Script:** `npm run tribute-brand-explorer-content-promotion-writer -- --dry-run`

## Purpose

Promote human-reviewed Explorer-facing copy for the v10 core gaps:

| Normalized key | Display label | Live Airtable target |
|--------------|---------------|----------------------|
| `idealAssetProfile` | Brand Profile Analysis | `Brand Setup - Brand Explorer Presentation` · slot `overview.typical_use_case` · **Body** |
| `standards` | Brand Standards | `Brand Setup - Brand Explorer Presentation` · slot `standards.intro` · **Body** |
| `questionsOwnersShouldAsk` | Questions Owners Should Ask | `Brand Setup - Brand Explorer Presentation` · slot `standards.questions` · **Body** |

**Not** Brand Setup - Brand Basics columns — those display labels are not present in live Airtable.

`sourceLinks` / `materials.file` are **out of scope** (already promoted).

## Commands

```bash
npm run tribute-brand-explorer-content-promotion-writer -- --dry-run
```

Apply (after dry-run preflight passes):

```bash
npm run tribute-brand-explorer-content-promotion-writer -- --apply --approve-tribute-brand-explorer-content-promotion --allow-human-review-copy
```

## Required behavior

- Dry-run by default.
- Meta API schema preflight before apply; apply blocked if any target field is missing.
- Dry-run report shows normalized key, display label, table, field name, field ID, current value, proposed value, writable flag.
- Creates presentation rows when slot rows are missing; patches **Body** only when rows exist.
- Uses `reports/tribute-brand-explorer-content-parity-audit.json` (v10) as gap source of truth.

## Guardrails

- No image / media attachment writes.
- No hero, gallery, value-driver, or recent-openings rewrites.
- No `materials.file` / sourceLinks writes.
- No Brand Website writes.
- No Company Validated / Company Validation Date writes.
- No Marriott validation implication language.
- Does not create new Airtable columns.

## Outputs

- `reports/tribute-brand-explorer-content-promotion-writer.md`
- `reports/tribute-brand-explorer-content-promotion-writer.json`
