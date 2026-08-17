# Brand Explorer — Wave 13 Open Items Resolution

Version: `brand-explorer-wave13-open-items-resolution-v1`

## Purpose

Stage 3.5 resolves prerequisites before Stage 4 tab-factory-build:

1. Create SO/ Brand Basics (Under Review only)
2. Document Fairmont naming (no rename)
3. Founder/manual review for The House of Originals

## Schema notes

- Table: `Brand Setup - Brand Basics`
- Allowed create fields: Brand Name, Brand Status, Parent Company, Internal Notes
- No Slug field and no display-alias field on Brand Basics — slug/display alias are code-side / notes
- Brand Status option used: `Under Review` (Active/Live forbidden here)

## Stage 4 recommendation

Proceed with **seven brands excluding The House of Originals** after SO/ Basics exists (recommendation **C**).

## Command

```bash
npm run brand-explorer-wave13-factory -- --stage open-items-resolution --dry-run
```
