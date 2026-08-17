# Brand Explorer — Wave 12 Official Source Packs

Version: `wave12-source-packs-v1`

## Purpose

Stage 3 of the Wave 12 factory builds **official source packs** for 12 Under Review brands before any tab-factory content generation.

## Source hierarchy

1. Brand-specific official brand page
2. Brand-specific development page where available
3. Official property pages (match by **property name**, not array index)
4. Official parent-company pages — **parent/platform context only**
5. Credible announcement / opening / development sources
6. Image sources tied to official brand or property pages

## Geography labels

- **CALA** — Caribbean & Latin America examples preferred when available
- **International Reference** — non-CALA examples explicitly labeled

## Target Guest Segments

Do not combine Luxury / Discerning with Leisure (or Experience-Oriented adjacency that renders as generic audience prose). Prefer brand-specific Bleisure / Experience-Oriented / Leisure / International Inbound only when source-supported.

Stage 3 records recommendations only. Do not patch Brand Basics until an approved later stage.

## Command

```bash
npm run brand-explorer-wave12-factory -- --stage source-packs --dry-run
```

## Outputs

- `reports/brand-explorer-wave12-source-pack-<slug>.md` (12 files)
- `reports/brand-explorer-wave12-source-pack-summary.{json,md}`
- This doc

## Next stage

Stage 4 — `tab-factory-build` (only after source pack review).

