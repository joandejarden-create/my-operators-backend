# Brand Explorer Presentation Copy Promotion Writer v9

**Status:** Gated copy-only writer for Tribute Portfolio presentation slots  
**Module:** `lib/partner-intelligence/brand-explorer-presentation-copy-promotion-writer.js`  
**Script:** `npm run brand-explorer-presentation-copy-promotion-writer -- --brand tribute-portfolio --dry-run`

## Purpose

Promote approved v8.5 copy recommendations into existing Explorer presentation rows for Tribute Portfolio, without touching media, Brand Setup fields, or registry records.

## Command

```bash
npm run brand-explorer-presentation-copy-promotion-writer -- --brand tribute-portfolio --dry-run
```

## Apply gate

```bash
npm run brand-explorer-presentation-copy-promotion-writer -- --brand tribute-portfolio \
  --apply --approve-brand-explorer-copy-promotion
```

Optional overwrite for polished nonblank copy:

```bash
--allow-nonblank-copy-overwrite
```

## Scope

- Reads v8.5 parity recommendations from `reports/brand-explorer-presentation-copy-parity-audit.json`.
- Reads live Tribute rows in `Brand Setup - Brand Explorer Presentation`.
- Operates only on existing promoted slots:
  - `overview.hero`
  - `materials.gallery.1`
  - `materials.gallery.2`
  - `materials.gallery.4`
  - `materials.gallery.5`
  - `materials.gallery.6`
  - `overview.scenario.1`
  - `overview.scenario.2`
- Proposes updates to `Title` and `Body` only.

## Value-driver scope note

When only Tribute value-driver parity (`overview.scenario.1` / `.2`) needs correction, prefer the focused module:

```bash
npm run brand-explorer-value-driver-copy-parity-fix -- --brand tribute-portfolio --dry-run
```

This avoids broad nonblank-copy overwrite decisions on hero/gallery slots.

## v10 prerequisite note

Before proposing any broader content promotion writer (v11), run:

```bash
npm run tribute-brand-explorer-content-parity-audit -- --dry-run
```

v10 identifies section-level completion gaps and staged copy candidates outside the narrow v9 promoted-slot scope.

## Guardrails

- Dry-run by default.
- No image field writes.
- No Brand Setup writes.
- No logo writes.
- No new slot row creation.
- No registry record writes.
- No Company Validated / Company Validation Date writes.
- No copy that implies Marriott validation.
- Missing slots remain blank (`materials.gallery.3`, `overview.scenario.3`, `footprint.openings`, boutique/mixed-use scenarios, PR/Openings link).

## Overwrite behavior

- Blank copy may be filled.
- Generic/demo/asset-registry copy may be replaced by default.
- Nonblank polished copy is protected unless `--allow-nonblank-copy-overwrite` is provided.
- All potential overwrites are listed in the report.

## Outputs

- `reports/brand-explorer-presentation-copy-promotion-writer.md`
- `reports/brand-explorer-presentation-copy-promotion-writer.json`

