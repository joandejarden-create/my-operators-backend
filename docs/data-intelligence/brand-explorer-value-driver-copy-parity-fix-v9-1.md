# Brand Explorer Value-Driver Copy Parity Fix v9.1

**Status:** Focused copy parity fixer for Tribute value-driver rows  
**Module:** `lib/partner-intelligence/brand-explorer-value-driver-copy-parity-fix.js`  
**Script:** `npm run brand-explorer-value-driver-copy-parity-fix -- --brand tribute-portfolio --dry-run`

## Purpose

Align Tribute Portfolio's **Where This Brand Creates the Most Value** copy with completed-brand standards:

- Strategic, owner/developer-facing value drivers.
- No property names in user-facing title/body.
- Images remain supporting visuals only.

This module targets only:

- `overview.scenario.1` (Resort)
- `overview.scenario.2` (Urban)

## Command

```bash
npm run brand-explorer-value-driver-copy-parity-fix -- --brand tribute-portfolio --dry-run
```

## Apply gate

```bash
npm run brand-explorer-value-driver-copy-parity-fix -- --brand tribute-portfolio \
  --apply --approve-brand-explorer-value-driver-copy-fix
```

## Scope

- Reads parity recommendations from `reports/brand-explorer-presentation-copy-parity-audit.json`.
- Reads live Tribute rows from `Brand Setup - Brand Explorer Presentation`.
- Proposes updates to `Title` and `Body` only for `overview.scenario.1` and `.2`.
- Leaves all other slots unchanged.

## Guardrails

- Dry-run by default.
- No image/attachment updates.
- No Brand Setup field updates.
- No logo updates.
- No creation of missing slots.
- No Company Validated / Company Validation Date changes.
- No Marriott-validation claims in user-facing copy.

## Outputs

- `reports/brand-explorer-value-driver-copy-parity-fix.md`
- `reports/brand-explorer-value-driver-copy-parity-fix.json`
