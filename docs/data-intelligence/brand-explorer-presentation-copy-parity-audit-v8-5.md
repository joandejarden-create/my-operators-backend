# Brand Explorer Presentation Copy Parity Audit v8.5

**Status:** Read-only copy parity audit for Brand Explorer presentation rows  
**Module:** `lib/partner-intelligence/brand-explorer-presentation-copy-parity-audit.js`  
**Script:** `npm run brand-explorer-presentation-copy-parity-audit -- --brand tribute-portfolio --dry-run`

## Purpose

Audit Tribute Portfolio presentation copy against completed reference standards (Radisson Blu by Choice, Radisson by Choice, Kimpton Hotels, Curio Collection by Hilton) before introducing a future writer module.

This module does not write Airtable and does not modify images.

## v9 handoff

Use this audit output as the recommendation source for:

- `npm run brand-explorer-presentation-copy-promotion-writer -- --brand tribute-portfolio --dry-run`

The v9 writer reads `reports/brand-explorer-presentation-copy-parity-audit.json` and applies copy-only updates to existing promoted slots when explicitly approved.

## Value-driver parity note

For Tribute `overview.scenario.*`, the parity standard is strategic and owner-facing:

- No property names in value-driver title/body.
- No "Value Driver — [Hotel Name]" framing.
- Scenario visuals can stay property-based, but copy must remain brand-level.

If targeted remediation is needed without touching hero/gallery copy, run:

```bash
npm run brand-explorer-value-driver-copy-parity-fix -- --brand tribute-portfolio --dry-run
```

## v10 full parity companion

Use v10 for full-profile parity and completion planning (beyond promoted presentation rows):

```bash
npm run tribute-brand-explorer-content-parity-audit -- --dry-run
```

v8.5 remains the focused promoted-slot copy audit that feeds v9.

## Command

```bash
npm run brand-explorer-presentation-copy-parity-audit -- --brand tribute-portfolio --dry-run
```

## Scope

- Reads Tribute Brand Setup + Brand Explorer Presentation rows.
- Reads reference copy materials from existing Brand Explorer fixtures.
- Audits promoted slots and intentionally missing slots.
- Flags style and structure mismatches.
- Produces proposed Tribute copy model with explicit classification:
  - source-backed copy
  - AI-drafted owner-facing copy
  - human-review copy
  - hold-blank fields

## Outputs

- `reports/brand-explorer-presentation-copy-parity-audit.md`
- `reports/brand-explorer-presentation-copy-parity-audit.json`

## Guardrails

- No Airtable writes.
- No Brand Setup writes.
- No presentation row writes.
- No image/media changes.
- No Company Validated field writes.
- No Marriott validation claims.
