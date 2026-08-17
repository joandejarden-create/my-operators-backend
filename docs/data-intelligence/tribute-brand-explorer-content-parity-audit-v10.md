# Tribute Full Brand Explorer Content Parity Audit v10

**Status:** Read-only full parity audit for Tribute Portfolio  
**Module:** `lib/partner-intelligence/tribute-brand-explorer-content-parity-audit.js`  
**Script:** `npm run tribute-brand-explorer-content-parity-audit -- --dry-run`

## Purpose

Run a full content/data parity assessment for Tribute Portfolio against completed Brand Explorer profiles (Radisson Blu by Choice, Radisson by Choice, Kimpton Hotels, Curio Collection by Hilton, Ascend Hotel Collection).

This is a content parity and completion audit only. It does not write Airtable, does not change images/media, and does not update Brand Setup fields.

## Command

```bash
npm run tribute-brand-explorer-content-parity-audit -- --dry-run
```

## Scope

- Reads Tribute live Brand Basics + Brand Explorer Presentation rows.
- Reads Tribute approved PI sources/facts for source-backed grounding.
- Reads completed reference fixtures and attempts live readback for reference brands where available.
- Compares Tribute section-by-section and field-by-field against completed-brand patterns.
- Classifies each area as:
  - Complete/comparable
  - Present but wrong style
  - Generic/demo-like
  - Missing
  - Source-backed but not promoted
  - Should remain blank until stronger evidence
- Produces staging-only proposed content updates and v11 readiness recommendation.

## Audited areas

- Hero / overview positioning
- Brand identity and parent company
- Brand family / collection context
- Segment / chain scale
- Brand promise
- Positioning summary
- Guest / owner value proposition
- Ideal hotel / asset profile
- Conversion / adaptive reuse fit
- Development model
- Loyalty / Marriott Bonvoy relationship
- Market / regional relevance
- Where This Brand Creates the Most Value
- Owner considerations
- Questions owners should ask
- Brand standards / owner considerations
- PDF/source links
- Image gallery titles/captions
- Recent openings / PR
- Data gaps / caveats
- External trust chip / source basis

## Outputs

- `reports/tribute-brand-explorer-content-parity-audit.md`
- `reports/tribute-brand-explorer-content-parity-audit.json`

## Guardrails

- Dry-run/read-only only.
- No Airtable writes.
- No Brand Setup writes.
- No presentation writes.
- No image/media changes.
- No Company Validated / Company Validation Date writes.
- No Marriott validation claims.
