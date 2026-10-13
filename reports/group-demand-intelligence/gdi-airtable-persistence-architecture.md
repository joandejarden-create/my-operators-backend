# GDI Airtable persistence architecture

**Status:** Live on canonical intelligence base `appa2cE7FTRmIbB32`  
**Date:** 2026-09-17 (cutover from Deal Capture MVP `appvtnDurnMSjINP6`)

## Canonical entities

| Entity | Table | Base |
|--------|-------|------|
| Group Demand Opportunities | `Group Demand Opportunities` (`tblRuReslJMwsfRQj`) | `appa2cE7FTRmIbB32` |
| Decisions | `Decisions` (`tblulPWvEmd3iiDhJ`) | same |
| Decision Events | `Decision Events` (`tblLL1CuKpvI43DtB`) | same |

Config: `AIRTABLE_GDI_BASE_ID` / `AIRTABLE_INTELLIGENCE_BASE_ID` — see `lib/decision-outcomes/airtable-base.js`.  
MVP blocked by `WRONG_CANONICAL_AIRTABLE_BASE`.

## Relationship

```
hotelId
  → Group Demand Opportunities (opportunityId)
      → Decisions (subjectId = opportunityId, productModule = GDI)
          → Decision Events (VALIDATION | ACTION | OUTCOME)
```

ADP uses the **same** Decisions + Decision Events tables.

## Filesystem role

Mirror + eval-only freezes. Live SoT is Airtable on `appa2cE7FTRmIbB32`.

## Scripts

```bash
npm run ensure:gdi-opportunities-airtable-schema -- --apply
node scripts/migrate-canonical-airtable-mvp-to-intelligence-base.mjs --apply
node scripts/verify-canonical-airtable-cutover.mjs
```
