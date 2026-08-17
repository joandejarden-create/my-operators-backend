# Production Census View Cleanup

**Status:** `production_census_views_manual_ui_steps_needed`
**Generated:** 2026-08-05T11:24:51.339Z

## Summary

- Four founder views exist on Hotel Property Census.
- Airtable Meta API **cannot** update existing view field visibility, order, or filters (PATCH/PUT → 404).
- Exact manual Hide fields + column order instructions are in the apply report.
- Field aliases: `Brand` → `Current Brand`; `Source Family` → `Family / Source Family`.

## Views

### Census - Core Identity
Show 13 fields (hide 82). Currently matches target: false
### Census - Enrichment
Show 23 fields (hide 72). Currently matches target: false
### Census - Owner Operator
Show 15 fields (hide 80). Currently matches target: false
### Census - Steward Review
Show 14 fields (hide 81). Currently matches target: false

## Steward Review filter (manual)

Filter Census - Steward Review where Human Review Required is checked. Expect **4** records.

## Safety

- No schema writes, no record writes, no Brand Explorer writes.
- Census validation pass: true
- Brand Explorer all_pass: true

## Next

Complete the manual Hide fields + column order steps for all four views (and Steward Review filter). Then freeze the Census field contract and begin descriptions + amenities enrichment.
