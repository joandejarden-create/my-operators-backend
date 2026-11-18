# GDI Airtable Purpose, Integrity & Full E2E Persistence Audit V1

Read-only audit of AI Demand Positioning base `appa2cE7FTRmIbB32`.

## Start here

1. `FOUNDER_EXECUTIVE_SUMMARY.md`  
2. `FOUNDER_DECISIONS_REQUIRED.md`  
3. `TABLE_PURPOSE_AND_OWNERSHIP_MATRIX.md`  
4. `PROPOSED_CANONICAL_PERSISTENCE_MAPPING.md`

## Deliverables

| File | Contents |
|------|----------|
| FOUNDER_EXECUTIVE_SUMMARY.md | Verdict + acceptance answers |
| COMPLETE_TABLE_INVENTORY.md | Live counts / IDs |
| TABLE_PURPOSE_AND_OWNERSHIP_MATRIX.md | Purpose, owner, classification |
| RECORD_QUALITY_AND_DUPLICATION_AUDIT.md | Quality / dups |
| EXISTING_WORKFLOW_READ_WRITE_MAP.md | Readers/writers |
| IDENTITY_AND_RELATIONSHIP_ANALYSIS.md | Overlaps / boundaries |
| SYNTHETIC_AND_PROVISIONAL_DATA_AUDIT.md | Seed/test/provisional isolation |
| MULTI_HOTEL_RECONCILIATION.md | Seven hotels + Bethesda 7 ops |
| PROPOSED_CANONICAL_PERSISTENCE_MAPPING.md | Full E2E destinations |
| TABLE_CONSOLIDATION_AND_CLEANUP_RECOMMENDATIONS.md | KEEP / RESTRICT / DEPRECATE |
| FOUNDER_DECISIONS_REQUIRED.md | Approval gate |
| IMPLEMENTATION_PLAN.md | Post-approval only |
| EVIDENCE_REGISTER.json | Live evidence |
| `_live_*.json` | Machine extracts |

## Reproduce (read-only)

```bash
node scripts/gdi-airtable-purpose-integrity-audit-v1.mjs
npm run test:gdi-airtable-purpose-integrity-audit-v1
```

Requires Airtable token in env for live refresh. Deterministic test validates committed report package shape without writing Airtable.
