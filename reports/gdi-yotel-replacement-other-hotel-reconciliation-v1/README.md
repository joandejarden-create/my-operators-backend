# YOTEL replacement + other-hotel reconciliation V1

**Final policy:** YOTEL = delete-and-replace; other hotels = reconcile-and-preserve.  
Prior YOTEL historical-retention guidance is **SUPERSEDED** (`SUPERSEDED_YOTEL_POLICY.md`).

## Required deliverables

| File | Role |
|---|---|
| `FOUNDER_EXECUTIVE_SUMMARY.md` | Stop-gate summary |
| `PREVIOUS_PROMPT_EXECUTION_STATUS.md` | What prior prompts actually changed |
| `SUPERSEDED_YOTEL_POLICY.md` | Explicit supersession of YOTEL retain-108 |
| `YOTEL_EXACT_DELETE_MANIFEST.json` | Exact Airtable delete allowlist |
| `YOTEL_108_RECORD_DELETE_MANIFEST.json` | Same delete set (original name) |
| `YOTEL_RECOVERABLE_BACKUP_PLAN.md` | Recovery procedure |
| `YOTEL_GDO_BACKUP_REDACTED.json` | Redacted full-field backup |
| `YOTEL_42_RECORD_TARGET_STATE.json` | Local baseline + eligibility split |
| `YOTEL_ELIGIBILITY_AND_PERSISTENCE_MATRIX.md` | Qualified vs research-only |
| `YOTEL_LINKED_RECORD_DEPENDENCY_AUDIT.md` | Decisions / shared links |
| `YOTEL_MIGRATION_EXECUTION_SEQUENCE.md` | Staged non-atomic sequence |
| `RADISSON_RECONCILIATION.md` | Non-destructive |
| `GUADALAJARA_RECONCILIATION.md` | Non-destructive |
| `WYNDHAM_CASTILLO_PRESERVATION.md` | Preserve |
| `ROCK_CREEK_CORRECTION_PLAN.md` | Separate auth |
| `PRODUCTION_MIGRATION_AUTHORIZATION_REQUEST.md` | Founder gate |
| `TEST_RESULTS.md` | Tests + live mutation check |

## Regenerate / test

```bash
node scripts/gdi-yotel-replacement-other-hotel-reconciliation-v1.mjs
node scripts/test-gdi-yotel-replacement-migration-plan-v1.mjs
```

**No production deletions or writes in this package.**
