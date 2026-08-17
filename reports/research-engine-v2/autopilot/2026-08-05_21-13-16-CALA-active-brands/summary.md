# Census Autopilot Summary (v2)

1. **Parent company:** (active-brand-setup)
2. **Region / country:** CALA
3. **Mode:** controlled
4. **Total records in scope:** 1
5. **Total processed:** 1
6. **Total updated:** 1
7. **Total skipped:** 0
8. **Total blocked:** 0
9. **Fields populated:** Asset Context
10. **Confidence High/Medium/Low/Hold:** 1 / 0 / 0 / 0
11. **Runtime:** 53 ms
12. **Remaining queues:** (none)
13. **Completion status:** `complete`
14. **Resume command:** (n/a)
15. **Recommended next:**

```bash
npm run census:autopilot -- --region CALA --parent-company IHG --mode plan
```

- **Batch size:** 250 (chunk only)
- **Max records:** (none — full scope)
- **Run until complete:** true
- **Status:** `production_census_autopilot_active_brand_setup_fastest_safe_ready`
- **Airtable writes:** false → Hotel Property Census
- **Brand Explorer writes:** false
- **Webhound candidates:** 0
- **Steward cases:** 0

---

## Queue orchestration (this run)

- Mode: controlled multi-queue (no `--queue`)
- Queues executed: description_extraction → amenities → radar → address → property_name_cleanup → property_type_asset_context → rooms_keys
- Soft-deferred: coordinate_resolution
- High proposals: 1 (Asset Context)
- Airtable writes: false
- See: queue-execution-report.md, approval-bundle.json
- Status: production_census_autopilot_queue_orchestration_fixed_ready_for_multi_queue_controlled
