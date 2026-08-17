# Hotel Property Census Autopilot Summary (v2)

Matched Active / Live Brand Setup brands to production Hotel Property Census records.

1. **Parent company:** (active-brand-setup)
2. **Region / country:** CALA
3. **Mode:** controlled
4. **Total records in scope:** 116
5. **Total processed:** 116
6. **Total updated:** 116
7. **Total skipped:** 0
8. **Total blocked:** 0
9. **Fields populated:** Human Review Required, Enrichment Status, Enrichment Priority, Last Reviewed Date, Current Brand, Data Confidence Tier
10. **Confidence High/Medium/Low/Hold:** 116 / 0 / 0 / 0
11. **Runtime:** 112 ms
12. **Remaining queues:** (none)
13. **Completion status:** `complete`
14. **Resume command:** (n/a)
15. **Recommended next:**

```bash
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode apply --strategy fastest-safe --run-until-complete --batch-size 250 --approval-bundle reports/research-engine-v2/autopilot/2026-08-06_23-16-11-CALA-active-brands/approval-bundle.json --confirm-approval-bundle-bound --enable-production-writes (+ all confirm flags)
```

- **Batch size:** 250 (chunk only)
- **Max records:** (none — full scope)
- **Run until complete:** true
- **Status:** `production_census_autopilot_active_brand_setup_fastest_safe_ready`
- **Airtable writes:** false → Deal Capture Platform / Hotel Property Census (`tbl9aY5ijiuIzzWam`)
- **Brand Setup / Brand Explorer / VIC writes:** false (read-only or blocked)
- **Webhound candidates:** 0
- **Steward cases:** 0
