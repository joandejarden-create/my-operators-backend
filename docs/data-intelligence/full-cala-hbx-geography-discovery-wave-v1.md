# Full CALA HBX Geography Discovery Wave

**DISCOVERY_STATUS:** `production_census_full_cala_hbx_geography_discovery_stop_for_founder_review`  
**FOUNDER_DECISION_REQUIRED:** **YES**  
**Decision:** `test_daily_quota_exhausted` (see `docs/data-intelligence/hbx-access-diagnostic-v1.md`)

## Policy / technical decision

Do **not** treat HTTP 403 alone as credential revocation. Sanitized Hotelbeds body is **`Quota exceeded`** on the configured TEST account.

Restore Content API access by waiting for TEST daily quota reset **or** provisioning separate entitled LIVE credentials. Re-run `npm run hbx:access-diagnostic` until Content API returns 200. Do not resume full-CALA discovery on TEST. Rate-limit patch is in place; discovery was **not** resumed.

```bash
ALLOW_CENSUS_AUTOPILOT_APPLY=1 CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 CONFIRM_NO_BRAND_EXPLORER_WRITES=1 CONFIRM_NO_BRAND_SETUP_WRITES=1 ENABLE_FULL_CALA_15K_CENSUS_SHELL=1 ENABLE_CENSUS_SHELL_INSERTS=1 ENABLE_CURRENT_BRAND_WRITES=0 ENABLE_BRAND_FAMILY_WRITES=0 ENABLE_ROOMS_WRITES=0 npm run census:full-cala-hbx-geography-discovery -- --mode resume --enable-production-writes
```

| Metric | Value |
| --- | ---: |
| In-scope geographies | 52 |
| HBX complete before (Wave1) | 5 |
| Attempted this run | 47 |
| HBX complete after (true) | 5 |
| Failed (auth/retry) | 47 |
| New HBX source rows | 0 |
| New shells inserted | 0 |
| Census before → after | 5956 → 5956 |

## Files

- Module: `lib/research-engine-v2/full-cala-hbx-geography-discovery-wave-v1.js`
- Ledger: `data/research-engine-v2/full-cala-hbx-geography-discovery/hbx-geography-discovery-ledger.json`
- Final report: `reports/research-engine-v2/full-cala-hbx-geography-discovery-final.json`
- CLI: `npm run census:full-cala-hbx-geography-discovery`

## Safeguards added

- Preflight HBX readability before geography claims
- 401/403 → `FAILED_REQUIRES_REVIEW` + founder stop
- 429/5xx → `FAILED_RETRYABLE` + founder stop
- Ledger repair for false zero-results
- Geography coverage audit ignores failed HBX statuses as “searched”
