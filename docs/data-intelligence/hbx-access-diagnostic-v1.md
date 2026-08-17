# HBX Access Diagnostic v1

**Status:** complete  
**Objective:** Distinguish quota / rate-limit / auth / signature / entitlement failures before any Census discovery resume.

## Verdict

| Field | Value |
| --- | --- |
| PRIMARY_ROOT_CAUSE | `TEST_DAILY_QUOTA_EXHAUSTED` |
| SECONDARY_ROOT_CAUSE | `ORCHESTRATOR_OVER_AGGRESSIVE_REQUEST_RATE` |
| SAFE_TO_RESUME_DISCOVERY | **NO** |
| Credential replacement required | NO |
| Account action required | YES (quota reset and/or entitled LIVE Content API) |
| Rate-limit patch required | YES (applied; not resumed) |

## Evidence (sanitized)

- TEST `GET /hotel-api/1.0/status` → **403**, message **`Quota exceeded`**
- TEST Content API `GET /hotel-content-api/1.0/hotels?from=1&to=1` → **403**, message **`Quota exceeded`**
- LIVE probe → **skipped** (no separate LIVE credentials; refused to send TEST key to LIVE host)
- Signature self-check → **PASS**
- Clock check → **PASS**
- Credentials present → YES / YES

This is **not** a licensing-revocation conclusion. Hotelbeds returns HTTP **403** with body `"Quota exceeded"` for exhausted TEST daily quota (also observed earlier in the content inventory hunt).

## Implementation inspection (Wave1 path)

- Client: `lib/research-engine-v2/hbx-content-api-client.js` (same as Wave1)
- Env: `HBX_API_KEY`, `HBX_API_SECRET`, `HBX_ENV` (default `test`)
- Signature: SHA256 hex of `apiKey + secret + unixSeconds` — no drift from Wave1
- Wave1 pacing: `HBX_BATCH_DELAY_MS` default **150ms** between pages
- Geography wave used the same 150ms default and sequential geos, but many pages × many geos burned TEST quota quickly; some responses were **429**

### Client bug fixed

`hbxFetchJson` previously set `error_code` from `json.error.code` / `.message` only, so string errors like `"Quota exceeded"` became `null`. Diagnostics and future pulls now extract string `error` correctly.

## Rate-limit patch (no resume)

- `lib/research-engine-v2/hbx-request-rate-limiter-v1.js` — concurrency 1, min interval 1200ms, Retry-After / exponential backoff on 429, per-run request budget
- Wired into `pullCountryHotels` + geography discovery wave defaults
- `.env.example` documents `HBX_MIN_REQUEST_INTERVAL_MS`, `HBX_MAX_REQUESTS_PER_RUN`, `HBX_MAX_RETRIES_ON_429`

## Ledger semantics

Unchanged by this diagnostic. Failed geos remain `FAILED_*`, not `COMPLETE_ZERO_RESULTS`.

## Next action

1. Wait for TEST daily quota reset **or** provision separate entitled **LIVE** Content API credentials (`HBX_API_KEY_LIVE` / `HBX_API_SECRET_LIVE`).
2. Re-run `npm run hbx:access-diagnostic` until Content API returns HTTP 200.
3. Only then consider `--mode resume` on the geography discovery wave (still do not use TEST for full CALA portfolio pull).

## Commands

```bash
npm run hbx:access-diagnostic
```

Report: `reports/research-engine-v2/hbx-access-diagnostic-v1.json`
