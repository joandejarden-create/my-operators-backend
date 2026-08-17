# Railway Discovery Stall Diagnostics

**Status:** `production_census_v4_railway_discovery_timeout_fix_complete`
**Lane outcome:** `official_directory_discovery_partial_network_remaining`
**Production status alias:** `production_census_v4_railway_discovery_partial_network_remaining`
**Generated:** 2026-08-09T10:45:00.000Z

## Verdict

OFFICIAL_DIRECTORY_DISCOVERY no longer hangs indefinitely on Railway. Per-country/source AbortController timeouts + skip-on-stall + resume checkpoints allow the worker to progress and exit partial when a source stalls (Hilton/Mexico).

## Local smoke

- Countries: Puerto Rico, Jamaica
- elapsed_ms: ~20s
- discovered: 71
- timed_out: 0
- hang_free: true
- artifacts written: `discovery-progress.json`, `discovery-stall-diagnostics.json`, `discovery-resume-checkpoint.json`

## Railway verification (deploy `fd355c6c`)

Observed logs:
- `[discover] railway_safe=1 countries=38 concurrency=2 fetch_timeout_ms=20000`
- Per-country Hilton successes (~9–21s)
- `timed_out family=Hilton country=Mexico ... timeout after 90000ms`
- Skip + continue to Choice / Marriott / IHG / Accor / Wyndham / Preferred
- Resume run #2: `resume_checkpoint_timed_out` for Mexico; `resume_checkpoint_unit_cache` for completed units
- `[discover] railway_safe_done status=official_directory_discovery_partial_network_remaining discovered=1245 timed_out=1 failed=0`
- Controller exited code 20 (`sleep_then_retry`) — not hung

## Fix summary

| Piece | Path |
|------|------|
| Railway-safe runtime | `lib/research-engine-v2/census-autopilot-v4/discovery-railway-safe.js` |
| Resumable orchestrator | `lib/research-engine-v2/census-autopilot-v4/discover-cala-properties-railway-safe.js` |
| timedFetch in Hilton/Marriott/Choice | `lib/hilton-brand-directory-extract.js`, `lib/marriott-brand-directory-extract.js`, Choice adapter |
| Controller wiring | `scripts/v4-full-build-controller.mjs` |
| Smoke | `scripts/smoke-census-v4-railway-discovery-safe.mjs` |

## Env (Railway worker)

```
CENSUS_DISCOVERY_RAILWAY_SAFE_MODE=1
CENSUS_DISCOVERY_FETCH_TIMEOUT_MS=20000
CENSUS_DISCOVERY_SOURCE_TIMEOUT_MS=60000
CENSUS_DISCOVERY_COUNTRY_TIMEOUT_MS=90000
CENSUS_DISCOVERY_SKIP_ON_TIMEOUT=1
CENSUS_DISCOVERY_RESUME=1
CENSUS_DISCOVERY_FORCE_REFRESH=0
CENSUS_DISCOVERY_CONCURRENCY=2
CENSUS_DISCOVERY_MAX_RETRIES=1
CENSUS_DISCOVERY_RETRY_BACKOFF_MS=3000
```

## Known remaining network gap

- Hilton Mexico brand-page crawl exceeds 90s country timeout on Railway → recorded as `timed_out`, skipped on resume until `CENSUS_DISCOVERY_FORCE_REFRESH=1`.
- Choice regional fetches often hit 20s fetch timeout with 0 rows on Railway (network/egress); fail-open continues.

## Artifacts

- `discovery-progress.json`
- `discovery-stall-diagnostics.json`
- `discovery-resume-checkpoint.json`
- `discovery-unit-cache/*`
- `63-railway-discovery-stall-diagnostics.json`
- this file
