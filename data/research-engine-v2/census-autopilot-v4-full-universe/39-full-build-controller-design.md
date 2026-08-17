# V4 Full-Build Controller Design

## Behavior (equivalent to while-true)

```
while true:
  refresh live Airtable + ledger snapshot
  probe = hasActionableUniverseWork(snapshot)
  if hard circuit → BLOCKED stop
  if probe.complete → COMPLETE stop
  if only temporary block → WAITING + schedule resume
  lane = chooseHighestValueLane(snapshot)
  process wave_batch_size of lane
  validate writes
  persist checkpoint (NOT terminate)
  if process max_iterations / max_runtime → INFRASTRUCTURE_RUNTIME_BOUNDARY + auto-resume ticket
```

## Modules

| Module | Role |
| --- | --- |
| `lib/.../census-autopilot-v4/full-build-controller.js` | probe, lane choice, city Proper Case, ledger helpers |
| `scripts/v4-full-build-controller.mjs` | executable outer loop + lane runners |
| `scripts/v4-full-build-supervisor.mjs` | reads `50-auto-resume-ticket.json` and re-invokes controller |

## Lane priority (BUILD MODE)

1. VERIFIED_READY_INSERT  
2. CITY_PROPER_CASE_REMEDIATION  
3. OFFICIAL_DIRECTORY_DISCOVERY  
4. INDEPENDENT_REDISCOVERY (Cvent challenge → official identity only)  
5. SERPAPI_IDENTITY_RESEARCH (EV; skipped when budget policy prefers free work)  
6. EXISTING_RECORD_REMEDIATION  
7. ADAPTER_NEEDED_ENGINEERING (classify; never halt global build)

## City Proper Case

All insert/update City writes go through `normalizeCityProperCase()` (`canonicalCalaCity` → else `toProperCasePlace`). ALL CAPS / all-lower production cities are remediated in lane 2.

## Production continuity

Simplest existing mechanism: **supervisor CLI + resume ticket** (`npm run census:v4-full-build-supervisor`), schedulable via OS Task Scheduler / cron / Railway cron hitting the same command. No Joan npm step required for normal continuation.
