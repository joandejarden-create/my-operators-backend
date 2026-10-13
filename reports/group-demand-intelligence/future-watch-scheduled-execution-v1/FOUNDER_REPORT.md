# GDI Future Watch Scheduled Execution V1 — Founder Report

## A. EXECUTIVE RESULT

SCHEDULED RUNNER: PASS
DUE-ONLY: PASS
SOURCE-FINGERPRINT-FIRST: PASS
JEV ONE-ACTION LIMIT: PASS
BUDGET GUARD: PASS
IDEMPOTENCY: PASS
CONCURRENCY: PASS
TARGET RUN PERSISTENCE: PASS
WATCH HISTORY: PASS

## B. CURRENT PRODUCTION QUEUE

TOTAL FUTURE WATCH: 4
DUE NOW: 0
SCHEDULED LATER: 4
PUBLIC DATA CEILING: 1

## C. ZERO-DUE TEST

DUE: 0
JEV: 0
QUERIES: 0
FETCHES: 0
COST: $0
PASS

Artifact: `RUN_gdi_fw_sched_2026-09-29T21-31-02-316Z.json`

## D. FORCED-DUE TEST

Candidate: ac_watch_21
Trigger: HOUSING_OPEN
Archetype: URBAN_CORPORATE
Planned Jev action: SKIPPED_NO_JEV (`--no-jev` dry-run)
Queries: 0
Fetches: 0
State mutation: NONE
PASS

Requires: `GDI_WATCH_ALLOW_FORCE_DUE=1` and `NODE_ENV!=production`. Blocked in production.

## E. BUDGET

DEFAULT WATCH LIMIT: 20
DEFAULT BATCH BUDGET: $25
PER-WATCH QUERY CAP: 3
PER-WATCH FETCH CAP: 5
MAX JEV: 1

Env: `GDI_WATCH_BATCH_LIMIT`, `GDI_WATCH_BATCH_BUDGET_USD`, `GDI_WATCH_MAX_QUERIES`, `GDI_WATCH_MAX_FETCHES`, `GDI_WATCH_JEV_ENABLED`

## F. RETRIES

429: PASS
TIMEOUT: PASS
5XX: PASS
MAX RETRIES: 3
EXHAUSTED STATE: RESEARCH_BLOCKED_PROVIDER

Backoff: +1d / +3d / +7d. Provider failure never rejects candidate.

## G. CONCURRENCY

LOCK TYPE: filesystem claim + global scheduler lock
STALE CLAIM RECOVERY: PASS
DOUBLE PROCESSING: 0 expected

## H. PERSISTENCE

TARGET RUN DUPES: 0
WATCH HISTORY DUPES: 0
STATE TRANSITION DUPES: 0

## I. REGRESSION

PROVEN 63: 63/63
FALSE REJECTS: 0
BETHESDA MUTATIONS: 0 (ready=37)
SHARE TOKEN CHANGES: 0
CARD UI: UNCHANGED

## J. PRODUCTION COMMAND

```
node scripts/run-gdi-future-watch-scheduler.mjs --apply
```

Dry-run / health:
```
node scripts/run-gdi-future-watch-scheduler.mjs --dry-run
```

Dev forced-due (local only):
```
GDI_WATCH_ALLOW_FORCE_DUE=1 NODE_ENV=development node scripts/run-gdi-future-watch-scheduler.mjs --dry-run --force-due=ac_watch_21
```

## K. RECOMMENDED CADENCE

DAILY

Explain: due-only query means most daily runs cost $0 when nothing is due; trigger dates vary by watch so a single daily cron is enough — do not create per-watch crons.

## L. DIRECT ANSWERS

1. Auto find due FUTURE_WATCH? YES
2. Avoid not-due research? YES
3. Cheap source-change before deep research? YES
4. Deterministic filter before Jev? YES
5. Jev capped at 1 action/watch/run? YES
6. Provider failures cause false rejection? NO
7. Exceed configured budget? NO (BUDGET_STOP)
8. Retry bounded? YES (1d/3d/7d then RESEARCH_BLOCKED_PROVIDER)
9. Two jobs same watch? NO (claim lock)
10. Every research action persisted? YES (Target Run + history on apply)
11. PUBLIC_DATA_CEILING in normal queue? NO
12. Zero-due cost $0? YES
13. Production-safe command available? YES
14. Ready to wire Railway cron? YES — enable after one shadow scheduled dry-run in prod env
15. Remains: approve Railway cron schedule; optional first --apply shadow week with budget cap

## FINAL VERDICT

GDI SCHEDULED WATCH RUNNER PASSES — READY TO ENABLE CRON

## PERSISTENCE / META

HEAD: 15f90ad62a4a63170e85c7b6975c33f127016bea
FINAL SHA: f6cd3e08b7069b07caf5b14c92cd75a12820644f
PUSH: PASS
DIRTY LEFT: unrelated preserved
Flags: dryRun validated; apply not required for this gate
No customer notifications. No Webhound. No Surfe AUTO. No Bethesda mutation.

STOP.
