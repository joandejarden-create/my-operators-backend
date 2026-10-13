# FPP Reconciliation — Bethesda Marriott Pilot 001 — 2026-10-01

## Live read

`FOUNDER_PROJECT_PLAN_LIVE_READ = UNAVAILABLE` (HTTP 401)

Source of truth remains Airtable GTM **Founder Project Plan** (`appKZuK006BWIVjNW` / `tblpCg0QZ0kIPXihE`).

No Bethesda / Pilot 001 / Founding Hotel Partner / ADP+GDI October 1 rows were found in stale repo FPP JSON reports either.

## What this means

FPP does **not** currently track the Bethesda founding pilot workstream as discrete tasks. Day-1 closure therefore creates:

1. Repo pilot SoT under `data/pilots/bethesda-marriott-001/`
2. Proposed FPP updates in `fpp-reconciliation.json` (no live write executed)

## Proposed status after Day-1 (when token restored)

| Task | Status after | Owner |
|---|---|---|
| Pilot master + Founding Partner #001 | Completed | Dealality |
| ADP Oct 1 baseline V1 | Completed | Dealality |
| October control query set locked | Completed | Dealality |
| GDI Day-1 snapshot | Completed | Dealality |
| Production deploy GDI PDF + client sharing | Completed | Dealality |
| Commercial agreement + billing | Active (founder) | Founder |
| Hotel inputs package | Active (hotel) | Bethesda / Rad |
| Future Watch cron | Active / held | Founder |
| November remeasurement | Active | Dealality |

## Commands (when GTM token works)

```bash
node scripts/bethesda-day1-fpp-live-read.mjs
node scripts/apply-fpp-agent-task-update.mjs --record-id <rec> --status "Needs Review" --progress "100%" --worker cursor --dry-run
# then --execute only after founder review; Completed requires --approved-by "Joan D."
```

Do **not** create duplicate Pilot 001 rows. Do **not** modify unrelated workstreams.
