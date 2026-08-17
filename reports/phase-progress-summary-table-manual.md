# Phase Progress Summary — Airtable dashboard table

Base: `appKZuK006BWIVjNW`

## What this is

A **separate table** (`Phase Progress Summary`) with one row per Phase plus a totals row — the snapshot table format you wanted:

| Phase | Total Tasks | Completed | Percent Done |
|-------|------------:|----------:|-------------:|
| Strategy & Foundations | 34 | 11 | 31.4% |
| … | … | … | … |
| — ALL PHASES — | 145 | 18 | 12.4% |

Standard Airtable **grouped grid views cannot** calculate % done per phase. This table is synced from Founder Project Plan by script.

## Refresh data

```bash
node scripts/sync-phase-progress-summary.mjs --execute
```

Re-run after you update task Status in Founder Project Plan.

## Recommended view (in Airtable UI)

1. Open table **Phase Progress Summary**
2. Rename default grid to **Phase Completion Dashboard**
3. **Hide:** Last Synced (optional), In Progress / Not Started if you only want the 4-column snapshot
4. **Sort:** Total Tasks descending (or Percent Done descending)
5. **Filter:** `{Phase} != '— ALL PHASES —'` for phases only; remove filter to include totals row at bottom
6. Pin view to sidebar

## Columns

| Field | Meaning |
|-------|---------|
| Phase | Phase name (or `— ALL PHASES —` for rollup) |
| Total Tasks | Task count excluding `[Phase rollup]` rows |
| Completed | Status = Completed |
| In Progress | Status = In Progress |
| Not Started | Status = Not Started |
| Other Status | All other statuses |
| Percent Done | Completed ÷ Total (shown as % in Airtable) |
| Last Synced | UTC timestamp of last script run |

## Source

Computed from **Founder Project Plan** (`tblpCg0QZ0kIPXihE`) via `lib/dealality-master-todo/phase-progress-compute.js`.
