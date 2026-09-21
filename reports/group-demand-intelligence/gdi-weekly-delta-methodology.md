# GDI Weekly Delta Methodology

**Version:** `gdi_weekly_delta_v1`  
**Module:** `lib/group-demand-intelligence/weekly-delta.js`  
**Runner:** `scripts/gdi-weekly-refresh.mjs`

## Purpose

Reusable weekly refresh for every GDI hotel:

EXISTING CANONICAL UNIVERSE + THIS WEEK'S RESEARCH → WEEKLY DELTA

## States

| State | Meaning |
|---|---|
| NEW | TRUE_ACTIONABLE candidate with no matching prior event cycle |
| UPDATED | Matched prior with material field changes |
| REACTIVATED | Prior inactive/watch becomes commercially actionable again |
| UNCHANGED | Matched prior, no material change (lastSeen updated) |
| CLOSED_DOWNGRADED | Newly closed/fully placed/past (not already inactive) |
| NOT_IN_CURRENT | Prior absent from this week's current set |

## Matching (not title-alone)

organization · normalized event name · acronym · series/cycle · year · date · location · official domain · venue

Different years → different `eventCycleId` (Annual 2026 ≠ Annual 2027).

## Newness rules

- New source URL / PDF alone ≠ NEW  
- New WHO / email / phone → UPDATED  
- New annual cycle → NEW  
- `isNewThisWeek = (firstSeenRunId === latestCompletedWeeklyRunId)`  
- `firstSeenAt` never resets  

## Material fields

event dates · location · venue · host · sourcing · type · priority · room demand · WHO name/email/phone

## Pipeline

1. Freeze baseline snapshots  
2. Run native discovery + Hygiene V3  
3. Union discovery candidates with existing bag (rediscovery)  
4. Compute delta  
5. Stage annotations (`--apply-stage`) without wiping customer validation/action/outcome  

## CLI

```bash
npm run gdi:weekly-refresh -- --hotel=recLuxvwwxID7U2B8 --max-queries=20 --apply-stage
npm run gdi:weekly-refresh -- --hotel=bethesda --skip-discovery --apply-stage
```
