# Simulation — contract v1 vs frozen five-hotel facing set

**Mode:** read-only (no Airtable writes)  
**Artifact:** `simulation-report.json`, `production-change-set.json`  
**Apply policy:** only upgrade **default Watch** stamps; never demote evidence-backed High / Pursue Now / Contact / Too Early.

## Before → after (customer-facing UI counts)

| Hotel | Priority before H/M/W | Priority after H/M/W | Action before P/C/W/TE | Action after P/C/W/TE | Patches |
|-------|----------------------|----------------------|------------------------|-----------------------|---------|
| Bethesda | 4 / 15 / 13 | **4 / 15 / 13** | 8 / 9 / 11 / 2 | **8 / 18 / 4 / 2** | 10 (action upgrades only) |
| Waterstone | 1 / 6 / 2 | **1 / 6 / 2** | 0 / 8 / 1 / 0 | **0 / 9 / 0 / 0** | 1 |
| Renaissance TS | 0 / 4 / 4 | **0 / 4 / 4** | 0 / 8 / 0 / 0 | **0 / 8 / 0 / 0** | 0 |
| Mayflower | 0 / 0 / 11 | **0 / 11 / 0** | 0 / 0 / 11 / 0 | **0 / 11 / 0 / 0** | 11 |
| San Diego | 0 / 0 / 21 | **0 / 21 / 0** | 0 / 0 / 21 / 0 | **0 / 21 / 0 / 0** | 21 |

**Total prepared patches:** 43 (see `production-change-set.json`).

### What this does *not* do

- Does **not** invent High / Pursue Now for SD or Mayflower (openBuying false; fit scores null).
- Does **not** map Core prospect packaging → High.
- Does **not** touch customer actions/outcomes/decisions.
- Does **not** change publication eligibility gates.

## Change causes

| Cause | Hotels | Example |
|-------|--------|---------|
| `import_default_watch_stamp` | San Diego (21), Mayflower (11), some Bethesda action-only | READY_FOR_CARD + thesis + usable contact but `WATCHLIST`/`WATCH` with empty reasons |
| Evidence-backed preserved | Bethesda High×4, Waterstone High×1, Renaissance Medium/Watch mix | Prior `classifyPriority` / qualification stamps kept |
| Legitimate hotel difference | AHIMA Bethesda vs Mayflower | Bethesda retains scored Medium; Mayflower upgrades from default Watch→Medium/Contact once fit thesis present — still not High |
| Unresolved / research | Any without thesis + without route | Would stay Watch + enrichmentNeeded (none in SD facing set) |

## San Diego after (intended UI)

- Priority: High **0** / Medium **21** / Watch **0**
- Action: Pursue Now **0** / Contact **21** / Watch **0** / Too Early **0**
- Packaging unchanged: Core **17** / Exploratory **4**

## Production change set (prepared, not applied)

Each patch sets:

```json
{
  "priority": "MEDIUM_PRIORITY",
  "bookingWindowStatus": "QUALIFY_NOW",
  "priorityReason": "...",
  "bookingWindowReason": "...",
  "priorityActionAssignmentVersion": "gdi_priority_action_assignment_v1",
  "priorityActionAssessedAt": "<iso>"
}
```

Preserve: `decisionId`, validation, action/outcome fields, `prospectReviewSection`.

**Do not apply until founder confirms.** Apply script should be a focused idempotent updater using `production-change-set.json` only.
