# Airtable Automation — Phase Completion Dashboard (auto-sync)

Base: **Owner Targets / GTM** (`appKZuK006BWIVjNW`)

This automation recalculates **Phase Completion Dashboard** whenever **Founder Project Plan** tasks change — no manual `node` command needed.

> **Note:** Airtable does not allow creating automations via API. You must paste the script once in the Airtable UI (~5 minutes). After that it runs automatically.

---

## Prerequisites

- Table **Founder Project Plan** (`tblpCg0QZ0kIPXihE`)
- Table **Phase Completion Dashboard** (`tblilet6ncUmmJQDv`) with fields:
  - Phase, Total Tasks, Completed, In Progress, Not Started, Other Status, Percent Done, Last Synced

---

## Setup (one time)

### 1. Open Automations

1. Open your GTM base in Airtable  
2. Click **Automations** (top right)  
3. **Create automation**

### 2. Name it

`Sync Phase Completion Dashboard`

### 3. Choose a trigger (recommended: both)

**Primary — instant updates**

| Setting | Value |
|---------|--------|
| Trigger | **When a record is updated** |
| Table | **Founder Project Plan** |
| Fields | All fields *(or at minimum: Status, Phase, Task)* |

**Optional — hourly backup** (create a second automation with the same script)

| Setting | Value |
|---------|--------|
| Trigger | **At a scheduled time** |
| Interval | Every **1 hour** |

### 4. Add action — Run script

1. **+ Add action** → **Run script**  
2. Open script file in this repo:  
   `airtable/automations/phase-completion-dashboard-sync.js`  
3. **Copy the entire file** and paste into the Airtable script editor  
4. Click **Test action** — you should see output like:
   - `updated: 16`
   - `created: 0`
   - `phases: 15`

### 5. Turn automation ON

Toggle the automation to **On**.

---

## What happens automatically

When you change **Status**, **Phase**, or any task field in Founder Project Plan:

1. Automation fires  
2. Script counts tasks per phase (skips `[Phase rollup]` rows)  
3. Updates every row in **Phase Completion Dashboard**  
4. Refreshes **— ALL PHASES —** totals row  
5. Sets **Last Synced** to current UTC time  

---

## Verify it works

1. Open **Founder Project Plan**  
2. Change any task **Status** (e.g. Not Started → In Progress)  
3. Wait ~10–30 seconds  
4. Open **Phase Completion Dashboard** — numbers and **Last Synced** should update  

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Script error: table not found | Confirm dashboard table is named exactly **Phase Completion Dashboard** |
| Field not found | Field names must match exactly (see prerequisites) |
| Automation not firing | Check automation is **On**; Team plan required for some triggers |
| Too many runs | Use **scheduled hourly** only, or limit trigger fields to Status + Phase |

---

## Fallback (manual)

If automation is off or you need a forced refresh:

```bash
node scripts/sync-phase-progress-summary.mjs --execute
```

---

## Script source of truth

Repo file: `airtable/automations/phase-completion-dashboard-sync.js`  
Node equivalent: `scripts/sync-phase-progress-summary.mjs`

Keep both in sync if you change counting logic.
