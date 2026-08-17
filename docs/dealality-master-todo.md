# Dealality Master To-Do List

## Purpose

The **Dealality Master To-Do List** is the single source of truth for founder/GTM/pilot operational tasks during pilot conversion readiness. It connects ChatGPT/Cursor planning work to Airtable so tasks are trackable, filterable, and visible alongside GTM execution.

**Not the same as:** the long-running **Founder Project Plan** roadmap (strategy/platform build tasks imported from Excel). Master tasks are distinguished by:

- `Source` = `ChatGPT Master To-Do` (when field exists), or
- `[Master To-Do | ChatGPT Master To-Do]` prefix in description (fallback), or
- `Success Metric` containing `Seed ID: mt-XX`

## Selected Airtable table

| Property | Value |
|----------|--------|
| Base | Owner Targets / GTM — `appKZuK006BWIVjNW` |
| Table | **Founder Project Plan** |
| Table ID | `tblpCg0QZ0kIPXihE` |

There is **no separate Actions / Tasks table** in the GTM base today. Founder Project Plan is the only task-shaped table and safely supports master tasks via `Source` filtering and dedicated views.

A new table is **not required** unless Joan decides to split founder roadmap rows from operational master tasks.

## Field meanings

| Master concept | Airtable field | Notes |
|----------------|----------------|-------|
| Task Name | `Task` | Primary label |
| Workstream | `Workstream` | Free text today; master values like `Pilot Target List` |
| Status | `Status` | Use `Completed` only (never `Done`). ChatGPT `Done` maps to `Completed` on write. |
| Priority | `Priority` | P0–P3 (Airtable labels: e.g. `P1 = Important Near-Term`) |
| Phase | `Phase` | Master phases (GTM / Outreach, Pilot Delivery, …) |
| Owner | `Assigned To` | Default `Joan D.` |
| Due Date | `End` | Optional on seed |
| Description | `Task Objective/Description` | |
| Next Action | `Next Action` | |
| Source | `Source` | **Recommended field to add** |
| Related Area | `Related Area` | **Recommended field to add** |
| Progress | `Progress` | |
| Blocking Issue | `Blocker` | |
| Dependency | `Dependency` | |
| Completed Date | `Completed Date` | Optional — only set when a real completion date is known (not auto-filled on upsert) |

See `lib/dealality-master-todo/master-todo-field-map.js` for the full mapping.

## Status definitions

| Status | Meaning |
|--------|---------|
| Not Started | Not begun |
| In Progress | Active work |
| Waiting | Blocked on external input |
| Drafted | Draft artifact exists; needs polish/finalize |
| Needs Review | Ready for founder review |
| Completed | Complete |
| Deferred | Intentionally postponed |
| Blocked | Cannot proceed |

## Task upsert process

### 1. Audit structure (read-only)

```bash
node scripts/audit-dealality-master-todo-structure.mjs --dry-run
```

Outputs:

- `reports/dealality-master-todo-structure-report.json`
- `reports/dealality-master-todo-manual-actions.md`
- `reports/dealality-master-todo-views-manual.md`

### 2. Dry-run upsert

```bash
node scripts/upsert-dealality-master-todo.mjs --dry-run
```

Review `reports/dealality-master-todo-upsert-report.json` for creates, updates, skips, and duplicates.

### 3. Execute upsert (after Joan approval)

```bash
node scripts/upsert-dealality-master-todo.mjs --execute --table-id tblpCg0QZ0kIPXihE
```

Rules:

- **No deletes**
- **No overwrite** of completed rows unless `--force-update-completed`
- **Low-confidence matches** are skipped (reported as possible duplicates)
- **Pilot Target List** outreach rows are **not** modified — task #7 references them in notes only

### 4. Update seed list

Edit `lib/dealality-master-todo/master-todo-seed.js`, then re-run audit + dry-run + execute.

## Views

Create manually in Airtable (see `reports/dealality-master-todo-views-manual.md`):

1. Master To-Do — Active
2. P1 Today / This Week
3. GTM / Outreach
4. Pilot Delivery
5. Access Hygiene
6. Completed
7. Deferred / Later

Filter master tasks with `{Source} = 'ChatGPT Master To-Do'` once that field exists.

## Rules of engagement

1. **ChatGPT / Cursor master tasks** → flow into this list via upsert script or ChatGPT Airtable actions (`createRecordsByTableId` on `tblpCg0QZ0kIPXihE`).
2. **Completed / Deferred tasks** remain in the table for history; hide via views (not delete).
3. **Founder Project Plan** rows without `Source = ChatGPT Master To-Do` stay in founder roadmap views.
4. **Do not** auto-update Pilot Target List records from master to-do scripts.
5. **Agent workers** (Cursor, ChatGPT, Helena AI) → see **`docs/fpp-agent-task-runner.md`** — agents stop at **Needs Review**; Joan approves **Completed**.

## Agent task runner

```bash
npm run fpp:next-task
node scripts/apply-fpp-agent-task-update.mjs --record-id recXXX --status "Needs Review" --progress "75%" --worker chatgpt --dry-run
```

Completed writes require `--approved-by "Joan D."`.

## Tests

```bash
npm run test:dealality-master-todo
npm run test:batch1-route-auth
```

## Related modules

- `lib/dealality-master-todo/master-todo-seed.js` — canonical task list
- `lib/dealality-master-todo/master-todo-field-map.js` — field + option targets
- `docs/dealality-airtable-chatgpt-openapi.yaml` — ChatGPT read/write API
