# Founder Project Plan — daily & phase views (manual setup)

Base: `appKZuK006BWIVjNW`
Table: **Founder Project Plan** (`tblpCg0QZ0kIPXihE`)

> **Note:** Airtable's API cannot create or configure views in this workspace.
> Create each view below in the Airtable UI (~2 min per view).

---

## Today's Focus

**Purpose:** What to work on now: all active master to-dos, in-progress P0/P1 founder tasks, and P0/P1 due this week. Excludes old overdue roadmap noise.

**Filter formula:**
```
AND(
  {Status} != 'Completed',
  {Status} != 'Deferred',
  {Status} != 'Not Needed',
  LEFT({Task}, 14) != '[Phase rollup]',
  OR(
    AND(
      {Source} = 'ChatGPT Master To-Do',
      OR(
        {Priority} = 'P0 = Urgent / Launch-Critical',
        {Priority} = 'P1 = Important Near-Term'
      )
    ),
    AND(
      {Status} = 'In Progress',
      OR(
        {Priority} = 'P0 = Urgent / Launch-Critical',
        {Priority} = 'P1 = Important Near-Term'
      )
    ),
    AND(
      OR(
        {Priority} = 'P0 = Urgent / Launch-Critical',
        {Priority} = 'P1 = Important Near-Term'
      ),
      OR(
        IS_SAME({End}, TODAY(), 'day'),
        IS_SAME({End}, TODAY(), 'week')
      )
    )
  )
)
```

**Sort:**
- Priority (asc)
- Roadmap Sort (asc)
- End (asc)

**Recommended visible fields:** Task, Priority, Status, End, Next Action, Phase, Roadmap Sort, Workstream, Source, Progress

**Setup steps:**
1. Duplicate Grid view or click + Add view → Grid.
2. Name: Today's Focus
3. Filter → Formula → paste filter above.
4. Sort: Priority ascending, then End ascending.
5. Hide columns you do not need; keep Next Action visible.
6. Pin this view in your Airtable sidebar.

---

## Master To-Do — Today

**Purpose:** Operational pilot/GTM tasks only (Source = ChatGPT Master To-Do), sorted by priority and due date.

**Filter formula:**
```
AND(
  {Source} = 'ChatGPT Master To-Do',
  {Status} != 'Completed',
  {Status} != 'Deferred'
)
```

**Sort:**
- Roadmap Sort (asc)

**Recommended visible fields:** Task, Roadmap Sort, Phase Number, Step Number, Priority, Status, End, Next Action, Related Area, Workstream, Progress, Blocker

---

## Executive Roadmap (Ordered)

**Purpose:** Full founder roadmap in execution order. Uses Roadmap Sort (phase.step key). Founder / PMO tracker is step 1 in each phase.

**Filter formula:**
```
AND(
  {Status} != 'Completed',
  {Status} != 'Deferred',
  {Status} != 'Not Needed',
  LEFT({Task}, 14) != '[Phase rollup]'
)
```

**Group by:** Phase Number

**Sort:**
- Roadmap Sort (asc)

**Recommended visible fields:** Roadmap Sort, Phase Number, Step Number, Phase, Task, Status, Priority, Start, End, Workstream, Progress, Next Action

**Setup steps:**
1. Add view → Grid → name: Executive Roadmap (Ordered).
2. Filter → Formula → paste filter above (active tasks only).
3. Sort: Roadmap Sort ascending (required — single sort rule).
4. Group: Phase Number ascending — NOT the Phase name field (name groups use old Airtable option order).
5. Optional: hide Phase Number column after grouping if redundant.
6. Pin next to Today's Focus.

---

## Phase Progress (Grouped)

**Purpose:** See how many tasks exist per Phase and scan completion by Status. Group headers show record counts.

**Filter formula:**
```
LEFT({Task}, 14) != '[Phase rollup]'
```

**Group by:** Phase Number

**Sort:**
- Roadmap Sort (asc)

**Recommended visible fields:** Task, Roadmap Sort, Step Number, Status, Priority, Progress, End, Workstream

**How to read completion counts:**
- Each Phase Number group header shows total task count.
- Group by Phase Number (1–15), not Phase name — matches roadmap order.
- Within each group, Roadmap Sort orders tasks (Founder / PMO first, then execution sequence).
- For numeric completion %, use the Phase Progress Report script (see below) or an Interface chart.

**Setup steps:**
1. Add view → Grid → name: Phase Progress (Grouped).
2. Filter → Formula → paste exclude-rollup filter.
3. Group → Phase Number (ascending). Do not group by Phase name.
4. Sort within groups: Roadmap Sort ascending.
5. Optional: color records by Status field for faster scanning.

---

## Phase — Completed Only

**Purpose:** Quick list of completed tasks grouped by Phase (for auditing what shipped).

**Filter formula:**
```
AND(
  {Status} = 'Completed',
  LEFT({Task}, 14) != '[Phase rollup]'
)
```

**Group by:** Phase

**Sort:**
- End (desc)
- Task (asc)

**Recommended visible fields:** Task, Phase, Completed Date, End, Workstream, Deliverables

---

## Why Phase sections looked out of order

- **Roadmap Sort** (1.01, 5.03…) is correct per row.
- **Group by Phase** (name) uses the Airtable select option order — which was Platform Design before GTM.
- **Fix in views:** Group by **Phase Number** (1–15) or sort by **Roadmap Sort** only.
- **Optional UI fix:** Reorder Phase field options manually (see below).

### Manual: reorder Phase select options (optional)

1. Open **Founder Project Plan** → click **Phase** column header → **Edit field**.
2. Drag options into roadmap order:
   1. Strategy & Foundations → 2. Product Definition → 3. Strategy & Design →
   4. Resources / Collateral → 5. GTM / Outreach → 6. Pilot Conversion →
   7. Pilot Delivery → 8. Product / Access → 9. Platform Design →
   10. Platform Build → 11. Content & GTM → 12. Testing & Pilot →
   13. Launch & Operations → 14. Scale & Optimize → 15. Later
3. Save. Then **Group by Phase** will match roadmap order.

API reorder script (if schema write is enabled later): `npm run sync:fpp-phase-select-order`


| Field | Purpose |
|-------|---------|
| **Phase Number** | Roadmap phase index (1–15) |
| **Step Number** | Order within phase (1, 2, 3…) |
| **Roadmap Sort** | Formula: `Phase Number + Step Number/100` — sort this column ↑ for full order |

Re-sync step order from repo:
```bash
npm run sync:fpp-phase-order
```

---

## Phase completion report (automated)

Run locally for exact completed/total counts per phase:
```bash
node scripts/report-founder-project-plan-phase-progress.mjs
```

Output: `reports/founder-project-plan-phase-progress.json`

