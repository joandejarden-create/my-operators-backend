# Founder Project Plan — manual Airtable view setup

Base: `appKZuK006BWIVjNW`
Table: **Founder Project Plan** (`tblpCg0QZ0kIPXihE`)

Airtable Meta API cannot configure filters, sorts, grouping, or hidden fields in this environment.
Create each grid view below in the Airtable UI.

## Founder Command Center

**Purpose:** Main operating view.

**Filter formula:**
```
AND(
  {Status} != 'Completed',
  {Status} != 'Not Needed',
  {Status} != 'Deferred'
)
```

**Group by:** Phase

**Sort:**
- Priority (asc) — P0 → P1 → P2 → P3
- End (asc) — Use Due Date after field rename

**Hide fields:** Field 14

- Hide legacy import columns (Field 14+) from this view.

## This Week

**Purpose:** Current-week focus and Founder Sprint 1.

**Filter formula:**
```
OR(
  IS_SAME({End}, TODAY(), 'week'),
  {Sprint / Wave} = 'Founder Sprint 1'
)
```

*Note:* Uses End (existing date field). After renaming End → Due Date in Airtable, update this formula to {Due Date}.

**Sort:**
- Priority (asc)
- End (asc) — Rename to Due Date when field is renamed

## By Phase

**Purpose:** Roadmap grouped by phase.

**Filter:** (none)

**Group by:** Phase

**Sort:**
- Priority (asc)
- End (asc) — Use Due Date after field rename

## By Workstream

**Purpose:** Workstream swimlanes.

**Filter:** (none)

**Group by:** Workstream

**Sort:**
- Phase (asc)
- Priority (asc)

- If Workstream remains free text, grouping uses raw text values until migrated.

## Pilot Readiness

**Purpose:** Pilot pipeline and outreach tasks.

**Filter formula:**
```
OR(
  {Phase} = 'Pilot Readiness',
  {Workstream} = 'Pilot Pipeline',
  {Workstream} = 'Owner Outreach',
  {Workstream} = 'Brand / Operator Outreach',
  {Related Table} = 'Pilot Target List',
  {Related Table} = 'Owner Targets'
)
```

**Sort:**
- Priority (asc)
- End (asc) — Use Due Date after field rename

## Platform Build Tracker

**Purpose:** Engineering and platform delivery.

**Filter formula:**
```
OR(
  {Phase} = 'Platform Build',
  {Workstream} = 'Platform Build'
)
```

**Sort:**
- Priority (asc)
- End (asc) — Use Due Date after field rename

## Blocked / Needs Decision

**Purpose:** Escalations and review queue.

**Filter formula:**
```
OR(
  {Status} = 'Blocked',
  {Status} = 'Ready for Review'
)
```

**Sort:**
- Priority (asc)
- End (asc) — Use Due Date after field rename

## Completed Milestones

**Purpose:** Shipped milestones.

**Filter formula:**
```
AND({Milestone?}, {Status} = 'Completed')
```

**Sort:**
- End (desc) — Use Due Date after field rename

## GTM & Outreach

**Purpose:** Go-to-market and relationship workstreams.

**Filter formula:**
```
OR(
  {Workstream} = 'GTM & Content',
  {Workstream} = 'CRM & Communications',
  {Workstream} = 'Owner Outreach',
  {Workstream} = 'Brand / Operator Outreach',
  {Workstream} = 'Referral Program'
)
```

**Sort:**
- Priority (asc)
- End (asc) — Use Due Date after field rename

## Executive Roadmap

**Purpose:** Milestone-only executive rollup.

**Filter formula:**
```
{Milestone?}
```

**Group by:** Phase

**Sort:**
- Phase (asc)
- End (asc) — Use Due Date after field rename

