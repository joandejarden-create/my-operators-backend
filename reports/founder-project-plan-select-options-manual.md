# Founder Project Plan — manual single-select option updates

Base: `appKZuK006BWIVjNW`
Table: **Founder Project Plan** (`tblpCg0QZ0kIPXihE`)

Airtable Meta API field PATCH for `options.choices` returned 422 in this environment.
Add the missing options below in the Airtable UI (Field configuration → Options).
Do **not** delete legacy options yet — record migration will happen in a separate script.

## Phase

- Field type: singleSelect
- Options to add: Product Definition, Data & Pipeline Setup, Pilot Readiness

**Target option order (new options should follow this list; keep legacy values at end):**
- Strategy & Foundations
- Product Definition
- Data & Pipeline Setup
- Platform Design
- Platform Build
- Pilot Readiness
- Testing & Pilot
- Launch & Operations
- Scale & Optimize

## Status

- Field type: singleSelect
- Options to add: Backlog, Blocked, Ready for Review, Deferred

**Target option order (new options should follow this list; keep legacy values at end):**
- Backlog
- Not Started
- In Progress
- Blocked
- Ready for Review
- Completed
- Deferred
- Not Needed

## Workstream

- Field type: multilineText
- Note: Workstream is free text today. Convert to Single select in Airtable UI, then re-run this script to add standard options.

