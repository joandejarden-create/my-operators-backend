# Dealality Master To-Do — manual actions

Generated: 2026-07-03T08:23:18.944Z
Base: `appKZuK006BWIVjNW`

## Selected source-of-truth table

- **Founder Project Plan** (`tblpCg0QZ0kIPXihE`)
- Only task-shaped table in GTM base. Supports Task, Workstream, Status, Phase, Priority, and operational fields. Use Source = ChatGPT Master To-Do to distinguish master tasks from founder roadmap rows.
- New table needed: **NO**

## Candidate tables inspected

### Founder Project Plan (`tblpCg0QZ0kIPXihE`)
- Fields: 23 | Views: 1

## Missing fields (add in Airtable UI — do not change types without approval)

- **GTM Resource Type** (singleSelect) — optional
- **Pilot Stage** (singleSelect) — optional
- **Order / Sort** (number) — optional

## Missing select options (add manually; API PATCH may return 422)

### Phase
Add: GTM / Outreach, Pilot Conversion, Pilot Delivery, Product / Access, Airtable / Data, Resources / Collateral, Later

### Workstream
Workstream is free text today. Master workstream values can be written as text; convert to single select later with Joan approval.
Recommended options: Pilot Target List, Outreach Execution, Reply Handling, GTM Resources, Pilot Offer, Pilot Conversion, Access Hygiene, Pilot Delivery, Product QA, Content / LinkedIn, Data / Reporting, Later

## Views to create

See C:\Users\joand\OneDrive\Documents\deal-capture-proxy\reports\dealality-master-todo-views-manual.md

## Upsert preview

Seed tasks: 25
Run dry-run upsert:
```
node scripts/upsert-dealality-master-todo.mjs --dry-run
```

## Requires Joan approval

- Add master Phase options (GTM / Outreach, Pilot Conversion, etc.) alongside existing founder phases.
- Review dry-run upsert report before running --execute.

