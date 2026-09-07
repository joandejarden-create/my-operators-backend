# 06 — Decision & Approval Interaction

## Decisions

Actions: `LOCK` | `APPROVE` | `AMEND` | `HOLD` | `REJECT`  
Endpoint: `POST /api/admin/helena-cmo/decisions/:id/action`  
Body: `{ action, note? }`

### Persistence (real today)

Writes `data/helena-cmo/founder-console-actions.json`  
Returns refreshed brief.  
`marketingOsSynced: false`, `executeTriggered: false`

### Persistence (not yet)

Marketing OS Marketing Decisions table upsert — **not connected**. UI states this clearly.

## Approvals (PREPARE)

Actions: `APPROVE` | `AMEND` | `HOLD` | `REJECT`  
Endpoint: `POST /api/admin/helena-cmo/approvals/:id/action`

`APPROVE` → status `APPROVED_PREPARE_ONLY`  
API reminder: does **not** publish, send, deploy, or merge.

## Irreversible external actions

**Not implemented.** No publish, LinkedIn schedule, website deploy, or merge hooks exist on these routes.
