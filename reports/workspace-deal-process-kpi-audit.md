# Workspace deal-process KPI audit

Generated: 2026-05-22T22:55:33.373Z

Single-deal journey: owner outreach → brand response → NDA → terms → close.

## Summary

- Steps tested: 12
- Passed: 12
- Failed: 0

## Mirror rules (must hold at every step)

| Owner KPI | Brand KPI |
|-----------|----------|
| Awaiting brand | Brand action |
| Owner action | Awaiting owner |
| Stuck / at risk | Stuck / at risk |
| Outreach/Requests sent (7d) | Same (request-sent date) |

## Step-by-step

### 1 — Owner sends outreach — PASS

- **Owner sees:** Owner submits deal / sends brand request
- **Brand sees:** New inbound appears
- Bucket: `new` · Brand next: Review new opportunity · Owner next: Awaiting brand response

| KPI | Owner | Brand |
|-----|-------|-------|
| Needs action | 0 | 1 |
| Awaiting counterparty | 1 | 0 |
| Stuck / at risk | 0 | 0 |
| Pipeline (new / review / terms / neg / closed / passed) | 1 / 0 / 0 / 0 / 0 / 0 | same |

### 2 — Brand opens (viewed) — PASS

- **Owner sees:** Awaiting brand
- **Brand sees:** Brand action — mark decision
- Bucket: `active-review` · Brand next: Mark decision · Owner next: —

| KPI | Owner | Brand |
|-----|-------|-------|
| Needs action | 0 | 1 |
| Awaiting counterparty | 1 | 0 |
| Stuck / at risk | 0 | 0 |
| Pipeline (new / review / terms / neg / closed / passed) | 0 / 1 / 0 / 0 / 0 / 0 | same |

### 3 — Brand requests more info — PASS

- **Owner sees:** Owner action — provide info
- **Brand sees:** Awaiting owner
- Bucket: `awaiting-info` · Brand next: Follow up with owner · Owner next: Provide requested information

| KPI | Owner | Brand |
|-----|-------|-------|
| Needs action | 1 | 0 |
| Awaiting counterparty | 0 | 1 |
| Stuck / at risk | 0 | 0 |
| Pipeline (new / review / terms / neg / closed / passed) | 0 / 0 / 0 / 0 / 0 / 0 | same |

Note: Flow only — not in pipeline columns

### 4 — Brand accepts — PASS

- **Owner sees:** Owner action — send term link
- **Brand sees:** Awaiting owner
- Bucket: `awaiting-info` · Brand next: Send NDA · Owner next: Send term entry link

| KPI | Owner | Brand |
|-----|-------|-------|
| Needs action | 1 | 0 |
| Awaiting counterparty | 0 | 1 |
| Stuck / at risk | 0 | 0 |
| Pipeline (new / review / terms / neg / closed / passed) | 0 / 0 / 0 / 0 / 0 / 0 | same |

### 5 — Brand sends NDA — PASS

- **Owner sees:** Awaiting brand (sign NDA)
- **Brand sees:** Brand action — NDA out
- Bucket: `nda-room` · Brand next: Awaiting signed NDA · Owner next: Send term entry link

| KPI | Owner | Brand |
|-----|-------|-------|
| Needs action | 0 | 1 |
| Awaiting counterparty | 1 | 0 |
| Stuck / at risk | 0 | 0 |
| Pipeline (new / review / terms / neg / closed / passed) | 0 / 0 / 0 / 1 / 0 / 0 | same |

### 6 — NDA signed, grant deal room — PASS

- **Owner sees:** Awaiting brand (open room)
- **Brand sees:** Brand action — grant access
- Bucket: `nda-room` · Brand next: Open deal room · Owner next: Send term entry link

| KPI | Owner | Brand |
|-----|-------|-------|
| Needs action | 0 | 1 |
| Awaiting counterparty | 1 | 0 |
| Stuck / at risk | 0 | 0 |
| Pipeline (new / review / terms / neg / closed / passed) | 0 / 0 / 0 / 1 / 0 / 0 | same |

### 7 — Deal room active — PASS

- **Owner sees:** Awaiting brand (deal room)
- **Brand sees:** Brand action — deal room
- Bucket: `nda-room` · Brand next: Review documents · Owner next: Track LOI & feasibility

| KPI | Owner | Brand |
|-----|-------|-------|
| Needs action | 0 | 1 |
| Awaiting counterparty | 1 | 0 |
| Stuck / at risk | 0 | 0 |
| Pipeline (new / review / terms / neg / closed / passed) | 0 / 0 / 0 / 1 / 0 / 0 | same |

### 8 — Pre-LOI / terms draft — PASS

- **Owner sees:** Owner action — compare terms
- **Brand sees:** Brand action — prepare terms
- Bucket: `terms-proposal` · Brand next: Review documents · Owner next: Owner to compare terms

| KPI | Owner | Brand |
|-----|-------|-------|
| Needs action | 1 | 1 |
| Awaiting counterparty | 1 | 1 |
| Stuck / at risk | 0 | 0 |
| Pipeline (new / review / terms / neg / closed / passed) | 0 / 0 / 1 / 0 / 0 / 0 | same |

Note: Mirror holds: owner action = brand awaiting owner; owner awaiting brand = brand action

### 9 — Proposal submitted — PASS

- **Owner sees:** Owner action — compare terms
- **Brand sees:** Brand action — follow up owner
- Bucket: `terms-proposal` · Brand next: Prepare preliminary terms · Owner next: Owner to compare terms

| KPI | Owner | Brand |
|-----|-------|-------|
| Needs action | 1 | 1 |
| Awaiting counterparty | 1 | 1 |
| Stuck / at risk | 0 | 0 |
| Pipeline (new / review / terms / neg / closed / passed) | 0 / 0 / 1 / 0 / 0 / 0 | same |

### 10 — Finalist — PASS

- **Owner sees:** Owner action — open deal room
- **Brand sees:** Brand internal review
- Bucket: `advanced` · Brand next: Send NDA · Owner next: Open Deal Room

| KPI | Owner | Brand |
|-----|-------|-------|
| Needs action | 1 | 1 |
| Awaiting counterparty | 1 | 1 |
| Stuck / at risk | 0 | 0 |
| Pipeline (new / review / terms / neg / closed / passed) | 0 / 0 / 0 / 1 / 0 / 0 | same |

### 11 — Stalled (overdue follow-up) — PASS

- **Owner sees:** Stuck / at risk
- **Brand sees:** Stuck / at risk
- Bucket: `terms-proposal` · Brand next: Send NDA · Owner next: Owner to compare terms

| KPI | Owner | Brand |
|-----|-------|-------|
| Needs action | 1 | 1 |
| Awaiting counterparty | 1 | 1 |
| Stuck / at risk | 1 | 1 |
| Pipeline (new / review / terms / neg / closed / passed) | 0 / 0 / 1 / 0 / 0 / 0 | same |

### 12 — Declined / passed — PASS

- **Owner sees:** Passed
- **Brand sees:** Passed
- Bucket: `archived` · Brand next: No action required · Owner next: —

| KPI | Owner | Brand |
|-----|-------|-------|
| Needs action | 0 | 0 |
| Awaiting counterparty | 0 | 0 |
| Stuck / at risk | 0 | 0 |
| Pipeline (new / review / terms / neg / closed / passed) | 0 / 0 / 0 / 0 / 0 / 1 | same |

## Findings for product / scale

1. **Scope:** Owner totals include every brand on the owner’s deals; brand totals are per signed-in brand. Compare the same BDR `id` when validating.
2. **Pipeline vs flow:** `awaiting-info` rows count in **Owner action / Awaiting owner** only, not in the six pipeline stage tiles.
3. **Dual flow counts:** At some steps (e.g. Pre-LOI, Finalist) both **Owner action** and **Awaiting brand** can be 1 for the same row because both sides have a defined next step. That is accurate for parallel tasks, not a mirror bug.
4. **Stuck:** Requires `nextFollowupDate` and/or `lastUpdated` / `lastActivity` on the row (full BDR API shape).
5. **Regression test:** `node scripts/test-workspace-kpi-mirror.mjs` + this audit.
