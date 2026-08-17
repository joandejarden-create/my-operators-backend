# Founder Project Plan — Agent Task Runner (Cursor, ChatGPT, Helena AI)

**Purpose:** Work through Founder Project Plan tasks with **Joan review and approval** before anything is marked **Completed**.

**Table:** Founder Project Plan `tblpCg0QZ0kIPXihE` (GTM base `appKZuK006BWIVjNW`)

---

## Core rule

| Who | May do |
|-----|--------|
| **Agents** (Cursor, ChatGPT, Helena) | Draft, implement, audit, set **In Progress** / **Needs Review** |
| **Joan** | **Approve** → **Completed**; send live outreach; production access |

**Agents never mark Completed without Joan’s explicit approval.**

---

## Workers

| Worker | Best for | Stop at |
|--------|----------|---------|
| **Cursor** | Code, scripts, Airtable sync, platform UI, PTL reports | Needs Review |
| **ChatGPT** | GTM copy, playbooks, call scripts, intake questions | Needs Review |
| **Helena AI** | Brand/operator material outreach + intake logging | Needs Review |
| **Human only** | Legal, financial, compliance, live sends, access hygiene | Joan executes |

Playbooks: `lib/dealality-master-todo/fpp-agent-workflow-config.js`

---

## Daily workflow

### 1. Pick next task

```bash
npm run fpp:next-task
# or filter by worker:
node scripts/run-next-fpp-task.mjs --worker chatgpt --list 5
```

Uses the same logic as **Today's Focus** (P1 master to-dos, in-progress P1, due this week).

Report: `reports/fpp-agent-next-task.json`

### 2. Worker executes playbook

- Follow steps in the JSON report / console output.
- Produce deliverable (doc, script output, draft copy, Helena intake row).
- **Do not** set Completed.

### 3. Agent submits for review

```bash
node scripts/apply-fpp-agent-task-update.mjs \
  --record-id recXXXXXXXX \
  --status "Needs Review" \
  --progress "75%" \
  --next-action "Draft in GTM notes — ready for Joan review" \
  --worker chatgpt \
  --dry-run

# After preview looks correct:
node scripts/apply-fpp-agent-task-update.mjs ... --execute
```

### 4. Joan reviews in Airtable

Open **Today's Focus** → task at **Needs Review** → check deliverable against **approval checklist** in the playbook.

### 5. Joan approves → Completed

Only after you sign off:

```bash
node scripts/apply-fpp-agent-task-update.mjs \
  --record-id recXXXXXXXX \
  --status Completed \
  --progress "100%" \
  --approved-by "Joan D." \
  --worker cursor \
  --execute
```

`--approved-by` is **required** for Completed. Script rejects agent-only completion.

---

## ChatGPT setup

1. Custom GPT with `docs/dealality-airtable-chatgpt-openapi.yaml` actions.
2. Instructions: *Stop at Needs Review; never set Completed; use read for PTL; writes only to FPP with Joan approval.*
3. For copy tasks (mt-04, mt-05, mt-06, mt-24): draft → paste into apply script or Joan updates Airtable.

---

## Helena AI setup

1. Outreach for **brand/operator data** tasks (Data Collection, reference materials).
2. Log every request in **Partner Intelligence - Helena Outreach Intake** (when table live).
3. On receipt → Partner Source Library → set FPP task **Needs Review**.
4. Helena does **not** mark outreach as sent or task Completed without Joan.

See: `docs/partner-helena-intake-airtable-fields.md`, `docs/partner-reference-material-collection-guide.md`

---

## Cursor setup

- Project rule: `.cursor/rules/fpp-agent-task-runner.mdc`
- Start sessions with: *“Run next FPP task for Cursor worker”*
- Always dry-run Airtable writes first.

---

## Status flow

```
Not Started → In Progress → Needs Review → (Joan approves) → Completed
                    ↑                           |
                    └──── revise ─────────────┘
```

| Status | Meaning |
|--------|---------|
| In Progress | Agent or Joan actively working |
| Needs Review | **Deliverable ready — waiting on Joan** |
| Completed | **Joan approved only** |

---

## What agents must not do

- Mark **Completed** without `--approved-by "Joan D."`
- Bulk-write **Pilot Target List** without explicit approval
- Send outreach emails or LinkedIn messages autonomously
- Change Airtable schema or production Memberstack/Webflow access
- Skip **Needs Review** for GTM copy and pilot materials

---

## Optional Airtable fields (future)

Not required to start. If added later:

| Field | Use |
|-------|-----|
| Worker | cursor / chatgpt / helena |
| Last Agent Run | ISO timestamp |
| Deliverable Link | URL or repo path |

---

## Regression checklist

- [ ] `npm run fpp:next-task` returns a sensible next task
- [ ] apply script **fails** Completed without `--approved-by`
- [ ] apply script **passes** Needs Review dry-run
- [ ] Completed with `--approved-by` writes End + Completed Date
- [ ] Today's Focus in Airtable matches queue count (approx.)

---

## Related

- `docs/dealality-master-todo.md` — master to-do upsert
- `reports/founder-project-plan-daily-views-manual.md` — Today's Focus view
- `lib/dealality-master-todo/fpp-agent-approval.js` — validation
