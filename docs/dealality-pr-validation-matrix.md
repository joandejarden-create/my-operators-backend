# Dealality PR validation matrix

> **Purpose:** Map changed paths → validation commands + risk tier so reviewers trust evidence bundles instead of line-by-line diffs.  
> **Usage:** Before opening or merging a PR, run commands for every row that matches your diff. Attach output paths or screenshots to the PR description.  
> **Automation:** `npm run dealality:pr-check-suggest` (reads `lib/dealality-pr-check-matrix.js`)  
> **Process rules:** `.cursor/rules/deal-capture-implementation-partner.mdc`  
> **Project memory:** `AGENTS.md`

---

## Risk tiers

| Tier | Meaning | Human review |
|------|---------|--------------|
| **Low** | Read-path UI, copy, docs, reports-only | Spot-check evidence; diff optional |
| **Medium** | Read-path logic, filtering, rendering, non-critical API reads | Review evidence + skim diff for touched modules |
| **High** | Airtable writes, schema mapping, auth, scoring, approval states, exports with PII | Full review: validation output + diff + dry-run payload preview |

**High-tier writes always require:** validation pass, sanitized payload preview (dev), exact field mapping, and rollback note in PR.

---

## Path → validation commands

### Core server & auth

| Changed paths | Risk | Run before merge |
|---------------|------|------------------|
| `api/me.js`, `api/auth-*`, Memberstack config | **High** | `npm run test:batch1-route-auth` |
| `api/*` (any deal/brand/operator route) | **Medium–High** | `npm run test:batch1-route-auth`; add `test:batch2a-route-auth` if operator/third-party routes |
| `server.js` route registration | **Medium** | Smoke relevant `test:batch*-route-auth` for mounted paths |

### Operator side

| Changed paths | Risk | Run before merge |
|---------------|------|------------------|
| `api/third-party-operator*.js`, `api/lib/operator-setup*.js` | **High** | `npm run test:batch2a-route-auth`; `npm run test:operator-setup-new-base-save-coverage` (if script exists) |
| `api/lib/third-party-operator-new-two-field-bindings.json` | **High** | Same as above + regenerate audit if mapping doc stale: `node scripts/generate-operator-setup-to-explorer-field-mapping-audit.mjs` |
| `public/third-party-operator-setup*.html`, `public/js/operator-setup*.js` | **Medium** | Manual: save dry-run in staging; `test:batch2a-final-validation` if intake validation touched |
| `public/js/operator-explorer*.js`, operator explorer API | **Medium** | Manual QA on one Active operator profile; check loading/empty/error states |

### Brand Explorer

| Changed paths | Risk | Run before merge |
|---------------|------|------------------|
| `fixtures/brand-explorer-presentation-*` | **Medium** | `npm run audit-choice-explorer-presentation-gaps`; `npm run audit-brand-explorer-presentation-formats` |
| `scripts/apply-*-explorer*`, `scripts/apply-choice-*` | **High** | Gap + format audits before **and** after apply; never apply in PR without dry-run log |
| `api/brand-library.js`, `public/js/brand-explorer*.js` | **Medium** | Manual: one brand detail load; list + detail cache behavior if TTL changed |
| `scripts/strip-internal-fee-notes-airtable.mjs` | **High** | Dry-run only in PR; apply in separate controlled run |

### Deals, deal room, NDA

| Changed paths | Risk | Run before merge |
|---------------|------|------------------|
| `api/brand-deal-requests.js`, deal room routes | **High** | `npm run test:batch1-cross-owner-access`; `npm run test:deal-next-action` if next-action logic touched |
| `public/js/deal-*`, NDA flows | **High** | Manual confidential-flow QA; attachment tests if CU tab: `npm run test:deal-setup-cu-attachment-*` |

### GTM / pilot / outreach

| Changed paths | Risk | Run before merge |
|---------------|------|------------------|
| `api/target-list.js`, `api/outreach-setup.js` | **High** | `npm run test:outreach-setup-field-map`; `npm run test:owner-targets-outreach-export` |
| `scripts/*gtm*`, `scripts/*pilot-target*` | **High** | Always `--dry-run` in PR; `npm run audit-gtm-owner-target-base` if schema touched |
| `scripts/setup-pilot-target-list-*` | **High** | Matching `npm run test:pilot-target-list-*` scripts |

### Master To-Do / Founder Project Plan

| Changed paths | Risk | Run before merge |
|---------------|------|------------------|
| `lib/dealality-master-todo/*`, `scripts/*master-todo*` | **Medium** | `npm run test:dealality-master-todo`; `node scripts/audit-dealality-master-todo-structure.mjs --dry-run` |
| `scripts/validate-founder-project-plan-phase-order.mjs` | **Low** | `npm run validate:fpp-phase-order` |

### Market demand / Scout / Radar

| Changed paths | Risk | Run before merge |
|---------------|------|------------------|
| `lib/market-demand/*`, `api/market-demand*` | **Medium** | `npm run test:market-demand`; `npm run validate:market-demand` if live API changed |
| `public/js/scout-*`, scout API | **Medium** | Matching `npm run test:scout-*` for touched module |
| `lib/travel-infrastructure/*`, `lib/radar-buildout/*` | **Medium** | `npm run test:travel-infrastructure-radar`; country audit script for affected market |
| `scripts/backfill-*-ti*`, `scripts/audit-market-travel-infrastructure.mjs` | **High** | Audit read-only in PR; backfill `--dry-run` only; output JSON in `data/` |

### Partner intelligence

| Changed paths | Risk | Run before merge |
|---------------|------|------------------|
| `api/lib/partner-intelligence-*` | **High** | Schema doc cross-check; `npm run ensure-partner-intelligence-tables` dry-run if fields added |
| `docs/partner-*-airtable-fields.md` | **Low** | Ensure code maps match doc |

### Airtable automations

| Changed paths | Risk | Run before merge |
|---------------|------|------------------|
| `airtable/automations/*.js` | **Medium** | Manual: paste-test in Airtable automation editor; verify field IDs against live schema |

### Docs-only

| Changed paths | Risk | Run before merge |
|---------------|------|------------------|
| `docs/*.md`, `reports/*.md` | **Low** | Link check if URLs added; no npm required |

### Dependency / config

| Changed paths | Risk | Run before merge |
|---------------|------|------------------|
| `package.json` | **Medium** | Run tests for any workspace area that imports new deps |
| `.env.example` | **Low** | Confirm no secrets; comments match actual env usage |

---

## PR description template

```markdown
## Summary
…

## Change impact
- [ ] Low / [ ] Medium / [ ] High

## Validation evidence
| Command | Result | Artifact |
|---------|--------|----------|
| `npm run …` | pass/fail | link or paste key lines |

## Airtable fields touched (if any)
- Table → field → read/write

## Rollback (High only)
…
```

---

## Maintenance

- When adding a new `scripts/test-*.mjs` or `audit-*` script, add a row to this matrix.
- When a path moves, update the row — **stale matrix = false confidence**.
- Quarterly: spot-check 3 recent PRs against this table.

---

## Not in scope (yet)

- Automated `dealality-pr-gate.mjs` orchestrator — add after this matrix proves useful on ~5–10 PRs.
- Auto-merge for Low tier — requires trusted CI running the matrix commands.

## Quick suggest from git diff

```bash
npm run dealality:pr-check-suggest
npm run dealality:pr-check-suggest -- --base main
npm run dealality:pr-check-suggest -- --json
```

Machine-readable rules: `lib/dealality-pr-check-matrix.js` (keep in sync when adding rows above).
