# Dealality parallel dry-run experiment

> **Goal:** Run two read-only agent/workstreams in parallel without Airtable writes or merge conflicts.  
> **Status:** Template for experiment #1 — safe to repeat with different country/brand scopes.

---

## Experiment #1 — Brand explorer gaps + CALA TI audit

### Workstreams (no file overlap)

| Session | Scope | Command | Output |
|---------|--------|---------|--------|
| **A — Brand** | Choice CHI presentation gap audit (read-only) | `npm run audit-choice-explorer-presentation-gaps` | `docs/choice-explorer-presentation-gap-audit.md` |
| **B — CALA TI** | One country travel-infrastructure audit (read-only) | See bundle script or pick from table below | `data/<country>-travel-infrastructure-audit.json` |

### Run both locally (single terminal)

```bash
npm run dealality:parallel-dry-run-audit
```

Or run in **two Cursor agent sessions** / git worktrees:

1. **Worktree A:** only edit `docs/choice-explorer-presentation-gap-audit.md` (audit output).
2. **Worktree B:** only edit `data/*-travel-infrastructure-audit.json` for one country.

### CALA TI audit options (session B)

Pick **one** country not yet on your sprint list, or re-audit for regression:

```bash
# Example — Dominican Republic (adjust if already complete)
node scripts/audit-market-travel-infrastructure.mjs \
  --country "Dominican Republic" \
  --market "Dominican Republic Countrywide" \
  --output data/dominican-republic-travel-infrastructure-audit.json
```

Pre-defined npm aliases exist for many CALA markets — see `package.json` scripts matching `audit:*-ti`.

---

## Rules for parallel agents

1. **No `--apply`** on any script in parallel experiment #1.
2. **No Airtable writes** — audits and gap reports only.
3. **Scope boundaries** — one session must not touch `fixtures/brand-explorer-*` while the other touches `data/*-ti*`.
4. **One PR per workstream** — or one PR with two clearly separated commits if outputs only.
5. **Review evidence, not diffs** — attach audit summary counts and top gaps to PR description (see `docs/dealality-pr-validation-matrix.md`).

---

## Success criteria

- [ ] Both commands exit 0
- [ ] Brand gap audit MD updated with current CHI brand counts
- [ ] TI audit JSON written with `summary` / issue counts documentable in PR
- [ ] No changes outside audit output paths
- [ ] PR validation matrix row filled for evidence

---

## Failure modes & mitigations

| Failure | Mitigation |
|---------|------------|
| Airtable 429 / rate limit | Stagger starts by 30s; run sequentially via bundle script |
| Missing `AIRTABLE_*` env | Session stops; do not retry with invented credentials |
| Agent edits code instead of running audit | Revert code; re-run with explicit read-only prompt |
| Both sessions edit `package.json` | Reset; re-scope sessions |

---

## Next experiments (after #1 passes)

| # | Pair | Risk |
|---|------|------|
| 2 | `audit-brand-explorer-presentation-formats` + `audit-gtm-owner-company-coverage` | Read-only |
| 3 | Fixture patch (dry-run apply) + master todo structure audit | Medium — single writer for fixtures |
| 4 | Overnight audit loop (5 countries, read-only) | See overnight section below |

---

## Overnight loop (experiment #5 — not yet)

**Do not run until experiment #1–2 succeed.**

When ready:

- **Objective:** Run TI audits for N countries in manifest until all pass or iteration cap.
- **Stop conditions:** max 10 iterations, max 5 countries per night, stop on 3 consecutive API failures.
- **Forbidden:** `--apply`, upsert scripts, fixture apply scripts.
- **Morning triage:** Review commit list + JSON summaries; merge only summaries, not agent "fixes."

---

## Cursor prompt snippets

**Session A (brand):**
```
Read-only task. Run npm run audit-choice-explorer-presentation-gaps.
Do not modify code or fixtures. Summarize gap counts by brand in PR-ready markdown.
```

**Session B (CALA TI):**
```
Read-only task. Run dealality:parallel-dry-run-audit TI leg OR audit one CALA country TI.
Write output JSON only under data/. No --apply. Summarize top 5 audit findings.
```
