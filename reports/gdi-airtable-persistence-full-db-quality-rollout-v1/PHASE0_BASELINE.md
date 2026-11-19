# Phase 0 — Integration and complete baseline

**Stream:** GDI (`C:\Dev\dealality\ws-gdi` / `ws/gdi` home; currently on feature branch)  
**Captured:** 2026-11-19  
**Founder auth:** Airtable persistence + full-database quality rollout (Phases 0–8)

## PR #462 status (do not merge from GDI chat)

| Field | Value |
|-------|--------|
| PR | [#462](https://github.com/joandejarden-create/my-operators-backend/pull/462) |
| State | **OPEN** |
| Title | feat(gdi): commercial fact selection Level 1/2 promotion |
| Head branch | `feat/gdi-commercial-fact-selection-v1` |
| Tip at capture (pre local commits) | `9a46e5c0e4c9299a04f57b8a70123656f6c6d50a` |
| Mergeable | MERGEABLE |
| Required CI (latest tip) | SUCCESS (gitleaks, GDI recovery, published inventory, Research Audit, Radar HE, Admin tabs) |
| Integration ownership | **Integration Controller** merges + post-merge sync |

## Exact version baseline (pre local-quality commit)

| Ref | SHA |
|-----|-----|
| Feature HEAD | `9a46e5c0e4c9299a04f57b8a70123656f6c6d50a` |
| `origin/main` | `a6cb58079f17fac6ec00fc164b19b4c29f3bb862` |
| Divergence | behind 4 (ADP PR #461 merges) / ahead 4 (#462 commits) |

### #462 commits already on remote tip

1. `d8471cb8e` — feat(gdi): commercial fact selection Level 1/2 promotion  
2. `f18c4b647` — feat(gdi): retain complete sources with section citations in fact selection  
3. `43e7c20d2` — fix(gdi): consolidate drawer citations into Sources (N) buttons  
4. `9a46e5c0e` — fix(gdi): final drawer parity — collapsed Sources, PDAC contact wording  

## Local-only work inventory (approved, not on #462 tip)

These existed only in the GDI worktree after founder UX certification. They are **not** assumed merged.

### Product / presentation

| Path | Role |
|------|------|
| `lib/.../gdi-person-source-link-v1.js` | Verified person-source links |
| `lib/.../gdi-public-contact-channel-hierarchy-v1.js` | Public email/phone before Surfe |
| `lib/.../gdi-team-page-secondary-candidates-v1.js` | Evidence-backed secondary contacts |
| `lib/.../gdi-sheraton-drawer-comparison-v1.js` | LEFT/RIGHT comparison composition |
| `public/js/.../gdi-contact-reveal-client-v1.js` | Shared Surfe binder (main + comparison) |
| `public/js/.../drawer-comparison-app.js` | Comparison shell + Dealality toast |
| `public/group-demand-intelligence-drawer-comparison.html` | Comparison page |
| `server.js` + `api/group-demand-intelligence.js` | `/drawer-comparison` route + mock reveal |

### Touched existing modules (contact labels, toast, Surfe gaps, recovery → DTO)

`dealality-gdi-ui.js`, `app.js`, `group-demand-intelligence.css`, `group-demand-intelligence.html`, who-to-contact / scaling-two-contact / secondary-named-contact / brief v2 / remediation / drawer models / opportunity-list-dto / certified commercial content contract.

### Evidence reports (preserved)

`reports/gdi-sheraton-guadalajara-drawer-comparison-v1/` — hierarchy matrix, channel validation, toast UX, reveal fix, inventory reconciliation, founder preview, screenshots.

### Explicitly excluded from this GDI PR (other stream / preserve)

| Path | Reason |
|------|--------|
| `public/brand-explorer-phase2e-preview/` | Brand Explorer local preview — leave untracked |
| Dirty `reports/gdi-brief-v2-*` / `gdi-portfolio-demand-intelligence-pdi-3-v1` JSON churn | Incidental hash noise — not part of this UX/persistence milestone |

## Integration plan

1. Commit local-only approved GDI quality UX onto `feat/gdi-commercial-fact-selection-v1`.  
2. Merge/ff `origin/main` into the feature branch when safe (preserve dirty/unrelated).  
3. `dealality:ci-precheck` + push → update **one** open GDI PR (#462).  
4. **STOP merge** — Integration Controller owns merge + CI verification + `post-merge-sync --stream gdi`.  
5. After merge + home sync: Phase 1 Airtable write/read-back certification on Sheraton `recCAGqkERSXgiku4`.

## Governance reminders for later phases

- Slice 2A slim payload does **not** certify full fact↔source graph retention — Phase 1 must extend/compatibly use durable research architecture.  
- No Surfe spend; Surfe session-only; no fixture contacts in production records.  
- No publication field overwrites; no drawer redesign; no parallel GDI engine.  
- Production Airtable writes authorized for verified intelligence under this workflow — still dry-run + baseline + read-back before batch rollout.

## Phase 0 exit criteria

- [x] #462 status confirmed OPEN + CI green at tip `9a46e5c0e`  
- [x] Local-only inventory documented  
- [ ] Local approved work committed + pushed on #462  
- [ ] IC merge + `ws/gdi` == `origin/main` (GDI chat does not merge)  
- [ ] Exact post-merge SHA recorded before Phase 1 production writes  
