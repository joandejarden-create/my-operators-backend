# Dealality Development Operating System (Phase 1)

**Status:** Phase 1 safety infrastructure  
**Baseline at authoring:** `origin/main` after PR #380 (`ed0d91aa8…`)

## Goals

- Product streams (Hotel Explorer, ADP, GDI, Brand Explorer) evolve in parallel.
- Completed work merges into `main` frequently via small tested PRs.
- Dirty/unfinished work is never discarded accidentally.
- Cursor chats do not spawn uncontrolled worktrees.

## Five permanent homes

| Role | Path | Branch policy |
|---|---|---|
| Integration Controller | `C:\Dev\dealality\my-operators-backend` | `main` |
| Hotel Explorer | `C:\Dev\dealality\ws-hotel-explorer` | short-lived `feat|fix|docs/…` from main |
| ADP | `C:\Dev\dealality\ws-adp` | short-lived task branches from main |
| GDI | `C:\Dev\dealality\ws-gdi` | short-lived task branches from main |
| Brand Explorer | `C:\Dev\dealality\ws-brand-explorer` | short-lived `feat|fix|docs|research/(brand-explorer|be)/…` from main |

Static registry: `docs/ai-build-system/worktree-registry.json`  
Live audit (does not edit registry): `npm run dealality:worktree-audit`  
Registry drift: `npm run dealality:worktree-registry-status`  
Worktree lifecycle (inventory / propose / founder-approved retire): `npm run dealality:worktree-lifecycle` — see `WORKTREE_LIFECYCLE_POLICY.md`

Only the Integration Controller commits registry changes. Temporary worktrees must be registered with purpose, owner, branch, PR/task, createdAt, and `lifecycleStatus`. Never auto-delete; retirement requires one founder approval phrase per batch.

## Standard task workflow

```
Preflight → Branch from origin/main → Implement → Targeted tests
→ Commit → Push (preserves remote even if PR blocked) → PR
→ dealality:ci-precheck + required CI → Integration Controller merge
→ Sync workspaces → Next task
```

Commands:

```bash
npm run dealality:git-preflight -- --op commit --stream <he|adp|gdi|be|integration>
npm run dealality:pr-check-suggest -- --base origin/main
npm run dealality:ci-precheck
```

**Merges are serialized** by the Integration Controller. Product chats stop at green PR.

### Post-merge workspace sync (required — HE / ADP / GDI / BE)

After each successful PR merge into `main`, Integration Controller runs the **single** supported command:

```bash
npm run dealality:post-merge-sync -- --mode post-merge --stream <he|adp|gdi|be> --pr <n>
# or: --merged-branch <feature-branch>
npm run dealality:post-merge-sync -- --mode post-merge --stream <he|adp|gdi|be> --pr <n> --apply
```

This plans (then optionally applies) safe sync for all product homes:

1. Affected workspace → return to registered home (`ws/hotel-explorer` / `ws/adp` / `ws/gdi` / `ws/brand-explorer`) when the merged head is explicit
2. Fast-forward home to `origin/main`
3. Verify `dealality:git-preflight --op status-check`
4. Idle peer homes → fetch + FF when not on an active unmerged task
5. Dirty primary/integration → skip auto mutation (`SKIP_INTEGRATION_DIRTY`)

Safety: never reset/clean/stash-discard; verified external backup before dirty return-home; never auto-switch active unfinished tasks; no extra worktrees for ordinary sync.

Contract: `docs/ai-build-system/POST_MERGE_WORKSPACE_SYNC.md`

### Universal PR completion + workspace ownership (required — all streams)

A GitHub merge is **not** task completion. Every PR must finish with home-branch restore, `origin/main` sync, Cursor branch correction, feature-branch retirement when safe, and an explicit **CLEAN_WORKTREE** or **PRESERVED_DIRTY** report (or a standardized **BLOCKED — …** status).

Cross-workstream feature branches are fail-closed on mutation ops. Do not stash/discard/reset/clean to force a clean Source Control view.

```bash
npm run dealality:pr-completion-report -- --audit
npm run dealality:pr-completion-report -- --stream <he|adp|gdi|be|integration> --pr <n>
npm run test:dealality-pr-completion-ownership-v1
```

Contract: `docs/ai-build-system/PR_COMPLETION_AND_WORKSPACE_OWNERSHIP.md`  
Cursor rule: `.cursor/rules/dealality-pr-completion-workspace-ownership.mdc`

### Radar authenticated application acceptance (required — HE / Radar)

For changes that affect hotel visibility, filtering, search, mapping, authentication, or Radar → Hotel Explorer navigation, automated tests and CI are **not sufficient**.

Validate in the actual authenticated application:

`http://localhost:8080/app#/opportunity-radar`

Completion states: **PASS** / **FAIL** / **BLOCKED**. Automated tests alone cannot upgrade **BLOCKED** → **PASS**.

Dynamic HPC: eligible Open/Pipeline hotels must appear without hotel-specific allowlists.

Contract + HE/Radar PR checklist: `docs/ai-build-system/RADAR_AUTHENTICATED_APP_ACCEPTANCE_GATE.md`  
Cursor rule: `.cursor/rules/dealality-radar-authenticated-acceptance-gate.mdc`  
Contract gate: `npm run test:radar-authenticated-app-acceptance-gate-v1`

## Git preflight (fail-closed)

`npm run dealality:git-preflight` validates worktree root, branch, dirty state, ownership, protected refs, force-push, and upstream dirty overlap.

It does **not** wrap every raw `git` invocation. Enforcement layers:

1. Cursor always-on rule: `.cursor/rules/dealality-worktree-governance.mdc`
2. Explicit npm preflight before mutations
3. Optional hooks: `.githooks/*` enabled via `git config core.hooksPath .githooks` (Controller only; not auto-enabled in Phase 1)

Destructive ops require `--founder-auth` plus `--preservation-ref` or `--backup-manifest`.

## Startup instructions (product chats)

### Hotel Explorer

```
You are the Hotel Explorer / Radar stream chat.
Registered workspace: C:\Dev\dealality\ws-hotel-explorer (once provisioned).
Before any Git mutation: npm run dealality:git-preflight -- --op <op> --stream he
One task → one branch → tests → authenticated /app#/opportunity-radar acceptance → commit → push → PR.
Do not merge; Integration Controller merges.
Radar visibility tasks are not PASS on CI/unit/API alone — see RADAR_AUTHENTICATED_APP_ACCEPTANCE_GATE.md.
No unregistered worktrees. No force-push. Preserve dirty work. STOP on blockers.
```

### ADP

```
You are the ADP stream chat.
Registered workspace: C:\Dev\dealality\ws-adp
Home branch: ws/adp (must track latest origin/main after each completed merge).
Before any Git mutation: npm run dealality:git-preflight -- --op <op> --stream adp
Permanent cycle: Implement → Test → PR → Founder Approval → Integration Controller merge
→ post-merge sync → return to ws/adp → verify clean → next task.
One task → one short-lived branch → tests → commit → push → PR. Do not merge from ADP; Integration Controller merges.
Do not leave ADP on a completed feature/fix branch. After merge confirmation, restore ws/adp and verify behind 0 / ahead 0 / dirty 0 before the next task.
If approved but unmerged: BLOCKED — AWAITING INTEGRATION CONTROLLER MERGE.
No unregistered worktrees. No force-push. Preserve dirty/untracked work. STOP on blockers.
```

### GDI

```
You are the GDI stream chat.
Registered workspace: C:\Dev\dealality\ws-gdi (once provisioned).
Before any Git mutation: npm run dealality:git-preflight -- --op <op> --stream gdi
One task → one branch → tests → commit → push → PR. Do not merge; Integration Controller merges.
No unregistered worktrees. No force-push. Preserve dirty work. STOP on blockers.
```

### Brand Explorer

```
You are the Brand Explorer (BE) stream chat.
Registered workspace: C:\Dev\dealality\ws-brand-explorer
Home branch: ws/brand-explorer (must track latest origin/main after each completed merge).
Before any Git mutation: npm run dealality:git-preflight -- --op <op> --stream be
Permanent cycle: Implement → Test → PR → Founder Approval (when required) → Integration Controller merge
→ post-merge sync → return to ws/brand-explorer → verify clean → next task.
One task → one short-lived branch → tests → commit → push → PR. Do not merge from BE; Integration Controller merges.
Do not develop BE in ADP/GDI/HE/Integration worktrees. No unregistered worktrees. No force-push. Preserve dirty work. STOP on blockers.
No Airtable writes, paid research, mass population, or publication without founder authorization.
```

## Related docs

- Preservation plan (primary 39 dirty paths): `docs/ai-build-system/primary-checkout-preservation-plan-20261118.md`
- Merge discipline: `.cursor/rules/gdi-github-branch-merge-discipline.mdc`
- CI precheck: `.cursor/rules/dealality-ci-precheck-discipline.mdc`
