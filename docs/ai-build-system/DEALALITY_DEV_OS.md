# Dealality Development Operating System (Phase 1)

**Status:** Phase 1 safety infrastructure  
**Baseline at authoring:** `origin/main` after PR #380 (`ed0d91aa8…`)

## Goals

- Three product streams (Hotel Explorer, ADP, GDI) evolve in parallel.
- Completed work merges into `main` frequently via small tested PRs.
- Dirty/unfinished work is never discarded accidentally.
- Cursor chats do not spawn uncontrolled worktrees.

## Four permanent homes

| Role | Path | Branch policy |
|---|---|---|
| Integration Controller | `C:\Dev\dealality\my-operators-backend` | `main` |
| Hotel Explorer | `C:\Dev\dealality\ws-hotel-explorer` | short-lived `feat|fix|docs/…` from main |
| ADP | `C:\Dev\dealality\ws-adp` | short-lived task branches from main |
| GDI | `C:\Dev\dealality\ws-gdi` | short-lived task branches from main |

Static registry: `docs/ai-build-system/worktree-registry.json`  
Live audit (does not edit registry): `npm run dealality:worktree-audit`  
Registry drift: `npm run dealality:worktree-registry-status`

Only the Integration Controller commits registry changes.

## Standard task workflow

```
Preflight → Branch from origin/main → Implement → Targeted tests
→ Commit → Push (preserves remote even if PR blocked) → PR
→ dealality:ci-precheck + required CI → Integration Controller merge
→ Sync workspaces → Next task
```

Commands:

```bash
npm run dealality:git-preflight -- --op commit --stream <he|adp|gdi|integration>
npm run dealality:pr-check-suggest -- --base origin/main
npm run dealality:ci-precheck
```

**Merges are serialized** by the Integration Controller. Product chats stop at green PR.

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
One task → one branch → tests → commit → push → PR. Do not merge; Integration Controller merges.
No unregistered worktrees. No force-push. Preserve dirty work. STOP on blockers.
```

### ADP

```
You are the ADP stream chat.
Registered workspace: C:\Dev\dealality\ws-adp (once provisioned).
Before any Git mutation: npm run dealality:git-preflight -- --op <op> --stream adp
One task → one branch → tests → commit → push → PR. Do not merge; Integration Controller merges.
No unregistered worktrees. No force-push. Preserve dirty work. STOP on blockers.
```

### GDI

```
You are the GDI stream chat.
Registered workspace: C:\Dev\dealality\ws-gdi (once provisioned).
Before any Git mutation: npm run dealality:git-preflight -- --op <op> --stream gdi
One task → one branch → tests → commit → push → PR. Do not merge; Integration Controller merges.
No unregistered worktrees. No force-push. Preserve dirty work. STOP on blockers.
```

## Related docs

- Preservation plan (primary 39 dirty paths): `docs/ai-build-system/primary-checkout-preservation-plan-20261118.md`
- Merge discipline: `.cursor/rules/gdi-github-branch-merge-discipline.mdc`
- CI precheck: `.cursor/rules/dealality-ci-precheck-discipline.mdc`
