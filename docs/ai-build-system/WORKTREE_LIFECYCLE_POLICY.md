# Worktree Lifecycle Policy (permanent)

**Contract:** `DEALALITY_WORKTREE_LIFECYCLE_V1`  
**Command:** `npm run dealality:worktree-lifecycle`  
**Module:** `lib/dealality-dev-os/worktree-lifecycle-v1.js`  
**Registry:** `docs/ai-build-system/worktree-registry.json` (same file — no parallel system)

## Purpose

Govern temporary Git worktrees inside the existing Dealality Development OS:

1. Reuse the workspace registry and post-merge synchronization infrastructure.
2. Register each temporary worktree with product, purpose, owner, branch, associated PR/task, creation date, and lifecycle status.
3. Automatically assess temporary worktrees after their PR merges into `main`.
4. Mark eligible worktrees `RETIREMENT_CANDIDATE` — **never** automatically delete them.
5. Block retirement when safety gates fail.
6. Detect junctions and unlink entries without deleting shared targets.
7. Require **one explicit founder approval** per retirement batch.
8. Retire with `git worktree remove` **without** `--force`; stop on exceptions; record partial completions.
9. Update the registry with retirement result, PR, commit, date, and preservation evidence.
10. Preserve all permanent workspaces; manage Git branches separately.

## Lifecycle statuses

| Status | Meaning |
|--------|---------|
| `PERMANENT` | Registered permanent home (Integration / HE / ADP / GDI / BE) |
| `ACTIVE` | Temporary with unique commits, open task, or not yet merged into `main` |
| `RETIREMENT_CANDIDATE` | Clean, HEAD ⊆ `origin/main`, no blockers — eligible for founder-approved batch |
| `PRESERVATION_REQUIRED` | Dirty, ignored research artifacts, or unprotected config |
| `BLOCKED` | Locks, nested worktrees, active dependency risks, missing path, etc. |
| `RETIRED` | Successfully removed after founder-approved batch (registry record retained) |

## Safety gates (block retirement)

- Uncommitted / untracked changes
- Unique commits (HEAD not ancestor of `origin/main`, or ahead of main)
- Ignored research artifacts (`reports`, `data`, `research`, …)
- Nested worktrees under the path
- Git / worktree locks
- Active dependencies (junction targets under permanent homes)
- Unprotected configuration (`.env` present without batch preservation evidence)
- Permanent workspace paths (always ineligible)
- `--force` / `git worktree remove --force` (always refused)

Junctions are **not** blockers by themselves. Retirement unlinks the reparse point only and never deletes the shared target (e.g. `ws-gdi/node_modules`).

## Commands

```bash
# Inventory + classify all registered Git worktrees
npm run dealality:worktree-lifecycle -- inventory
npm run dealality:worktree-lifecycle -- inventory --json --out reports/ai-build-system/worktree-lifecycle-inventory.json

# Prioritized retirement proposal (no deletes)
npm run dealality:worktree-lifecycle -- propose --batch-limit 8

# After PR merge (also hooked from post-merge sync apply)
npm run dealality:worktree-lifecycle -- assess-after-merge --pr <n> --merged-branch <name>
npm run dealality:worktree-lifecycle -- assess-after-merge --pr <n> --merged-branch <name> --write-registry

# Founder-approved retirement
npm run dealality:worktree-lifecycle -- retire --proposal <file> --founder-approve "<PHRASE>"
npm run dealality:worktree-lifecycle -- retire --proposal <file> --founder-approve "<PHRASE>" --apply
```

## Post-merge integration

`npm run dealality:post-merge-sync -- --mode post-merge --stream <…> --pr <n> --apply` runs a **read-only** lifecycle assess for temporary worktrees matching the merged PR/branch and prints candidate counts. Registry writes for `RETIREMENT_CANDIDATE` use:

```bash
npm run dealality:worktree-lifecycle -- assess-after-merge --pr <n> --merged-branch <name> --write-registry
```

No worktree is deleted from post-merge sync.

## Temporary worktree registration fields

When provisioning a temporary worktree, add an entry under `temporaryWorkspaces` with at least:

| Field | Required |
|-------|----------|
| `id` | yes |
| `stream` | yes (`integration` \| `he` \| `adp` \| `gdi` \| `be`) |
| `path` | yes |
| `lifecycle` | yes (`temporary`) |
| `owner` | yes |
| `role` (purpose) | yes |
| `expectedBranchPattern` or `branch` | yes |
| `associatedPr` / `associatedTask` | when known |
| `createdAt` | yes (ISO) |
| `lifecycleStatus` | yes (`ACTIVE` initially) |
| `safeRemovalEligibility` | recommended |

## Permanent workspaces

Never auto-delete. Never include in retirement batches. Branch lifecycle for `ws/*` / `main` remains owned by post-merge sync + product chats — not by worktree retirement.

## Historical cleanup

Inventory → classify → proposal → **one founder approval phrase** → `--apply` retire.  
This governance PR itself performs **no** historical worktree removals.
