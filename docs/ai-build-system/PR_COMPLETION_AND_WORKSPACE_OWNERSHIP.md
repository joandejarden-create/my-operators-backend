# Universal PR completion and workspace ownership (Dev OS)

**Contract:** `DEALALITY_PR_COMPLETION_AND_WORKSPACE_OWNERSHIP_V1`  
**Applies to:** GDI (`ws-gdi` / `ws/gdi`), ADP (`ws-adp` / `ws/adp`), Hotel Explorer (`ws-hotel-explorer` / `ws/hotel-explorer`), Integration Controller  

## Non-negotiable

A successful GitHub PR merge is **not** a completed development task until:

1. Required CI checks passed.
2. The PR merged into `main`.
3. The owning permanent workspace returned to its registered **home branch**.
4. That home branch is synchronized with latest `origin/main`.
5. Cursor active branch association corrected (`SetActiveBranch` / UI chip).
6. Temporary feature branches retired when safe.
7. Working-tree cleanliness explicitly verified and reported.
8. Valuable research / runtime / execution evidence preserved.
9. Other workspaces not disturbed.

Opening a PR, passing CI, or merging into GitHub alone is **not sufficient**.

## Ownership enforcement

Authoritative ownership source: `docs/ai-build-system/worktree-registry.json`.

Before branch mutations, verify:

- Actual Git worktree root
- Registered owning workstream
- Current branch and HEAD
- Requested target branch
- In-progress merge / rebase / cherry-pick
- Tracked and untracked changes
- Preservation needs for ignored runtime evidence

**Fail closed** when operations violate ownership or would overwrite unpreserved changes.

Cursor conversation branch chips are **not** authoritative Git state.

Cross-workstream feature branch checkouts/merges are prohibited.

## Preservation

Do **not** automatically stash, discard, force-checkout, reset, or `git clean` to resolve conflicts.

A clean Source Control display must never take priority over preserving work.

## Integration Controller

Owns: merge authorization/execution, CI verification, remote merges that do not disturb dirty local trees, safe permanent-home sync, post-merge verification, consistent completion reporting.

Product chats must not independently merge to `main` or run disruptive cross-worktree operations.

## Standardized completion statuses

| Status | Meaning |
|--------|---------|
| **COMPLETE — MERGED_AND_HOME_SYNCED — CLEAN_WORKTREE** | Merged, home synced, working tree clean |
| **COMPLETE — MERGED_AND_HOME_SYNCED — PRESERVED_DIRTY** | Merged + home synced; intentional local/research artifacts remain (list them) |
| **BLOCKED — PRESERVATION_REQUIRED** | Cannot continue without protecting existing work |
| **BLOCKED — CROSS_WORKSTREAM_BRANCH** | Branch violates registered workspace ownership |
| **BLOCKED — UNRESOLVED_GIT_OPERATION** | Active merge/rebase/cherry-pick must be reconciled |
| **BLOCKED — HOME_NOT_SYNCED** | Not on home and/or home/HEAD not at `origin/main` |
| **BLOCKED — NOT_MERGED** / **BLOCKED — CI_INCOMPLETE** | Merge or CI gate incomplete |

Never report completion from PR merge status alone.

## Required final report fields

- PR number and merge SHA
- Owning workstream and worktree path
- Permanent home branch
- `git branch --show-current`
- `git rev-parse HEAD`
- `git rev-parse origin/main`
- HEAD synchronization status
- Tracked modification count
- Visible untracked count
- Ignored evidence preservation status
- Cursor active branch
- Temporary feature branch retirement status
- Peer worktree impact
- Final standardized completion status

## Tooling (reuse — do not fork)

| Concern | Command / module |
|---------|------------------|
| Ownership + dirty gates | `npm run dealality:git-preflight` → `lib/dealality-dev-os/git-preflight.js` + `workspace-ownership.js` |
| Post-merge home sync | `npm run dealality:post-merge-sync` → `POST_MERGE_WORKSPACE_SYNC.md` |
| Completion report | `npm run dealality:pr-completion-report` → `lib/dealality-dev-os/pr-completion-ownership.js` |
| Ownership audit | `npm run dealality:pr-completion-report -- --audit` |
| Regression gate | `npm run test:dealality-pr-completion-ownership-v1` |

### Recovery before disruptive enforcement

Live workspace recovery (especially cross-stream checkouts and dirty preserve state) must be inventoried with `--audit` **before** forcing home returns. Fail-closed mutation gates refuse new cross-stream switches/merges; they do **not** auto-reset or discard existing dirty trees.

## Related

- `docs/ai-build-system/POST_MERGE_WORKSPACE_SYNC.md`
- `docs/ai-build-system/DEALALITY_DEV_OS.md`
- `.cursor/rules/dealality-worktree-governance.mdc`
- `.cursor/rules/gdi-github-branch-merge-discipline.mdc`
- `.cursor/rules/dealality-pr-completion-workspace-ownership.mdc`
