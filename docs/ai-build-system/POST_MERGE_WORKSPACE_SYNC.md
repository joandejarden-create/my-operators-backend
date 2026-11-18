# Post-merge product workspace sync (Dev OS)

**Contract:** `DEALALITY_POST_MERGE_WORKSPACE_SYNC_V1`  
**Command:** `npm run dealality:post-merge-sync`  
**Module:** `lib/dealality-dev-os/post-merge-workspace-sync.js`

## Purpose

After each successful PR merge into `origin/main`, keep the three permanent product workspaces aligned so the next Hotel Explorer / ADP / GDI chat starts from the latest validated main — **without creating additional worktrees**.

| Stream | Workspace | Home branch |
|--------|-----------|-------------|
| HE | `C:/Dev/dealality/ws-hotel-explorer` | `ws/hotel-explorer` |
| ADP | `C:/Dev/dealality/ws-adp` | `ws/adp` |
| GDI | `C:/Dev/dealality/ws-gdi` | `ws/gdi` |

## Mandatory Integration Controller step

After merge confirmation:

```bash
npm run dealality:post-merge-sync -- --mode post-merge --stream <he|adp|gdi> --pr <n>
# or: --merged-branch <feature-branch>
# review plan, then:
npm run dealality:post-merge-sync -- --mode post-merge --stream <he|adp|gdi> --pr <n> --apply
```

Default is **dry-run**. `--apply` performs only safe fetch / verified backup (when dirty) / switch-home / `merge --ff-only origin/main` / preflight verify.

Return-home is authorized only with an **explicit** merged identity (`--pr` head and/or `--merged-branch`). A new branch whose tip equals `origin/main` with only dirty WIP is treated as an **active task** (fetch-only).

## Safety rules (fail-closed)

1. **Never** `reset --hard`, `clean -fd`, stash-discard, force-push, or overwrite dirty/untracked files.
2. **Do not** automatically switch branches in a workspace with an **active unmerged task** — **fetch only**.
3. If return-to-home is authorized and the workspace is dirty with **no** upstream overlap → `PRESERVE_THEN_RETURN_HOME` (SHA-256 verified external backup, then switch + FF).
4. If dirty paths overlap upstream changes → `BLOCKED_UPSTREAM_DIRTY_OVERLAP`.
5. If home is behind and dirty paths do **not** overlap → allow FF while preserving dirty files.
6. Dirty **primary/integration** checkout → skip auto-sync there (`SKIP_INTEGRATION_DIRTY`); still sync product workspaces when safe.
7. Do **not** provision new worktrees for ordinary post-merge sync.

## Peer workspaces

When one stream’s PR merges, the other two should still:

- `git fetch` (shared refs), and
- fast-forward their **home** branch when idle (on home, no overlap),

but must **not** leave an active feature branch.

```bash
npm run dealality:post-merge-sync -- --mode peer-sync
npm run dealality:post-merge-sync -- --mode peer-sync --apply
```

## Actions

| Action | Meaning |
|--------|---------|
| `ALREADY_SYNCED` | Home tip matches `origin/main` |
| `FAST_FORWARD_HOME` | On home; safe `merge --ff-only origin/main` |
| `RETURN_HOME_THEN_FF` | Clean explicit merged head → switch home → FF |
| `PRESERVE_THEN_RETURN_HOME` | Dirty explicit merged head → verified backup → switch → FF |
| `FETCH_ONLY_ACTIVE_TASK` | Active unmerged task — fetch only |
| `BLOCKED_UPSTREAM_DIRTY_OVERLAP` | Sync would risk dirty paths overlapping upstream |
| `SKIP_INTEGRATION_DIRTY` | Primary dirty — preserve |

## Verification

After apply, each mutated product workspace must pass:

```bash
npm run dealality:git-preflight -- --op status-check --stream <he|adp|gdi>
```

Gate: `npm run test:dealality-post-merge-workspace-sync-v1`
