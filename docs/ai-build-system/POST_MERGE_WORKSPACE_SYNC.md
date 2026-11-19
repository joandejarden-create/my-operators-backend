# Post-merge product workspace sync (Dev OS)

**Contract:** `DEALALITY_POST_MERGE_WORKSPACE_SYNC_V1`  
**Command (single supported):** `npm run dealality:post-merge-sync`  
**CLI:** `scripts/dealality-post-merge-sync-v1.mjs`  
**Module:** `lib/dealality-dev-os/post-merge-workspace-sync.js`  

There is exactly one supported npm command. Do not add a second competing post-merge sync entrypoint.

## Purpose

After each successful PR merge into `origin/main`, keep the three permanent product workspaces aligned so the next Hotel Explorer / ADP / GDI chat starts from the latest validated main — **without creating additional worktrees**.

| Stream | Workspace | Home branch |
|--------|-----------|-------------|
| HE | `C:/Dev/dealality/ws-hotel-explorer` | `ws/hotel-explorer` |
| ADP | `C:/Dev/dealality/ws-adp` | `ws/adp` |
| GDI | `C:/Dev/dealality/ws-gdi` | `ws/gdi` |
| BE | `C:/Dev/dealality/ws-brand-explorer` | `ws/brand-explorer` |

## Mandatory Integration Controller step

After merge confirmation:

```bash
npm run dealality:post-merge-sync -- --mode post-merge --stream <he|adp|gdi|be> --pr <n>
# or: --merged-branch <feature-branch>
# review plan, then:
npm run dealality:post-merge-sync -- --mode post-merge --stream <he|adp|gdi|be> --pr <n> --apply
```

Default is **dry-run**. `--apply` performs only safe fetch / verified backup (when dirty) / switch-home / `merge --ff-only origin/main` / preflight verify.

Return-home is authorized only with an **explicit** merged identity (`--pr` head and/or `--merged-branch`). A new branch whose tip equals `origin/main` with only dirty WIP is treated as an **active task** (fetch-only).

## Product-chat responsibility (especially GDI)

Separation of duties:

| Role | Owns |
|------|------|
| Product chat (HE / ADP / GDI) | Implementation, tests, PR creation, then **STOP** |
| Integration Controller | CI verification, merge into `main`, post-merge sync apply |
| Product workspace restoration | Home branch + `origin/main` FF after confirmed merge |

GDI permanent rule: a development cycle is **not** complete when a PR is merely opened. After Integration Controller merges, the GDI chat (or Integration post-merge apply) must leave `C:/Dev/dealality/ws-gdi` on synchronized `ws/gdi` before the next GDI task starts.

Report final state with one of:

- `MERGED_AND_HOME_SYNCED`
- `MERGED_AND_HOME_SYNCED — CLEAN_WORKTREE`
- `AWAITING_INTEGRATION_CONTROLLER`
- `BLOCKED — PRESERVATION_REQUIRED`
- `BLOCKED — CROSS_WORKSTREAM_BRANCH`
- `BLOCKED — UNRESOLVED_GIT_OPERATION`
- `BLOCKED — WORKSPACE_RESTORATION_REQUIRED`

## Workspace ownership (permanent)

Authoritative map: `docs/ai-build-system/worktree-registry.json`.  
Enforcement: `npm run dealality:git-preflight` + `lib/dealality-dev-os/workspace-ownership-guard-v1.js` (optional `.githooks/post-checkout`). For `switch`/`checkout`, require `--target-ref <branch>` (alias `--to-branch`); unknown flags and missing targets exit nonzero so the CLI cannot silently authorize a prohibited mutation.

- Product homes must not check out another stream’s feature branches (e.g. `ws-gdi` must not check out `fix/adp-*`).
- Unresolved merge/rebase/orphan UU index stages fail closed.
- Stash is not a routine branch-switch workaround (`FOUNDER_AUTH_STASH_PRESERVE` required).
- Integration Controller merges PRs remotely: `npm run dealality:remote-pr-merge -- --pr <n>` — no feature-branch checkout, no dirty-tree mutation.
- **Cursor `SetActiveBranch` / UI branch chips are not Git authorization** and cannot bypass ownership guards.

Cursor may show historical/multi-root branch chips that are **not** the active Git branch of `ws-gdi`. Verify with `git branch --show-current` and `git rev-parse HEAD` in the registered path; use `SetActiveBranch` / workspace reattach when the UI chip is stale. Do not create worktrees or reset WIP to “fix” the chip.

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
npm run dealality:git-preflight -- --op status-check --stream <he|adp|gdi|be>
```

Gate: `npm run test:dealality-post-merge-workspace-sync-v1`
