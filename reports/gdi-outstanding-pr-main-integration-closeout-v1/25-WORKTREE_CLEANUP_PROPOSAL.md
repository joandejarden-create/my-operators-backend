# Worktree cleanup proposal (separate founder authorization)

**Do not retire in this assignment.**

## Observed (from prior audit + live workspace)

- ~1,381+ dirty paths in `ws-gdi` (preserved; post-merge sync used PRESERVE_THEN_RETURN_HOME).
- ~154 unregistered worktrees historically reported.
- Integration primary: dirty / may be detached — `SKIP_INTEGRATION_DIRTY`.

## Proposed classification batches (future)

| Class | Examples | Action when authorized |
|---|---|---|
| Permanent homes | `ws-gdi`, `ws-adp`, `ws-hotel-explorer`, `ws-brand-explorer`, Integration | Never retire |
| Active unmerged task | HE/ADP detached dirty trees | Fetch-only; preserve |
| Already-integrated GDI feature branches | local `feat/gdi-*` matching merged PRs #598–#678 | RETIREMENT_CANDIDATE after unique-commit check |
| Frozen `_gdi-*-deploy-wt` | depth/discovery/scaling deploy checkouts | RETIREMENT_CANDIDATE when clean + unused |
| Research artifacts / screenshots | `page-*.png`, `reports/gdi-workspace-preserve-*` | Preserve evidence; do not delete blindly |
| Superseded | #578/#496 branches | Keep until founder branch-delete auth |

Use `npm run dealality:worktree-lifecycle -- propose` for exact batches. Retirement requires the exact founder approval phrase per batch; no `git clean`, no `--force` worktree remove, no stash discard.
