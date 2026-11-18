# Git worktree and branch status

## Cursor workspace

- Path: `C:\Dev\dealality\wt-gdi-isolation-post-365`
- Branch: `feat/gdi-all-hotel-airtable-persistence-completion-v1`
- HEAD base: `2fd417f5c` (PR #368 merge on `origin/main`)

## Primary checkout

- Path: `C:\Dev\dealality\my-operators-backend`
- Branch: `main` @ `2fd417f5c` tracking `origin/main`
- Synchronized with merge of PR #368

## Changes inflation (~144k)

Not unmerged feature-branch source. Cursor Changes inflate from **untracked research artifacts** under this isolation worktree (Full E2E state dirs, association-events runs, scaling runner reports, tmp gitleaks). Short `git status` shows ~11–12 dirty/untracked paths, not 144k edited source lines.

Preserved WIP (do not discard):

- Modified: `data/.../recUOyzOXn2Zdp98I/pursuits.json`, Bethesda reconciliation JSON
- Untracked research state dirs for Radisson / Bethesda / YOTEL
- Untracked report dirs from prior research runs

## Completed feature branch

- `feat/gdi-multi-hotel-airtable-reconciliation-v2` work was squash-merged via PR #368
- Local retirement of that branch is safe when clean; remote tip may linger until pruned
- This isolation worktree remains required for active persistence-completion work + preserved WIP

## ADP

- `C:\Dev\dealality\_adp-atomic-delivery-deploy-wt` remains isolated / untouched

## Safety

- No `git clean`, `reset --hard`, or forced WIP moves
- Obsolete clean deploy worktrees exist elsewhere; retirement deferred unless founder authorizes cleanup of confirmed-unused trees
