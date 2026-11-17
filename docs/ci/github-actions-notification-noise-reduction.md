# GitHub Actions notification noise reduction

**Status:** Permanent repository practice  
**Contract:** `dealality-ci-precheck-notification-noise-v1`

## Problem

During normal Cursor development, intermediate PR commits often fail CI (especially gitleaks false positives). Cursor then fixes and merges successfully, but the founder still receives emails for the earlier failed runs.

## What the repository can do

| Control | Effect |
|---|---|
| Local `npm run dealality:ci-precheck` | Catch gitleaks + missing workflow concurrency before PR open/update |
| Path-filtered workflows | Skip unrelated heavy suites on docs-only / unrelated diffs |
| `concurrency` + `cancel-in-progress: true` on PR workflows | Cancel superseded runs so late failures are not compounded by obsolete jobs |
| Narrow `.gitleaks.toml` allowlists | Reduce known-safe metadata FPs without disabling secret detection |
| Cursor rule `dealality-ci-precheck-discipline.mdc` | Agents must precheck before opening avoidable-failing PRs |

## What the repository cannot do

**GitHub Actions email notifications are controlled by each GitHub user’s notification settings.**

Repository workflows, Actions permissions, and code **cannot guarantee** suppression of GitHub’s built-in failure emails.

### Required manual setting (founder)

1. Open [GitHub notification settings](https://github.com/settings/notifications)
2. **System** → **Actions**
3. Choose **Don't notify** (or email only on workflows you watch)

Optional: watch the repository for Issues/PR conversations without Actions email.

## Workflow inventory (this repo)

| Workflow | PR | Push main | Manual | Superseded cancel |
|---|---|---|---|---|
| Secret scan (PR) / gitleaks | yes | yes | no | yes |
| GDI regression recovery baseline | path-filtered | path-filtered | no | yes |
| GDI Research Audit admin relocation | path-filtered | path-filtered | no | yes |
| ADP Admin workspace navigation | path-filtered | path-filtered | no | yes |
| GDI published inventory mirror | path-filtered | path-filtered | no | yes |
| Dealality production release | no | no | yes | **no** (`cancel-in-progress: false`) |
| Census Overnight Enrichment | no | no | yes | n/a |

## Remaining legitimate failures (still email if notifications enabled)

- Real secrets or high-entropy credentials in commits
- Genuine test regressions
- Missing required configuration for a live deploy / overnight job
- First push of a PR that still fails a required check (precheck missed)

Do **not** disable gitleaks, mark secret scan `continue-on-error`, or remove required merge checks to hide these.

## Commands

```bash
npm run dealality:pr-check-suggest -- --base origin/main
npm run dealality:ci-precheck
npm run test:dealality-ci-precheck-notification-noise-v1
```
