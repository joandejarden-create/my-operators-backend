# Dealality — CI prechecks & notification noise reduction V1

## Objective

Reduce avoidable GitHub Actions failure emails during Cursor development **without** weakening security checks.

## Recent failure patterns (repo-wide sample)

Dominant recent failures were **Secret scan (PR) / gitleaks** on intermediate PR commits (YOTEL observability, Toronto packages, Radisson certification, Sheraton, Wyndham, ADP share activation). Secondary: occasional GDI regression failures on intermediate pushes. Final commits and merges typically succeeded — founder still received earlier failure emails.

Root causes:

1. No local gitleaks precheck before PR push.
2. No cancel-in-progress on PR workflows → superseded runs finished as failures.
3. Recurring generic-api-key FPs on feature-flag labels / env-name documentation / research metadata.
4. GitHub emails every failed workflow conclusion (user setting).

## Corrections

| Change | Detail |
|---|---|
| Local precheck | `npm run dealality:ci-precheck` — gitleaks over `origin/main..HEAD` + concurrency sanity |
| Cursor discipline | `.cursor/rules/dealality-ci-precheck-discipline.mdc` (+ GDI merge rule updated) |
| PR concurrency | All PR-triggered workflows cancel superseded runs |
| Gitleaks | Narrow allowlists for `FLAG=0/1` labels and bare env *names*; defaults remain on |
| Admin CI | Static preflight asserts Execution Monitor tab wiring |
| Docs | `docs/ci/github-actions-notification-noise-reduction.md` |

## Workflows optimized

- `secret-scan-pr.yml`
- `gdi-regression-recovery-baseline.yml`
- `gdi-research-audit-admin-relocation.yml`
- `adp-admin-workspace-navigation.yml`
- `gdi-local-published-inventory-mirror.yml`

**Not cancelled:** `dealality-production-release.yml` (`cancel-in-progress: false`).

## Required manual GitHub setting

GitHub Settings → Notifications → System → Actions → **Don't notify**.

Repo code cannot suppress Actions emails.

## Remaining legitimate failures

Real secrets, real test regressions, authorized deploy/overnight misconfig — these must still fail CI.

## GitHub

| Item | Value |
|---|---|
| PR | [#338](https://github.com/joandejarden-create/my-operators-backend/pull/338) |
| CI | PASS (Admin, GDI recovery, Research Audit, Spice, gitleaks) |
| Merge status | MERGED |
| Merge commit | `47b3f959640736bf411184b9d45afae69f41ffc5` |
| `main` synchronized | Confirmed |

**Manual action still required:** GitHub Settings → Notifications → System → Actions → **Don't notify**.
