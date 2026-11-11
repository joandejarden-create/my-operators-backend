# Dealality development and release runbook

`main` is the integration branch. The active application source is only `C:\Dev\dealality\my-operators-backend`. The consolidated checkout `C:\Dev\dealality\my-operators-backend-consolidated-20261111` is an archived recovery snapshot and is not the working application. Future GDI and ADP changes start in the original directory, are tested, and are merged into `main` before development continues. Full GDI E2E remains not verified until a later certification says otherwise. Code on `main` is not that certification.

## Branch workflow

1. Start from current `origin/main`.
2. Use a short-lived feature branch.
3. Commit source in logical groups. Leave `.env`, share tokens, hotel stores, feedback, and generated reports uncommitted.
4. Run `npm run dealality:dev-release-guard` and the tests for the paths you changed.
5. Open a pull request. Required CI is the existing gitleaks check plus the path workflows already on the branch.
6. A feature is complete when it is on `main` and its checks passed. Do not leave completed source only in an untracked file or a private worktree.
7. Production deploy stays on the existing manual workflow and must name the commit. This runbook does not authorize a deploy.

## Storage

Operational trees stay where they are until the approved migration in `docs/dealality-operational-data-boundary-v1.md`. `GDI_DATA_ROOT` can point local runs at an external copy. The default root remains `data/group-demand-intelligence`.

## Checks

```bash
npm run dealality:dev-release-guard
npm run test:adp-canonical-31-certification-posture-v1
npm run test:dealality-operational-data-boundary-v1
npm run test:gdi-full-e2e-certification-readiness-v1
npm run test:gdi-local-published-inventory-mirror-v1
npm run test:gdi-customer-publication-invariants-v1
npm run test:gdi-universal-need-date-intelligence-v1
npm run test:gdi-research-audit-admin-relocation-v1
```

## Rollback

The pre-alignment checkout, including local data, is in `C:\Dev\dealality\authorized-backups\primary-cutover-backup-20261111`. Git refs are in `C:\Dev\dealality\authorized-backups\primary-cutover-20261111.bundle`. Restore by copying the backed-up paths. Do not use `git reset --hard` or `git clean`.
