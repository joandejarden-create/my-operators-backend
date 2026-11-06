# Production deploy guard

Hard gates for production uploads. **Sole live entry point is GitHub Actions.**

## Canonical live entrypoint

GitHub Actions workflow **Dealality production release**  
(`.github/workflows/dealality-production-release.yml`)

See [DEALALITY_GITHUB_ACTIONS_PRODUCTION_RELEASE.md](./DEALALITY_GITHUB_ACTIONS_PRODUCTION_RELEASE.md).

```bash
gh workflow run dealality-production-release.yml \
  -f product=AVA \
  -f approved_sha=<40hex> \
  -f authorization_phrase=I_AUTHORIZE_PRODUCTION_RELEASE \
  -f dry_run=true
```

## Local dry-run only

```bash
npm run deploy:production:dry-run
npm run preflight:railway-raw   # documents the block
```

Do **not** use raw `railway up` for production. Emergency bypass (skips gates):

```bash
DEALALITY_ALLOW_RAW_RAILWAY_UP=1 railway up --detach
```

## Policy

| Rule | Enforcement |
|------|-------------|
| Live deploy channel | `DEALALITY_RELEASE_CHANNEL=github-actions` (Actions only) |
| Exact SHA authorization | workflow input + preflight |
| Concurrent releases | Actions concurrency queue (`cancel-in-progress: false`) |
| Shared lock root | required for live path inside Actions runner |
| Dirty / wrong tree | preflight + `deploy:production` guards |
| Incomplete product tree | `assert:production-assets` before upload |
| Postdeploy smoke | runs unless `DEALALITY_SKIP_POSTDEPLOY_SMOKE=1` |
| Never auto-cancel Railway | workflow policy + coordinator `RAILWAY_BUSY` |

## What the Actions path runs

1. Phrase + SHA validation
2. Clean checkout of `approved_sha`
3. Release lock under runner temp shared root
4. Coordinator → `npm run deploy:production` (live) or dry-run exit
5. Share-contract continuity check
6. Lock release / FAILED recovery note (no Railway cancel)

## Recommendation

- **Canonical:** GitHub Actions workflow for all ADP/AVA/GDI production releases.
- **Local Cursor:** dry-run and readiness only; never live `railway up`.
