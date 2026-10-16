# Deploy status (Full GDI E2E contract)

| Item | Status |
|------|--------|
| PR #118 (initial E2E) | merged @ `9a576c8e` |
| PR #119 (fail-closed evaluator) | merged @ `e6d9a609` |
| Railway lean pack | `.railwayignore` V2: GDI reports PDF-only; marketing CDN source textures excluded; `.venv`/tmp scratch ignored. Estimated upload **~173 MB** (was ~252 MB / prior fail ~379 MB). |
| Pre-deploy gates | `assert:production-assets` PASS (45 files / 7 surfaces) |
| `npm run deploy:production` | *Pending from clean main worktree after lean PR merges* |
| Customer inventory | **unchanged** (shared coordinator/contract code only) |
| Active Ren / Tampa / ADP | **not interrupted** |

**Deploy policy:** clean `main` only via `npm run deploy:production`. No dirty/hotfix override for this release.
