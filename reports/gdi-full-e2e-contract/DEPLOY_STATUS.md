# Deploy status (Full GDI E2E contract)

| Item | Status |
|------|--------|
| PR #118 (initial E2E) | merged @ `9a576c8e` |
| PR #119 (fail-closed evaluator) | merged @ `e6d9a609` |
| PR #120 (lean `.railwayignore`) | merged @ `acc380bf` |
| Pack estimate | **~173 MB** (GDI reports PDF-only; marketing CDN textures excluded; `.venv`/tmp ignored) |
| Direct `railway up` from tree | **FAILED** Cloudflare 413 (~340 MB) — CLI pack diverges from ignore estimate |
| Lean-stage upload | **SUCCESS** — `railway up <stage> --path-as-root` from filtered tree |
| Railway deployment | `8cd94aaf-9607-43d5-876f-9feb5902740e` **SUCCESS** |
| Code SHA uploaded | `acc380bf` (main tip at deploy) |
| Postdeploy smoke | **PASS** (health + HI/ADP/Brand/Operator/BAI/GDI share surfaces + Golden Four) |
| Customer inventory | **unchanged** |
| Active Ren / Tampa / ADP | **not interrupted** |

**Durable path:** `npm run deploy:production` now builds `scripts/build-railway-lean-stage.mjs` and uploads with `--path-as-root` unless `DEALALITY_RAILWAY_DIRECT_UP=1`.
