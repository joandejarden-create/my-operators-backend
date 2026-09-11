# Production deploy packet

Founder-facing docs for Dealality production durability on Railway (`my-operators-backend`). Facts audited 2026-09-11. Do not invent Railway dashboard settings beyond what is recorded here and in repo config.

| Doc | Purpose |
|-----|---------|
| [CURRENT_PRODUCTION_DEPLOY_ARCHITECTURE.md](./CURRENT_PRODUCTION_DEPLOY_ARCHITECTURE.md) | Live Railway target, CLI upload model, builder/start |
| [PRODUCTION_REQUIRED_ASSETS.md](./PRODUCTION_REQUIRED_ASSETS.md) | Manifest surfaces + gate commands |
| [PR40_VS_PRODUCTION_DIFF_AUDIT.md](./PR40_VS_PRODUCTION_DIFF_AUDIT.md) | PR #40 vs main tip vs live hotfix |
| [PRODUCTION_BRANCH_DURABILITY.md](./PRODUCTION_BRANCH_DURABILITY.md) | Why branch-scoped uploads wipe peers |
| [RAILWAY_STATIC_ASSET_AUDIT.md](./RAILWAY_STATIC_ASSET_AUDIT.md) | How static + sendFile assets ship |
| [PRODUCTION_DEPLOY_GUARD.md](./PRODUCTION_DEPLOY_GUARD.md) | Hard guards and override env vars |
| [PRODUCTION_DEPLOY_RUNBOOK.md](./PRODUCTION_DEPLOY_RUNBOOK.md) | Canonical deploy / hotfix steps |
| [PRODUCTION_SMOKE_MATRIX.md](./PRODUCTION_SMOKE_MATRIX.md) | Post-deploy smoke checklist |
| [PACKET_PRODUCTION_DURABILITY_COMPLETION_REPORT.md](./PACKET_PRODUCTION_DURABILITY_COMPLETION_REPORT.md) | Gate status + recommendation |

**Canonical commands:** `npm run assert:production-assets`, `test:production-routes-local`, `test:production-critical-surfaces`, `deploy:production`, `deploy:production:dry-run`, `assert:hotel-explorer-share-assets`, `test:hotel-explorer-share-drawer-visible`, `preflight:railway-raw`.

**Manifest:** `config/production-required-assets.json`
