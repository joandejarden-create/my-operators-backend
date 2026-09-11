# Packet: production durability completion report

**Packet date:** 2026-09-11  
**Scope:** Document + gate status for Railway production durability (HI wipe → hotfix restore → PR #40 path).  
**Out of scope:** Europe / Webhound content.

## Executive verdict

Production HI is healthy again on hotfix deploy `2222ebdc`, but **`main` is still incomplete** (no HI golden-demo at `f509b87`). PR #40 (`339aab4`) is MERGEABLE and HI-content-ready, yet **NEEDS_UPDATE** for durability scripts + drawer visual gate before treating main-driven deploy as safe.

**Recommendation:** KEEP CLI WITH HARD GUARDS short-term; MOVE TO MAIN-DRIVEN after PR #40 merges and Railway GitHub deploy can target `main` safely.

## Live incident during this packet

After HI hotfix `2222ebdc`, an ADP-only restore `3d8aeb75` ("P0 restore ADP V91…") wiped HI again (golden-demo / research CSS / hotel-explorer.js → 404).  

Coexistence restore via gated tree: deploy `08b72ffc` — **HI + ADP + Brand/Operator share all HTTP 200** (postdeploy smoke PASS).

## Audited production snapshot

| Item | Fact |
|------|------|
| Project | `serene-reverence` |
| Service | `my-operators-backend` |
| Environment | `production` |
| URL | https://my-operators-backend-production.up.railway.app |
| Deploy ID (current coexistence) | `08b72ffc` |
| Prior HI-only wipe | `3d8aeb75` (ADP restore) after `2222ebdc` (HI hotfix) |
| Method | manual CLI `railway up` (not historical GitHub auto-deploy from main) |
| Root cause of wipe | CLI replace of entire service with incomplete tree → peer product 404s |

## Gate checklist

| Gate | Status | Notes |
|------|--------|-------|
| `MAIN_ASSET_COMPLETENESS` | **FAIL** | Until PR #40 merges; `origin/main` `f509b87` lacks HI golden-demo |
| `PR40_MERGE_READINESS` | **BLOCKED** | Needs approving review (cannot self-approve); MERGEABLE + gitleaks pass |
| `PR40_HI_CONTENT` | **PASS** | HI share + Research Center CSS fix ready for main |
| `PR40_DURABILITY_PACKET` | **FAIL** | Missing vs ADP branch: assert/deploy durability scripts; drawer visual gate |
| `PRODUCTION_HI_HEALTH` | **PASS** | Hotfix tip restore; Golden Four HI share live |
| `GOLDEN_FOUR_REGRESSION` | **PASS** (prior restore) / **DEFERRED** re-run | Not re-run in this packet; production healthy after restore verification |
| `SHARE_DRAWER_VISUAL` | **PASS** (prior restore) / **DEFERRED** re-run | Gate script exists on ADP branch; not on PR #40; not re-run in this packet |
| `ADP_CASAS_REGISTRY_AFTER_HOTFIX` | **BLOCKED** / unknown risk | May have been wiped when hotfix redeployed; reconcile after main path stable |
| `CLI_HARD_GUARDS` | **PASS** (in tree) | `assert:production-assets`, `deploy:production`, `preflight:railway-raw`, manifest |
| `MAIN_DRIVEN_RAILWAY_GITHUB` | **BLOCKED** | Only after PR #40 + durability follow-up + safe main targeting |

## Required follow-ups

1. Approve + merge PR #40.
2. Land durability packet onto main (if not already via parallel branch): manifest gates, `deploy:production`, drawer visual test.
3. Reconcile ADP Casas / share-registry if wiped by hotfix upload.
4. Re-run Golden Four + drawer visual against production once main is the deploy source.
5. Enable main-driven Railway GitHub deploy; keep CLI as guarded emergency only.

## Canonical commands (now)

```bash
npm run assert:production-assets
npm run test:production-routes-local
npm run test:production-critical-surfaces
npm run deploy:production
npm run deploy:production:dry-run
npm run assert:hotel-explorer-share-assets
npm run test:hotel-explorer-share-drawer-visible
npm run preflight:railway-raw
```

## Docs in this packet

See [README.md](./README.md).
