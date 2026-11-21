# IC proposal — register HIF V6 Track A / Track B workspaces

**Status:** PROPOSAL ONLY — product chat must not edit `worktree-registry.json`  
**As of:** 2026-11-21  
**Founder auth:** Parallel discovery tracks after merge `9783ff27bc940fcee3b692088353c38674e6e188`

## Provisional worktrees (created for non-destructive parallel research)

| Id | Path | Branch | Role |
|---|---|---|---|
| hif-v6-track-a | `C:/Dev/dealality/ws-hif-v6-track-a` | `feat/hif-v6-track-a-cmbs-owner` | Track A commercial/contract discovery |
| hif-v6-track-b | `C:/Dev/dealality/ws-hif-v6-track-b` | `feat/hif-v6-track-b-regulatory-wave1` | Track B government/regulatory discovery |

## Rules

- Do **not** develop V6 waves in dirty `ws-hotel-explorer` / ADP / GDI / BE trees
- Shared HIF core ownership remains under HE stream; tracks must not conflict on prospect engines
- Integration Controller owns registry edit + sequential PR merges
- Temporary lifecycle until founder promotes to permanent or retires after merge

## Requested IC action

Add both entries under `temporaryWorkspaces` (or permanent if founder prefers), then run `dealality:worktree-registry-status`.
