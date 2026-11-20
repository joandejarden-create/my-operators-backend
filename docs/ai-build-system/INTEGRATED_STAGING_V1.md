# Dealality Integrated Staging V1

**Status:** Founder-review gate — do not cut over `:8080`, merge, or deploy without explicit GO.  
**Canonical URL (after cutover):** `http://localhost:8080/app#/`  
**Canonical checkout:** `C:\Dev\dealality\ws-integrated-staging`  
**Home branch:** `ws/integrated-staging`

## Architecture

One integrated Dealality environment for founder review:

| Layer | Role |
|-------|------|
| Product worktrees (`ws-gdi`, `ws-adp`, `ws-hotel-explorer`, `ws-brand-explorer`) | Feature development on isolated ports only |
| Integration Controller (`my-operators-backend`) | Merges + governance; must not be the everyday founder UX |
| **Integrated Staging** (`ws-integrated-staging`) | Sole owner of port **8080**; main + explicitly staged feature commits |
| Production (Railway / live) | Separate approval + deploy controls — **never** auto-updated by staging refresh |

Staging is isolated from production. Refresh never publishes, never runs destructive migrations, and never modifies source product worktrees.

## Port 8080 ownership

Only these checkouts may bind `PORT=8080`:

1. Basename `ws-integrated-staging`, or presence of `.dealality-integrated-staging` marker
2. Legacy primary basenames `my-operators-backend` / `my-operators-backend-canonical-main` (until cutover completes)

**Denied:** all product `ws-*` feature homes — even if `DEALALITY_CANONICAL_CHECKOUT=1`.

Developers may run `DEALALITY_ALLOW_WORKTREE_SERVER=1 PORT=8091 node server.js` (or similar) for debugging. Founder review does not require those ports.

## Feature integration procedure

### Merged changes (main)

```bash
npm run dealality:integrated-staging -- --mode refresh-main
npm run dealality:integrated-staging -- --mode refresh-main --apply
npm run dealality:integrated-staging -- --mode validate --apply
```

### Unmerged feature (explicit only)

```bash
npm run dealality:integrated-staging -- --mode stage-feature --feature-ref <branch|sha> --feature-label "<name>"
npm run dealality:integrated-staging -- --mode stage-feature --feature-ref <branch|sha> --feature-label "<name>" --apply
npm run dealality:integrated-staging -- --mode validate --apply
```

On conflict: merge aborts; `KNOWN_GOOD.json` is retained. Incompatible branches are never silently combined.

A successful staging integration does **not** authorize merge to `main` or production deploy.

## Safe refresh / rollback

```bash
# Status (read-only)
npm run dealality:integrated-staging -- --mode status

# Bootstrap checkout (does not start :8080)
npm run dealality:integrated-staging -- --mode bootstrap
npm run dealality:integrated-staging -- --mode bootstrap --apply

# Cutover plan only — requires founder GO before any process stop/start
npm run dealality:integrated-staging -- --mode cutover-plan

# Rollback plan (staging checkout only; never product trees / production)
npm run dealality:integrated-staging -- --mode rollback-plan
```

Process rules:

- Do not terminate unknown `:8080` owners.
- Do not reset/clean/stash-discard product WIP.
- Replace temporary hold wrappers with this CLI + staging checkout server start after founder cutover approval.

## Environment Status panel

- API: `GET /api/admin/integrated-staging/status` (admin auth)
- UI: injected on `/app.html` when serving from the staging checkout (or `?stagingStatus=1`)
- Shows: environment, base main commit, staged feature commits, last refresh, server/validation status, merged vs staging-only vs not-integrated
- Staging-only features are labeled **NOT PRODUCTION**

## Registry (IC only)

Product chats must **not** edit `docs/ai-build-system/worktree-registry.json`.  
Proposed entry: `docs/ai-build-system/integrated-staging-registry-proposal-v1.json` — Integration Controller applies in a tiny governance PR.

## Production separation

| Action | Staging | Production |
|--------|---------|------------|
| Refresh main into staging | Yes | No |
| Stage unmerged feature | Yes (explicit) | No |
| Bind `:8080` | Staging only | N/A |
| Railway / live deploy | Never automatic | Existing approval controls |

## Initial verification case

Merged Surfe Usage (PR #493): Admin Resources → Surfe Usage must remain available alongside GDI, ADP, Hotel Explorer, and Brand Explorer surfaces after staging bootstrap/validate.

## Tests

```bash
npm run test:dealality-integrated-staging-v1
```
