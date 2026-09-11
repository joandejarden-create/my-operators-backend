# PR #40 vs production diff audit

Audited comparison: `origin/main`, PR #40 head, and live production hotfix tip.

## Tips

| Ref | SHA / ID | Notes |
|-----|----------|--------|
| `origin/main` tip | `f509b87` | ADP PR #43 — **HI golden-demo NOT on main** |
| PR #40 head | `339aab4` | branch `hotfix/restore-mexico-hi-share-onto-main` |
| Live production deploy | `2222ebdc` | hotfix restore Golden Four HI share after ADP wipe |

## PR #40 status

- **MERGEABLE**
- **gitleaks:** pass
- **Needs approving review** (cannot self-approve)
- Contains: HI share restore + Research Center CSS fix `97b126d` + merge of main ADP
- **Verdict:** `NEEDS_UPDATE` — add durability scripts + drawer visual gate from ADP feature branch before or immediately after merge
- **HI asset content itself:** READY for main

## Missing on PR #40 vs ADP feature branch

These landed on the ADP feature branch but are **not** on PR #40 head:

1. `test-hotel-explorer-share-drawer-visible.mjs` / `npm run test:hotel-explorer-share-drawer-visible`
2. `assert-production-assets` / `deploy:production` durability packet (manifest + guards + runbook scripts)
3. ADP V91 Casas commits after main merge (`2cb68e3` and related) — production is currently the hotfix tip, so **HI is current**; **Casas ADP registry may have been wiped** when the hotfix redeployed

## Production vs main

- Production is **not** equal to `origin/main`.
- Production is running the **hotfix tip** (HI restored).
- `main` still lacks HI golden-demo → `MAIN_ASSET_COMPLETENESS = FAIL` until PR #40 merges (and durability follow-ups land).

## Recommended merge sequence

1. Approve + merge PR #40 (HI onto main).
2. Immediately land durability packet: `assert:production-assets`, `deploy:production`, drawer visual gate.
3. Reconcile ADP Casas / share-registry state if wiped by hotfix redeploy.
4. Only then enable main-driven Railway GitHub deploy.
