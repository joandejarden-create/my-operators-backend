# PR #40 final reconciliation vs live production

**Date:** 2026-09-11  
**Production deploy:** `08b72ffc` (message: `deploy:production coexistence HI+ADP (gated tree) 5465352`)  
**PR #40 head (after reconciliation):** `b86ce91`  
**main tip:** `f509b87`  
**PR URL:** https://github.com/joandejarden-create/my-operators-backend/pull/40

## Verdict

After bringing live-only ADP V91 + golden-demo static deps onto PR #40:

**PR40_CONTAINS_ALL_LIVE_FIXES = PASS**  
**PR40_NOT_STALE_VS_MAIN = PASS** (merge-base = `f509b87` = main tip; no commits on main not in PR)  
**NO_LIVE_ONLY_HOTFIX = PASS** (for critical production surfaces reconciled below)  
**PR40_READY_TO_MERGE = PASS** (gates green; merge blocked only by required approving review)

## A / B / C comparison

| Axis | Ref | Notes |
|------|-----|--------|
| A Production | Railway `08b72ffc` from working tree SHA `5465352` | HI + ADP + Brand/Operator 200; drawer visual 4/4 |
| B PR #40 | `hotfix/restore-mexico-hi-share-onto-main` @ `b86ce91` | HI share + durability + ADP V91 Casas + print/book shell deps |
| C main | `f509b87` | ADP trusted release PR #43; **no** HI golden-demo until PR #40 merges |

## Initial LIVE_ONLY gaps (found → fixed)

Before update, production SHA `5465352` had content **not** on PR #40 `4ddf4ef`:

| Item | Status now |
|------|------------|
| `2cb68e3` ADP V91 BPP RANK_ONLY + Casas published | Cherry-picked → `59fd35a` |
| `owner-ai-demand-share.html` v91 cache bust | Hash match vs `5465352` |
| `ai-demand-positioning.js` V91 | Hash match |
| `bpp-customer-published-v1.json` | Hash match |
| `adp-share-registry/active-tokens.json` | Hash match |
| `hotel-intelligence-golden-demo.html` UTF-8 | Checked out from `5465352` → `c005b70` |
| `dealality-report-print-chrome.css/js` + `dealality-book-shell.js` | Added → `b86ce91` |

### LIVE_ONLY after reconciliation

```
LIVE_ONLY_FILES: NONE (critical production surfaces)
LIVE_ONLY_COMMITS: NONE required for healthy HI+ADP+share coexistence
LIVE_ONLY_ROUTE_CHANGES: NONE
LIVE_ONLY_CSS: NONE
LIVE_ONLY_API_CHANGES: NONE
```

Note: ADP branch still has a large divergent history of checkpoint commits; those are **not** required to reproduce current healthy production once V91 + HI + durability are on PR #40.

## MAIN_ONLY_CHANGES_AT_RISK

```
NONE — git log origin/hotfix..origin/main is empty.
```

## Pre-merge gates (PR #40 worktree)

| Gate | Result |
|------|--------|
| `npm run assert:production-assets` | **PASS** (after static deps) |
| `npm run test:production-routes-local` | **PASS** |
| `npm run test:production-critical-surfaces` | **PASS** (live Railway) |
| `npm run test:hotel-explorer-share-drawer-visible` | **PASS** 4/4 |
| Incomplete tree block | **PASS** (assert fails when golden-demo static deps / HI assets missing) |

## Manifest coverage

`config/production-required-assets.json` covers HOTEL_INTELLIGENCE, ADP, BRAND_EXPLORER, OPERATOR_EXPLORER, BRAND_AI, PLATFORM_CORE including Research Center CSS + golden-demo + critical APIs.

## Raw deploy policy

- **STANDARD:** `npm run deploy:production`
- **RAW `railway up`:** EMERGENCY / UNSAFE WITHOUT GATES (`npm run preflight:railway-raw` / docs)

## Founder action

```
PR #40 STATUS:
READY_TO_MERGE

FOUNDER ACTION:
Approve PR #40
Merge PR #40 into main
```

Do not bypass branch protection. After merge, run guarded deploy from **main** only.
