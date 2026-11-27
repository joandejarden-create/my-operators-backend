# Integration Controller merge gate — PR #613

**Stream:** GDI (`C:\Dev\dealality\ws-gdi`)  
**At:** 2026-11-27  

## Corrected head SHA

| Claim | SHA |
|---|---|
| Outdated founder note | `df1da264a86706e31d7125b0f2c4057ebebfae54` (superseded) |
| Prior closeout typo | `82191ddc5b7b920210a125ee5fb1cd145f67f896` (older) |
| GitHub head before live-adapter push | `d153ecdba4ca63de859fcc26f165bddfa663bd28` |

Latest HEAD advances after the live-adapter + gitleaks-fix push on this PR.

## Merge authority

- Integration Controller **only** merges PR #613 into `main`.
- GDI product chat does **not** merge.
- After merge: `npm run dealality:post-merge-sync -- --mode post-merge --stream gdi --pr 613` then `--apply`.
- Preserve unrelated dirty worktree files; no parallel worktrees.

## Paid execution (separate gate)

Proposed token `FOUNDER_GO_WYNDHAM_WHO_HOW_V3_TRANCHE_20261127` remains **PROPOSED**.  
Paid Stage 3 requires: merge + home sync + $0 adapter tests PASS + explicit founder GO.
