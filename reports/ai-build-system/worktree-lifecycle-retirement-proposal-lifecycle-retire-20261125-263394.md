# Worktree lifecycle retirement proposal

**Batch ID:** `lifecycle-retire-20261125-263394`
**Created:** 2026-11-25T14:51:03.394Z
**origin/main:** `2f07e0012a790dd69f442e703745e1b87df86c3f`
**Worktrees inventoried:** 146

## Classification summary

- **PERMANENT:** 5
- **ACTIVE:** 10
- **RETIREMENT_CANDIDATE:** 2
- **PRESERVATION_REQUIRED:** 129
- **BLOCKED:** 0

## Proposed next batch (2) — LOW risk

| # | Path | Stream | Recoverable (est.) | Junctions |
|---|---|---|---|---|
| 1 | `C:/Dev/dealality/_ic-main-batch2` | integration | 3293830679 | 0 |
| 2 | `C:/Dev/dealality/gdi-release-main/.worktrees/ai-admin-menu-fix` | gdi | 2795535179 | 0 |

**Estimated recoverable (batch):** 6089365858 bytes

## Safety

- Never auto-delete
- Never `git worktree remove --force`
- Junction targets preserved
- Permanent workspaces preserved
- Branches managed separately

## Founder approval

Reply with exactly:

`AUTHORIZE_WORKTREE_LIFECYCLE_RETIRE_LIFECYCLE_RETIRE_20261125_263394`

Then run:

```bash
npm run dealality:worktree-lifecycle -- retire --proposal reports/ai-build-system/worktree-lifecycle-retirement-proposal-lifecycle-retire-20261125-263394.json --founder-approve "AUTHORIZE_WORKTREE_LIFECYCLE_RETIRE_LIFECYCLE_RETIRE_20261125_263394"
# review dry-run, then:
npm run dealality:worktree-lifecycle -- retire --proposal reports/ai-build-system/worktree-lifecycle-retirement-proposal-lifecycle-retire-20261125-263394.json --founder-approve "AUTHORIZE_WORKTREE_LIFECYCLE_RETIRE_LIFECYCLE_RETIRE_20261125_263394" --apply
```

## Recommended next historical cleanup batch

| # | Risk | Path | Dirty | Blockers |
|---|---|---|---|---|
| 1 | LOW | `C:/Dev/dealality/_ic-main-batch2` | 0 | - |
| 2 | LOW | `C:/Dev/dealality/gdi-release-main/.worktrees/ai-admin-menu-fix` | 0 | - |
| 3 | MEDIUM_AFTER_REPORT_PRESERVE | `C:/Dev/dealality/my-operators-backend-gdi-permanence-main` | 1 | UNCOMMITTED_CHANGES, UNPROTECTED_CONFIGURATION |
| 4 | MEDIUM_AFTER_REPORT_PRESERVE | `C:/Dev/dealality/my-operators-backend-pr235` | 1 | UNCOMMITTED_CHANGES, UNPROTECTED_CONFIGURATION |
| 5 | MEDIUM_AFTER_REPORT_PRESERVE | `C:/Dev/dealality/wt-gdi-portfolio-qualified-opportunities-v1` | 1 | UNCOMMITTED_CHANGES, UNPROTECTED_CONFIGURATION |
| 6 | MEDIUM_AFTER_REPORT_PRESERVE | `C:/Dev/dealality/wt-ic-gdi-portfolio-serve-main` | 1 | UNCOMMITTED_CHANGES, UNPROTECTED_CONFIGURATION |
| 7 | MEDIUM_AFTER_REPORT_PRESERVE | `C:/Dev/dealality/_ava-v4-prod-cutover-e1eec3ee` | 2 | UNCOMMITTED_CHANGES |
| 8 | MEDIUM_AFTER_REPORT_PRESERVE | `C:/Dev/dealality/wt-radisson-6seg` | 2 | UNCOMMITTED_CHANGES, UNPROTECTED_CONFIGURATION |

## Hold groups (not auto-retired)

- PRESERVATION_REQUIRED: 129
- lowRiskAfterPreservationAck: 0
- BLOCKED: 0
- ACTIVE: 10
- PERMANENT: 5
