# Production sync activation proposal

**Status:** Proposal only — **STOP FOR FOUNDER REVIEW**

## Current posture

| Guard | State |
|-------|--------|
| Production writes | **Blocked** |
| CREATE candidates (8 hotels) | **0** |
| Qualified UPDATE candidates | **1** (Bethesda GDO — not in PR #367 scope) |
| Estimated ops in manifest | **1** (Bethesda UPDATE only) |

Bulk multi-hotel GDO sync is **not** proposed.

## Recommended next pilot (minimal)

### Bundle A — Rock Creek venue alignment (1 record)

- Table: Private Event Venues
- Record: `recf35Q9B6jpX3Ivm`
- Change: structured Venue ID → `pev_53b689545914754d` (payload already aligned)
- See: `ROCK_CREEK_NON_DESTRUCTIVE_REMEDIATION_PLAN.md`

### Bundle B — Bethesda GDO commercial correction (optional, separate decision)

- Opportunity: `gdi_opp_accp_annual_meeting_20261001`
- Change: priority HIGH → DISQUALIFIED (filesystem-grounded)
- Requires: conflict-free read + explicit field allowlist

### Explicitly deferred from next pilot

- Any YOTEL / Radisson / Guadalajara CREATE
- Nobu AT-only import (65 rows)
- Guadalajara 43 FS-only rows
- Automated resolution of 2 org-name CONFLICTs without founder merge policy

## Activation prerequisites

From `MULTI_HOTEL_DRY_RUN_SYNC_MANIFEST.json` `nextProductionPilotRequires`:

1. `FOUNDER_EXPLICIT_ALLOWLIST`
2. `ROCK_CREEK_STRUCTURED_ID_REMEDIATION_DECISION`
3. `PER_HOTEL_QUALIFIED_CREATE_UPDATE_SET`
4. `authorizeProductionWrites=true`
5. `exactWriteSetAuthorization=<validated manifest>`
6. `apply=true`

## Success criteria for next apply

- Zero write failures
- Read-back matches manifest per op
- Re-run planner → **UNCHANGED** for all applied IDs
- No customer publication or CURRENT pointer change
- No unauthorized GDO creates