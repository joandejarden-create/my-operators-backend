# Rock Creek Mansion — non-destructive remediation plan

**Status:** Planned only — **not applied in this phase**

## Objective

Align structured Venue ID with domain-canonical identity and embedded JSON, without splitting links or creating a duplicate venue row.

## Proposed authorized write (single record)

**Table:** Private Event Venues  
**Record:** `recf35Q9B6jpX3Ivm`

| Action | Field | From | To |
|--------|-------|------|-----|
| UPDATE | Structured Venue ID | `pev_42542bf58aac936b` | `pev_53b689545914754d` |
| VERIFY | Venue Payload JSON `venueId` | `pev_53b689545914754d` | unchanged (already correct) |

## What we will not do

- Create a second Private Event Venue row
- Re-key Hotel Venue Fit or Private Event Signals (already on domain ID)
- Merge unrelated venues or change lodging classification
- Run this update without explicit founder authorization in a named write manifest

## Preconditions (founder gate)

1. Approve `ROCK_CREEK_STRUCTURED_ID_REMEDIATION_DECISION` in `FOUNDER_DECISIONS_REQUIRED.md`
2. Include this op in an exact write-set allowlist (not a broad sync)
3. `authorizeProductionWrites=true` + `apply=true` on persistence adapter only after allowlist review
4. Mandatory read-back: structured ID, payload `venueId`, website, address

## Regression guards (already in code)

- `NEVER_REWRITE_VENUE_ID_FROM_GEO_ONLY_WHEN_DOMAIN_KNOWN`
- `MATCH_EXISTING_BY_DOMAIN_OR_NAME_BEFORE_CREATE`
- `PATCH_STRUCTURED_AND_PAYLOAD_TOGETHER_WHEN_AUTHORIZED`

## Rollback posture

Single-field revert on structured ID if read-back fails; links do not need relinking because they already reference `pev_53b689545914754d`.