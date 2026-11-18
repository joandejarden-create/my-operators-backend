# Global identity and link integrity

## Shared demand identity

- Contract: `gdi-shared-demand-identity-v1` (PR #366 lineage)
- **Valid:** One global Demand Generator reused across hotels with **distinct** hotel GDO rows
- **Invalid:** Same opportunity ID on two hotels, or hotel ID mismatch on shared ID

## Dry-run isolation sample

From `MULTI_HOTEL_DRY_RUN_SYNC_MANIFEST.json`:

```json
"sharedIdentitySample": {
  "ok": true,
  "checked": 0,
  "note": "NO_SHARED_DG_IN_SAMPLE",
  "isolationContractPresent": true
}
```

No cross-hotel DG collision surfaced in the current eight-hotel opportunity sample; isolation assertions remain wired in planner tests.

## Rock Creek (global venue spine)

- Canonical venue: `pev_53b689545914754d`
- Structured pilot ID `pev_42542bf58aac936b` has **zero** Hotel Venue Fit or Private Event Signal links
- All live links attach to domain ID — link integrity favors domain-canonical remediation

## Opportunity-level conflicts (multi-hotel)

| Hotel | Opportunity ID | Issue |
|-------|------------------|--------|
| Radisson | `rx_d_f5c3bd3d073a` | SHARED_IDENTITY_CONFLICT — organization name mismatch |
| Bethesda | `gdi_opp_series_ctn_annual_conference_2027` | Org name variant (NIDA Clinical Trials Network vs CTN shorthand) |

Both classified **CONFLICT** — no automated UPDATE.

## Privacy

- `neverCopyHotelPrivateFeedbackToGlobal: true` in storage authority manifest
- Planner does not emit Decisions/DecisionEvents into DG/GDO sync operations