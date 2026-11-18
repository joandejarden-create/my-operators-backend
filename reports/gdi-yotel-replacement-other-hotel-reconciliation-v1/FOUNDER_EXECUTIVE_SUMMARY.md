# YOTEL replacement + other-hotel reconciliation — founder summary

**STOP FOR FOUNDER REVIEW — NO DELETIONS OR BULK WRITES EXECUTED**

## Directive applied

- **YOTEL:** replace obsolete Airtable GDO inventory with current local research baseline (controlled deletion + selective qualified recreate).
- **All other hotels:** non-destructive reconcile / preserve.
- **Rock Creek:** correction prepared, not applied with YOTEL.

## YOTEL verified counts

| Item | Count |
|------|------:|
| Airtable GDO (hotelId-scoped re-query) | 108 |
| Matches prior 108 | true |
| Local baseline opportunities | 42 |
| ID overlap FS∩AT | 0 (replacement, not merge) |
| Qualified GDO create candidates | 0 |
| Research-only (filesystem remains authority) | 42 |
| Out-of-scope rows rejected | 0 |

## Safety

- Exact hotel ID allowlist: `recrPQcZg7SFARRb2`
- Exact delete ID manifest committed
- Redacted full-field backup committed; restore procedure documented
- Shared DG/Program/Signal/Venue **not** in delete set
- Publication STAGED/CURRENT untouched
- Primary WIP preserved

## Other hotels

- Radisson: reconcile — FS 66 / AT 36; conflicts 1
- Guadalajara: reconcile — FS 43 / AT 0; CREATE 0
- Wyndham: preserve — FS 82 / AT 82; UNCHANGED 82
- Castillo: preserve — FS 20 / AT 20; UNCHANGED 20

## Critical migration implication

**Qualified GDO create candidates from the local 42 = 0** under current commercial eligibility (confirmed/reliable demand + credible entry point).

So the authorized YOTEL replacement, when you approve it, is:

1. Delete the exact 108 obsolete Airtable GDO rows.
2. Keep all 42 local opportunities on the filesystem research store as the authoritative baseline.
3. Do **not** bulk-insert the 42 into GDO until individual rows become commercially qualified and are separately allowlisted.

This still achieves the founder goal: remove the obsolete 108 active GDO inventory and make the current local research baseline authoritative without inventing unqualified GDO rows.

## Next

Review `FOUNDER_PRODUCTION_AUTHORIZATION_REQUEST.md` and exact manifests. No production migration until you authorize the delete allowlist (and any future qualified create set).
