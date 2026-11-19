# HE-INTEL-001 P2 — Founder completion report (base correction)

**Status:** `AWAITING_INTEGRATION_CONTROLLER` — STOP before merge  
**PR:** [#422](https://github.com/joandejarden-create/my-operators-backend/pull/422)  
**Correction:** HI graph moved to HPC base `appCCUsuGsE1ifoLk`

## Correct HPC base verification

- Base `appCCUsuGsE1ifoLk` contains Hotel Property Census `tbl9aY5ijiuIzzWam`
- Schema ensure refuses forbidden base `appa2cE7FTRmIbB32`

## Five new table IDs (HPC base)

| Table | ID |
| --- | --- |
| HI Organizations | `tbliXEfUOWk32AN9y` |
| HI Hotel–Org Relationships | `tblGiwN0foXXDESvZ` |
| HI Org Contacts | `tbldJAj0nK1l39JZL` |
| HI Contact Channels | `tblqC7C4ypy5Zojyw` |
| HI Ownership Research Runs | `tblGwk1YefumCf2eu` |
| HI Graph Evidence (new, HPC-side) | `tblc9JhKT1edr4grE` |

Links: Org self-parent; Relationship→Org + →HPC; Contact→Org; Channel→Person/Org; Run→HPC; GraphEvidence→HPC.

HPC field count increased by reciprocal link fields only (expected). Owner/Operator/Radar Status not intentionally modified.

## Evidence architecture

**Decision:** `HI Graph Evidence` on HPC base for graph-entity provenance.  
**Preserved:** ADP `Hotel Intelligence Evidence` `tblHxS8x0niDEGVsh` — not deleted/mutated.

## Old five-table deletion

| Result | Detail |
| --- | --- |
| Emptiness recheck | All five empty |
| Evidence preserved | Yes |
| API DELETE | **FAILED** — Meta API `DELETE /tables/{id}` → `404 NOT_FOUND` |
| Tables still on wrong base | Yes (manual UI deletion required) |

**Blocker:** Airtable API cannot delete tables with current token/API. Founder should delete the five empty misplaced tables in the UI on `appa2cE7FTRmIbB32`. Do not delete Evidence.

## Tests

- `test:he-intel-001-p2-airtable-graph-v1` — PASS (includes base gate)
- `test:he-intel-001-research-engine-v1` — PASS
- CI: update on push of corrected HEAD

## WIP preservation

Unrelated dirty HE/ADP artifacts not committed.

---

**STOP** — no merge until founder review (esp. manual deletion of misplaced tables).
