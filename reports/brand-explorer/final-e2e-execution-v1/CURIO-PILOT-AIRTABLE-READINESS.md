# Curio Pilot — Airtable Readiness (pre-write)

**Brand:** Curio Collection by Hilton · `receQkxgjlezsc1xg`  
**Candidate version:** `phase2h-component-parity`  
**Content hash (presentation blocks):** `c0cc375b048668a2ace9acce4bb395f27ea2b63067a2bd80607f427e6430a089`  
**Airtable writes this cycle:** **0**

## Write destinations (canonical)

| Destination | Table | Write class |
|-------------|-------|-------------|
| Customer-facing copy | Brand Setup - Brand Explorer Presentation | founder_publish (after gates) |
| Identity | Brand Setup - Brand Basics | read / preserve — no wipe |
| Footprint metrics | Hotel Property Census | system_owned — read for validation |
| Brand Setup children | Brand Setup - * | only where ownership allows |

## Intended procedure (when gates clear)

1. Immutable backup of all Presentation rows for `receQkxgjlezsc1xg`
2. Exact field-level write plan (update by recordId; create only for new slotKeys)
3. Concurrency / identity check
4. Apply Presentation writes only (staged boundary)
5. Readback every written field
6. Render Compare Studio LEFT from Airtable readback vs locked candidate hash
7. Promote customer only after readback PASS
8. Rollback = restore backup snapshot; do not leave partial customer profile

## Remaining Gate 2 / publication blockers (genuine)

| Item | Blocks write? | Notes |
|------|---------------|-------|
| Gate 1 visual/functionality | **Cleared** (this cycle artifact) | Does not auto-approve content |
| Scorecard `FOUNDER_APPROVED` | **Yes** | Still `NOT_FOUNDER_APPROVED` |
| Manifest completeness outstanding (8 areas) | **Yes for full profile** | Guest/owner, loyalty, gallery, similar brands, etc. |
| Research gaps (11) | Warning / slot-level | Do not publish incomplete slots as finished |
| Activation `productionWritesAuthorized` | **Yes** | Still false |
| Write plan recordId binding | **Yes until verified** | Prior dry-run risked mass creates when baseline IDs missing |
| Ascend | N/A | Kept separate (`priorityBlocked=1`) |

## Pilot readiness verdict

**READY_FOR_CONTROLLED_WRITE_PLANNING** — **NOT** ready for production apply.

Next: after PR merge + sync, generate Curio write plan from live Airtable Presentation rows (dry-run) with Gate1=approved and scorecard still fail-closed until founder explicitly clears write-eligible slots.
