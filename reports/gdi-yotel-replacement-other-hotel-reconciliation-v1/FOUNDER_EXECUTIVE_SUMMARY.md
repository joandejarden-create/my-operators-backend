# YOTEL delete-and-replace + other-hotel reconcile — founder summary

**STOP FOR FOUNDER REVIEW — NO DELETIONS OR BULK WRITES EXECUTED**

## Corrective status

The earlier reconcile-pack language that treated YOTEL’s 108 Airtable GDOs as **historical inventory to preserve** is **SUPERSEDED**. See `SUPERSEDED_YOTEL_POLICY.md` and `PREVIOUS_PROMPT_EXECUTION_STATUS.md`.

**Final direction:** YOTEL = delete-and-replace; every other hotel = reconcile-and-preserve.

## What already existed (reused, not restarted)

PR #368 already contained:

1. Multi-hotel reconcile V2 audit + Rock Creek finding (`f30a862ea`)
2. YOTEL delete/replace preparation manifests (`e4996a4ed`)

This corrective turn: live re-verified no Airtable mutations; added supersession docs + required deliverable aliases; updated contradictory YOTEL retention wording.

## Live verification (this turn)

| Check | Result |
|---|---|
| YOTEL Airtable GDOs | **108** still present |
| Manifest opportunity IDs vs live | exact match |
| Unexpected mutations | **none** |
| Local research baseline | **42** |
| Qualified GDO creates from 42 | **0** |
| Research-only (filesystem) | **42** |

## YOTEL proposed production migration (NOT authorized yet)

1. Delete exactly the IDs in `YOTEL_EXACT_DELETE_MANIFEST.json` / `YOTEL_108_RECORD_DELETE_MANIFEST.json`
2. Keep all 42 on filesystem Research Preview as authoritative baseline
3. Create GDO rows only when individually commercially eligible (currently **0**)
4. Never delete shared DG/Program/Signal/Venue; never touch other hotels; never change publication packages

## Other hotels

| Hotel | Policy | Status |
|---|---|---|
| Radisson | Reconcile | FS 66 / AT 36; 1 org-name conflict; no wholesale delete |
| Guadalajara | Reconcile | FS 43 / AT 0; CREATE candidates 0 |
| Wyndham | Preserve | 82 / 82 UNCHANGED |
| Castillo | Preserve | 20 / 20 UNCHANGED |
| Bethesda / Nobu / Waterstone | Preserve & reconcile | covered in V2 pack; not wholesale replace |
| Rock Creek | Separate one-field Venue ID restore to `pev_53b689545914754d` | prepared; not part of YOTEL apply |

## Authorization

See `PRODUCTION_MIGRATION_AUTHORIZATION_REQUEST.md`. This corrective prompt is **not** authorization to execute deletion.
