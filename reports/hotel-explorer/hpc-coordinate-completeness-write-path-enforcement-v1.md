# HPC Radar Status write-path enforcement audit v1

**Contract:** `HPC_OPEN_PIPELINE_COORDINATE_COMPLETENESS_GATE_V1`  
**PR:** #403  
**Airtable writes:** none in this task

## Question

Which active workflows can set HPC `Radar Status` to Open or Pipeline, and do they invoke the coordinate gate before writing?

## Findings

| Pathway | Writes `Radar Status`? | Gate invoked? | Notes |
| --- | --- | --- | --- |
| Census Autopilot allowlisted apply | **No** | N/A (forbidden) | `Radar Status` added to `AUTOPILOT_FORBIDDEN_FIELDS`; also absent from `AUTOPILOT_ALLOWED_WRITE_FIELDS` |
| `production-census-write` VIC import path | **No** | N/A | Does not set `Radar Status` |
| First-pass enrichment radar patches | **No** | N/A | Writes `Radar Display Status` / geography fields — **not** `Radar Status` |
| Shared guard `guardHpcRadarStatusPatch` | Guard only | **Yes** (when used) | Blocks Open/Pipeline patches lacking valid coordinates; strips status from sanitized patch |
| Manual Airtable UI | **Yes (possible)** | **No** | Outstanding enforcement gap — UI edits bypass Node guards |
| Ad-hoc scripts / raw Airtable API | **Possible** | **Only if they call the guard** | No in-repo writer currently sets `Radar Status` Open/Pipeline |

## Covered by PR #403

1. Shared validator + activation evaluation (`evaluateOpenPipelineActivation`).
2. Explicit Autopilot forbid of `Radar Status`.
3. Reusable write guard for any future Node writer (`guardHpcRadarStatusPatch`).
4. Admin/Radar diagnostics for existing Open/Pipeline exceptions (status preserved).

## Outstanding gaps (explicit)

1. **Manual Airtable UI** can still set Open/Pipeline without coordinates — cannot be enforced from this repo alone (Airtable interface / automation rules would be required).
2. **No existing automated Open/Pipeline writer** was found to wrap; permanent enforcement for *future* Node writers requires adopting `guardHpcRadarStatusPatch` (documented; not yet every generic Airtable helper).
3. Claiming “permanent enforcement” from the validator library alone would be incorrect — Autopilot forbid + guard adoption + UI gap disclosure is the accurate state.

## Status preservation

Existing Open/Pipeline records (including Radisson + UMusic) are **not** demoted by this PR. Exceptions remain flagged until founder-authorized coordinate apply.
