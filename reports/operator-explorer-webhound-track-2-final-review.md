# Webhound Track 2 — Final Review

**Session:** `6695f5be-443b-4685-860a-b9c0b37e5be6`  
**Name:** OE Calibration-01 Track 2 Assignments + Management Scope  
**Status:** `done=true` · `budget_complete` · `output_ready=true`  
**Cost:** ~$9.84 / $10.00  
**Rows:** 96 (15 `company_scope` + 81 `assignment`)  
**Saved:** `data/operator-explorer/webhound/track-2-session-6695f5be-rows.json`  
**Classification:** `data/operator-explorer/webhound/track-2-validation-classify.json`

## Timing note

Session completed **after** Research Graduation Airtable writes finished. Partial rows were correctly deferred during Waves 01–02 and graduation. This review does **not** auto-merge into Airtable or change Record Purpose.

## Companies covered (15)

Hilton, Accor, IHG, Marriott, Hyatt, Minor, Sonesta, Four Seasons, Rosewood, Mandarin Oriental, Barceló, Radisson Hotel Group, Meliá, Auberge, Shangri-La.

## Validation classification (heuristic vs live Assignments)

| Class | Count | Notes |
| ----- | ----: | ----- |
| Unique useful (candidate) | 27 | Named current-ish rows not obviously already in Assignments |
| Duplicate | 11 | Overlap with existing Property Names |
| Enhancement | 43 | Announced / pipeline / multi-deal status — useful but not Auto-Publish current |
| Conflict | 0 | No hard auto-flagged RHG/CALA collisions in this pass (still apply manual Choice/Americas review for Radisson) |
| Weak source | 0 | Under this filter; many trade-press URLs still need primary confirmation before write |
| Irrelevant (company_scope) | 15 | Scope/BMC context rows — not Assignment creates |

## Publication policy

- Do **not** merge announced/pipeline rows as Current Assignments without status = Announced / Upcoming.
- Prefer official brand/operator URLs over trade press when both exist.
- Radisson rows remain under the Research RHG EMEA/APAC scope decision — do not treat Americas geography as RHG Production evidence.
- Entity resolution must map Webhound `company_canonical` → existing Masters (e.g. Hilton → `Hilton (Managed)`).

## Merge recommendation

**Hold merge** pending founder approval of a supplemental **Webhound Track 2 validation write plan**.

Suggested follow-up threads (optional separate runs / tasks):

1. Validate the 27 unique-useful current candidates against primary sources and CALA priority.
2. Extract announced pipeline as Announced assignments only for graduated Track 2 brands.
3. Radisson row audit against Choice Americas vs RHG EMEA/APAC Master scope.

## Impact on Record Purpose

**None.** Graduation decisions stand. Webhound must not change Purpose automatically.
