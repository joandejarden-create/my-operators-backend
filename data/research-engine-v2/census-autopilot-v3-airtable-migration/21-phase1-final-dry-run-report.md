# V3 Phase 1 — Final Dry Run Report

**Run ID:** `cav3_2026-08-08T15-04-05-566Z`  
**Writes executed:** **NO**  
**Hard gates:** **ALL PASS**

## Schema
1. Production Census master: **Hotel Property Census** (`tbl9aY5ijiuIzzWam`)
2. Census fields: **116**
3. Golden Priority fields mapping directly: see `02-golden-to-airtable-field-map.json`
4. Schema changes required: **None for pilot** (Verified lifecycle states proposed as research-side; do not auto-add Airtable fields)
5. Must not write: linked **Hotel Property Brand Affiliations / Source Evidence / Steward Review**; no formula fields present

## Pilot
6. Evaluated: **1021**
7. Eligible selected: **150**
8. NEW_INSERT: **189** (in pool) / inserts proposed **115**
9. EXACT_EXISTING_MATCH: **832**
10. HIGH_EXISTING_MATCH: **0**
11. POSSIBLE_DUPLICATE: **0**
12. IDENTITY_CONFLICT: **0**

## Field classes (row counts)
13. AUTO_WRITE_SAFE: **2212**
14. CORROBORATED_WRITE: **690**
15. STEWARD_REVIEW: **345**
16. FIRST_PARTY_VALIDATION: **150**
17. BLOCKED_RIGHTS: **1050**
18. PROHIBITED: **0**

## Provenance / Firewalls
19–23. Provenance failures: **0** (target 0)
24. Cvent-derived production values proposed: **0**
25. Legacy-derived production values proposed: **0**
26. Either firewall fail? **NO**

## Rooms
27. Pilot hotels with Rooms written: **0**
28. Rooms Pending: **150**
29. Rooms Unknown blocked Verified? **NO** — state `VERIFIED — ROOMS PENDING`
30. Rooms inferred? **NO**

## SerpApi
31. Fields relying solely on SerpApi in proposed writes: **0**
32. Persistence classification: **NOT APPROVED** (downstream-use review / clarification pending)
33. Blocked field rows: **1050**
34. Same-property official fields still writable: yes (identity/geography/URL/brand)

## Dry run
35. Proposed inserts: **115**
36. Proposed updates: **35**
37. Blank fills: **337**
38. Contradictions (steward): **160**
39. Temporal changes (steward): **36**
40. Blocked writes: **1050**
41. Steward-review writes: **495**

## Safety
42–48. Gates: cvent_firewall=PASS, legacy_firewall=PASS, provenance_gate=PASS, duplicate_gate=PASS, source_rights_gate=PASS, rooms_not_inferred=PASS, rooms_unknown_does_not_block_verified=PASS, pre_write_snapshot_complete=PASS, rollback_payload_complete=PASS, no_linked_record_writes=PASS

## Phase 2 NOT run
Artifacts 22–30 deferred until Joan authorizes.

---

## AUTHORIZATION GATE — STOP BEFORE WRITE

Hard gates: **PASS**

| Item | Count |
|------|------:|
| Records to write (Pilot A then B) | **150** (A=25, B+=125) |
| Inserts | **115** |
| Updates (blank-fill) | **35** |
| Fields (insert field keys avg) | ~**22** per insert |
| Rooms Pending | **150** |
| SerpApi-blocked field rows | **1050** |
| Duplicate/conflict in auto cohort | **0** (excluded from auto) |
| Rollback ready | **YES** (`20-rollback-plan.json` + snapshot) |

### To authorize Phase 2 (Joan only)

```bash
# Explicit run-level gate — do NOT set until ready
set ENABLE_VERIFIED_CENSUS_WRITES=1
npm run census:autopilot-v3-airtable-migration -- --phase2
```

Phase 2 will process Pilot A (25) then Pilot B only if Pilot A circuit breakers stay clean.
**Per-property approval is not required** once this env gate is set.

Do **not** enable this gate until you have reviewed this dry-run.
