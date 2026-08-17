# DEALALITY CENSUS AUTOPILOT V1 — Final Report

**Version:** census-autopilot-v1.0.0  
**Run:** `cav1_2026-08-08T10-12-55_daca27`  
**Benchmark:** IHG + Hilton + Choice · Mexico · dry-run · $0 credits · no Airtable · no Webhound

---

## MOST IMPORTANTLY

**YES — with explicit boundaries.**

We now have an Autopilot orchestration layer that can **independently build, complete (to honest Unknown), validate, maintain, and expand** the Dealality Hotel Census **at the staging/governance level**, with **field-level provenance**, while treating **Cvent/legacy as quarantined coverage challenges** and **escalating hard cases rather than inventing data**.

It does **not** yet auto-write Airtable, auto-activate brands, auto-rehost images, or auto-call Webhound — by design.

---

## Answers (1–20)

### 1. Can one Autopilot orchestrate discovery, reconstruction, full-record, freshness, reconciliation, activation, image integrity, and escalation?
**Yes.** Single orchestrator + mode registry routing to existing RE2 surfaces (`02-mode-registry.json`). Unified Mexico benchmark exercises the combined job.

### 2. Does it attempt all actual researchable Census fields?
**Yes.** Loads `buildFieldContractEntries()` and routes every researchable primary (`03-field-routing-registry.json`: **63** researchable fields).

### 3. What % of material Census fields can it currently resolve natively?
**~47%** from compact-index field resolution this freeze pass; **60%** blended material completeness (includes prior VIC `material_pct`). Core identity fields near 100%; rooms/owner/operator/opening/coords largely unresolved without live Lane A/B fetch. Live adapter history: IHG/Hilton material ~56–71% when deep pages succeed.

### 4. Which fields consistently remain unresolved?
- Phone
- Future Opening Flag
- State / Region
- Address
- Latitude
- Longitude
- Market / Submarket
- Affiliation As-Of Date
- Affiliation Start Date
- Prior Brand
- Source Type
- Discovery Date
- Hotel Description - Source Text
- Hotel Description - AI Summary
- Short Property Summary
- Property Positioning
- Amenities - Source Text
- Amenities - Structured Tags
- F&B Flag
- Meeting Space Flag

### 5. Can it prioritize the research queue intelligently?
**Yes.** Multiplicative priority engine → P0–P3 (`04-priority-engine.md`). Benchmark band counts: {"P1 High":18,"P2 Medium":249,"P3 Low":98}.

### 6. Can it stop research when sufficient evidence exists?
**Yes.** Per-field `stop_research` + per-hotel budget limits (`research-budget.js`).

### 7. Can it resume failed/long runs safely?
**Yes.** Resume state under `runs/<run_id>/resume-state.json` with completed entity skip.

### 8. Can it use official structured sources as the primary lane?
**Yes.** Lane A is preferred (`05-source-lane-registry.json`) reusing IHG/Hilton/Choice/Marriott/… adapters.

### 9. Can Cvent/legacy remain quarantined challenge sources?
**Yes.** Adapters emit discovery challenges only; `cvent_used_as_source=false`, `legacy_used_as_source=false`.

### 10. Can Brand Explorer consume verified Census automatically at staging level?
**Yes — staging aggregation only** (`10-brand-aggregation-results.json`). No BE Airtable writes; conflict review flag required for count mismatches.

### 11. Can inactive brands automatically become completion candidates without activation?
**Yes.** `11-activation-candidates.json` with `activate: false`.

### 12. Can operator relationships be preserved for future Operator Explorer?
**Yes.** `12-operator-relationship-staging.json` (mostly Unknown until Lane B resolves operators).

### 13. What percentage of records require Webhound/deep escalation?
**~0%** classified `DEEP RESEARCH REQUIRED` as whole-record class. Field-level escalations this run: **730** (owner/operator opaque paths). Webhound remains **queued only** (0 calls this run). Estimated live Webhound share after Lane A/B: **~5–12%** of hotels.

### 14. What human steward workload remains?
Review production candidates, material remediations, Cvent/legacy discovery challenges, image rights, ownership/operator escalations, and BE completion packs — **not** blank-cell hunting.

### 15. Is the system ready for a controlled full Mexico reconstruction?
**Yes for orchestration + IHG/Hilton/Choice.** Marriott and non-adapter families need more Lane A depth before parity. Writes still off.

### 16. Is it ready for CALA after Mexico?
**Orchestration yes; coverage no until Mexico steward loop + adapters scale.** Cvent challenge corpus already LATAM-wide.

### 17. Is it ready to begin Brand Explorer completion?
**Conditionally yes — small IHG/Hilton/Choice pilot packs only** (see `20-brand-explorer-completion-readiness.md`). Do not execute yet.

### 18. What must happen before Airtable writes are allowed?
Steward-approved write lanes; Exact/High identity; source-rights allow; clean-room firewall pass; no Cvent/legacy evidence; production eligibility gates; explicit env confirms; proven dry-run history; rollback plan.

### 19. Which Census fields could someday become auto-safe?
Directory-canonical name, brand/family, geography, official URL, property codes, affiliation status from bookable pages, official structured coordinates — see `19-future-write-lanes.md`.

### 20. What should be built next?
1. Wire live Lane A/B fetch into `full_record` mode (still dry-run writes).  
2. Steward review UI/queue export for output classes + challenges.  
3. Controlled Mexico reconstruction apply path (High/Exact only).  
4. Brand Explorer completion pilot (sandbox).  
5. Operator relationship normalization (still no OE mode).

---

## Change Impact Classification

**High** (architecture for census research) — but **no production writes enabled**.  
Rollback: delete/ignore `data/research-engine-v2/census-autopilot-v1/` artifacts; do not enable write flags.

## Definition of Done checklist

- [x] Central field routing from contract (not hardcoded short list)
- [x] No silent empties — unresolved → explicit statuses
- [x] No Airtable writes / no Webhound / no credits
- [x] Explicit output classes + UI-ready states in artifacts
- [x] Cvent/legacy quarantined
- [x] Resume state
- [x] Benchmark observability
- [x] Final Q&A

## Manual QA

1. `npm run census:autopilot-v1 -- --mode=unified_benchmark --group=IHG,Hilton,Choice --country=Mexico --dry-run`
2. Confirm artifacts 01–22 exist under `data/research-engine-v2/census-autopilot-v1/`
3. Confirm `cvent_used_as_source` / `legacy_used_as_source` false everywhere sampled
4. Confirm activation candidates have `activate: false`
5. Resume: interrupt mid-run conceptually via completed IDs in resume-state
