# Operator Explorer — Schema Migration Plan

**Date:** 2026-08-09  
**Status:** Plan only — no destructive migration early

---

## Phase 0 — No-change classification (NOW)

- Freeze understanding via this audit package  
- Classify universe (real / research / dummy)  
- Classify Brand Explorer parents for management capability (BM addendum)  
- Split calibration into Track 1 (third-party) + Track 2 (brand-managed)  
- Enable hide/exclude for dummies in Explorer/Fit loaders  
- Document SoT map (Presence, Claims, Master, Case Studies)  
- Document Candidate Type vocab + Brand Managed Capability relationship type (**design only**)  
- **No Airtable schema changes required to start**

## Phase 1 — Safe additions

- Add Record Purpose (or Test Only) — additive  
- Add Candidate Type on Master/Profile only after founder approves cleaned vocab  
- Optional Claims→PI Source link  
- Optional Presence grain fields  
- Create Assignments table + typed Brand Relationships table (empty) including `Brand Managed Capability` type  
- Extend brand-managed operator link registry aliases (code) before new Masters  
- Dual-write **off** until writers ready  

## Phase 2 — Dual-write / derived validation

- Research waves write Claims + Presence + Assignments  
- Publish summaries to Active Countries / experience flags  
- Compare derived vs manual; exception report on mismatch  

## Phase 3 — Consumer migration

- Fit geo: Presence-first (already preferred in code when records exist)  
- Fit comps: Assignments + Case Study strength  
- Explorer sections read Assignments / typed brand edges  
- Stop new writes to deprecated flat fields  

## Phase 4 — Legacy deprecation

- Platform flat Market Presence Type for scoring  
- bf_* score inputs  
- Opaque geography JSON as SoT  
- Only after dependency map green  

## Phase 5 — Retirement (optional)

- Archive dummy Masters  
- Remove unused commercial fields after binding proof  
- Collapse duplicate soft brand family fields if derived stable  

---

## Rollback posture

Every apply wave: backup manifest → dry-run → apply → post-write validate → rollback notes in report.  
Feature flags keep Fit/owner surfaces off during migration.
