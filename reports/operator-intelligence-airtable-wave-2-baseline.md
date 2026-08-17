# Operator Intelligence — Airtable Wave 2 Pre-Write Baseline

**Date:** 2026-08-04  
**Branch:** `app-shell-left-nav`  
**Commit:** `3c88c0b`  
**Feature flag:** `OPERATOR_FIT_ENGINE_V2=0` (off)

---

## Tests run

| Suite | Result |
| ----- | ------ |
| `test:operator-fit-v2` | Pass |
| `test:operator-fit-readiness` | Pass |
| `test:operator-intelligence-calibration` | Pass |
| `operator-fit-v2-shadow-comparison` | Pass |
| Phase 5E | Pass |
| Companies | Pass |
| OAS snapshot page | **2 pre-existing FAIL** (out of scope) |

---

## Calibration records (local)

`data/operator-intelligence/calibration-cohort/` — 6 operators, 14 sources, 11 claims, 15 comparables, 2 exceptions.

## Proposed schema operations

1. Ensure `Partner Intelligence - Source Library` (reuse)  
2. Create `Operator Intelligence - Claims` (minimum claim spine)  
3. Optional fields on Case Studies: `Why Comparable`, `Comparability Strength` (if missing)  
4. Reuse `Operator Setup - Brand Relationships` (already ensured in repo)

## Proposed record operations (Group A + Cenote)

| Operator | Field | Proposed | Taxonomy note |
| -------- | ----- | -------- | ------------- |
| Arbor | Active Countries | Mexico | **Skip** United States (not in approved options audit) |
| GHL | Active Countries | Colombia, Peru, Chile, Panama | Skip Ecuador/Guatemala if not in live options |
| Playa | Active Countries | Mexico, Jamaica, Dominican Republic | OK |
| Aimbridge | Active Countries | Mexico | OK |
| Aimbridge / GHL / Playa | Management Structures | Full third-party management (+ Franchise support for GHL) | Skip Owner-Operated (not in options) |
| Cenote | Active Countries | Mexico only (normalize) | Remove unsupported 7 countries |
| HE | Active Countries | Skip overwrite | Already populated |
| Cohort | Case Studies / Sources / Claims | Additive create | Dedup by property name + operator |

## Current readiness (pre-write, overlay)

Ranking Ready ≥1 project type: Arbor, HE, Playa, Aimbridge, GHL (project-specific). Cenote Conditional.

Real deals (overlay): A=1, B=0, C=4 Ranking Ready.

## Backup / rollback plan

1. Export Master + Platform + Commercial (+ Case Studies linked) for 6 IDs to `backups/operator-intelligence/<timestamp>/`  
2. Rollback = PATCH restored field snapshots  
3. New tables: archive/hide; stop writers  

## Protected

`scoreOperatorMatchForDeal`, Brand Match v2, owner intake, My Deals wiring, feature flag default, ODR-as-shortlist, OAS weights.
