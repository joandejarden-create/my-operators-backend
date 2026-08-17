# Operator Explorer — Phase 1 Founder Approvals

**Decision date:** 2026-08-10  
**Status:** APPROVED for controlled Airtable foundation apply + calibration seed  
**Branch / commit at authorization:** `app-shell-left-nav` / see pre-apply baseline  

## Scope lock

This approval authorizes Phase 1 Operator Explorer **Airtable foundation + calibration seed only**.

It does **not** authorize:

- Operator Fit scoring / v2.1 changes  
- Owner pilot enablement  
- My Deals wiring  
- Public Operator Explorer UI build  
- Blind overwrite of Master derived summary fields  
- Deletion of Test Fixture Masters  

## Approvals

| # | Item | Decision |
| - | ---- | -------- |
| 1.1 | Record Purpose (`Production` / `Research` / `Test Fixture`) | **APPROVED** |
| 1.2 | `Operator Intelligence - Assignments` table | **APPROVED** |
| 1.3 | Assignment schema (minimal / calibration-stressed) | **APPROVED WITH MINIMALISM** |
| 1.4 | Typed `Operator Intelligence - Brand Relationships` | **APPROVED** |
| 1.5 | Brand Managed Capability (scoped; not project approval) | **APPROVED** |
| 1.6 | Market Presence remains geographic SoT (+ minor fields) | **APPROVED** |
| 1.7 | Claims remains claim spine (+ minor extensions) | **APPROVED** |
| 1.8 | Claims ↔ PI Source Library linkage | **APPROVED** |
| 1.9 | Operating Model axis | **APPROVED** |
| 1.10 | Management Availability axis | **APPROVED** |
| 1.11 | Create 10 new Operator Masters (provisional Track 2) | **APPROVED** |
| 1.12 | Calibration seed for 27 entities | **APPROVED** |
| 1.13 | Explorer readiness policy (separate from Fit) | **APPROVED** |
| 1.14 | Fit Data Readiness diagnostic only | **APPROVED** |
| 1.15 | Publication + wave exception policy | **APPROVED** |
| 1.16 | Phase 1 Airtable apply under write-plan controls | **APPROVED** |

## Explicit non-creates

Do **not** create duplicate Masters for MxM, HMS, AccorHotels nomenclature, IHG management aliases, NH (under Minor), Iberostar Managed twin, or individual hotel brands.

## Record Purpose mapping authority

36 existing Masters: use classification from `reports/operator-explorer-current-universe-audit.md` + calibration Record Purpose recommendation.

- Nine Beta/Dummy → `Test Fixture` (retain; exclude from production universes)  
- Research Stage (Álvarez, Tremun, AADESA) → `Research`  
- Production Real (including brand-managed core 5) → `Production`  
- Real — Research Required → `Production` (real companies; research incomplete)  
- New Track 2 Masters → `Research` initially  

## Apply controls

1. Verified backup before writes  
2. Exact write plan before mutations  
3. Conflict holdouts skip affected facts only  
4. Schema validate before seed  
5. Airtable-backed profile payloads after seed  
6. Stop before Fit / owner / My Deals work  

## Supersedes

Architecture dry-run “no writes” constraint for this Phase 1 apply only. Calibration research dry-run artifacts remain the seed authority unless a validated Webhound merge is documented.
