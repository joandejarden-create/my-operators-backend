# Group Demand Intelligence — Build Rules & Architecture Audit

**Date:** 2026-09-12  
**Mode:** MODE A — BUILD (Dealality Product Builder OS)  
**Pilot hotel:** Bethesda Marriott (`hotelId` = census `recLuxvwwxID7U2B8`; ADP property id `adp_bethesda_marriott` is READ-ONLY reference only)

---

## Guiding principle

> Experiment with the product proposition without creating experimental technical debt.

Architecture, contracts, IDs, research escalation, evidence, APIs, permissions, auditing, and testing follow production Dealality standards. The **page** may stay experimental and separate from ADP; the **system underneath** must be composable later.

---

## Applicable Dealality rules (authoritative)

| Source | Application to GDI |
|--------|-------------------|
| `.cursor/rules/deal-capture-implementation-partner.mdc` | Correctness, no invented schema, UI loading/empty/error, validation before writes, central maps |
| `AGENTS.md` | Census geography authority, no silent production writes, Webhound = Level-5 learning not SoT |
| `docs/ai-build-system/DEALALITY_PRODUCT_CONSTITUTION.md` | Evidence vs interpretation vs action; confidence; no black-box recommendations |
| `docs/ai-build-system/AI_BUILD_PROTOCOL.md` + `CURSOR_IMPLEMENTATION_PROTOCOL.md` | Inspect → plan → implement → test → document learnings |
| `docs/data-intelligence/INTELLIGENCE_GOVERNANCE.md` | Provenance, confidence, no unverified promotion |
| `docs/data-intelligence/DATA_VALIDATION_PROTOCOL.md` | FACT vs derived claims; source handling |
| Hotel Intelligence research policy | External research default OFF; **$5 hard cap** per pilot run (`lib/hotel-intelligence/research/policy.js`) |
| Research escalation policy | Levels 0–4 in `lib/hotel-intelligence/research-methods/escalation-policy.js` |
| ADP methodology governance | **Do not alter** ADP scoring, methodology, published reports, or official observations |
| Design system | Prefer existing intelligence-page patterns; do not expand locked ADP recovery design pack |

---

## Architecture being reused

1. **Hotel Intelligence Research Center** — orchestration, spend-guard, audit-log, Webhound provider, never auto-start paid research on GET  
2. **Research Methods Library** — playbook schema, dispatcher, escalation ladder, readiness graduation  
3. **Evidence / confidence patterns** — append-only evidence, source authority tiers, confidence vocabulary  
4. **Hotel Census read path** — `census-read` / canonical hotel identity (`recLuxvwwxID7U2B8`)  
5. **Market Demand product** — standalone demand page sibling (`/market-demand`) for route/page isolation pattern  
6. **Admin auth** — `memberstackAuth` + `requireDealalityUser` + `requireAdminAccess` (+ local demo admin)  
7. **Feature-flag pattern** — Operator Fit style env default OFF  
8. **ADP Leak Audit isolation pattern** — copy shapes, never mutate ADP published/runtime/baselines  
9. **ADP Bethesda property profile** — READ-ONLY seed for rooms/meeting space/comp set/demand anchors  

---

## Architecture being extended (not forked)

| Extension | How |
|-----------|-----|
| New product namespace | `lib/group-demand-intelligence/`, `api/group-demand-intelligence.js`, `data/group-demand-intelligence/` |
| New research subject grain | **Specific piece of group demand** (event/meeting instance), not hotel-only or org-only |
| New playbook pack | GDI event/association/medical/gov methods — EXPERIMENTAL readiness |
| FACT / INFERENCE claim typing | Explicit enum on GDI evidence rows (aligned with constitution; HI uses VERIFIED/PROBABLE/SIGNAL) |
| Hotel Group Demand Profile | Configuration + census-backed capability profile per `hotelId` |
| Cost ledger | Per-run provider/Webhound/Apify/Ampfy/LLM cost accounting for Admin Audit |
| Experimental Planner Consideration | Separate observation store; may *read* ADP patterns; never writes ADP history |

---

## Explicitly NOT duplicated

| Do not build | Reuse instead |
|--------------|---------------|
| Parallel research engine | HI Research Center + research-methods escalation |
| Parallel hotel identity | Census `hotelId` / canonical hotel |
| Parallel company/org/person SoT | Reference canonical entities; GDI contacts are opportunity-linked only |
| Parallel evidence store for hotels | HI evidence-store patterns; GDI evidence keyed to opportunities |
| Second Ampfy/Apify client stack | Existing Apify local-client + approval gates |
| ADP UI / scoring / published packs | Separate product; ADP read-only if referenced |
| Bethesda/Marriott hardcoded architecture | Pilot config under `config/group-demand-intelligence/hotels/` |

---

## Ampfy / Apify clarification (material)

**Finding:** No `Ampfy` / `AMPFY` integration exists in this repository.  
**Present:** **Apify** is the hotel extraction / Actor integration.

**Decision for V1:**

- Level 4 supplemental enrichment uses an **adapter interface** with:
  - `apify` — existing Dealality Apify stack where useful
  - `ampfy` — reserved provider slot (`provider_not_configured` until credentials/product exist)
- Neither becomes system of record.
- Spec language “Ampfy” is honored as Level-4 supplemental research; implementation maps to Apify when Ampfy is unavailable, with explicit provider labeling.

---

## Risks

| Risk | Mitigation |
|------|------------|
| Invented attendance / room blocks | UNKNOWN / ESTIMATED + evidence required; quality gate rejects invented High Priority |
| Parallel engine drift | GDI orchestrator calls HI escalation/spend-guard; no second Webhound client |
| ADP contamination | Namespace isolation tests; no writes under `data/ai-demand-positioning/` |
| Bethesda hardcoding | Demand priorities are hotel config JSON keyed by `hotelId` |
| Cost blowout | Hard $5 Webhound/run; feature flag; admin-only Run Research; no paid work on page load |
| Weak commercial value | Cap volume 10–25 qualified; prioritize High Priority quality; user feedback loop |
| Ampfy name mismatch | Documented; adapter reserved; Apify used where applicable |

---

## Dependencies

- Census / HI read APIs (hotel identity, rooms)
- HI research policy + Webhound MCP client (optional Level 5)
- SerpAPI / native fetch patterns (Level 2–3)
- Admin auth middleware
- Filesystem persistence under `data/group-demand-intelligence/` (V1; Airtable promotion deferred)
- Feature flag `GROUP_DEMAND_INTELLIGENCE_V1`

---

## Technical decisions

1. **Standalone route** `/group-demand-intelligence` — not inside ADP.  
2. **Canonical key** = Census `hotelId` (`rec…`). ADP property ids are optional aliases only.  
3. **Persistence V1** = versioned JSON under `data/group-demand-intelligence/` (audit-friendly; no destructive migrations).  
4. **No Airtable writes** in pilot unless separately approved.  
5. **Research never auto-runs** on GET/page load.  
6. **Scoring weights** live in config (not UI hardcode).  
7. **Feedback stored** but does not auto-retrain weights.  
8. **Shared event intelligence** designed so the same event can later be evaluated against many hotels without re-researching from scratch.

---

## Approval posture

Founder brief already locks product scope, unit of analysis, scoring weights, escalation order, Webhound $5 cap, and non-integration with ADP.  
**Proceeding to build** unless a blocking technical issue appears (none found).
