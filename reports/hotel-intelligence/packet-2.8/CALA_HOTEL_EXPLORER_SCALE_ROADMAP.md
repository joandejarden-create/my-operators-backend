# CALA_HOTEL_EXPLORER_SCALE_ROADMAP

> Packet 2.8 · Factory scale strategy · **No batch research execution in this packet**  
> Census baseline: **5,956** Hotel Property Census hotels · Corpus spend to date: **~$25.4** across **5** Webhound runs on **4** golden hotels

## 1. Current state

| Layer | Status |
|-------|--------|
| Golden learning corpus | 4 hotels · 5 Webhound runs · frozen `HOTEL_INTELLIGENCE_LEARNING_CORPUS_V1` |
| Learning registry | **PRESENT** — 5 CANDIDATE records (`RESEARCH_LEARNING_REGISTRY.json`) |
| Completeness engine | **PRESENT** — `completeness-evaluator.js` |
| Research planner | **PRESENT** — native-first ladder (`research-planner.js`) |
| Hotel Explorer assembler | **PARTIAL** — shared stub; golden four still on property-specific cohorts |
| Prompt compiler (2.8A) | **PRESENT** — `compileWebhoundPrompt` + `prompt_hash`; gate PASSES |
| `finding_id` binding | **COMPLETE** on all Full HI + C&O |
| 25-hotel pilot | **SELECTED** (CONFIRMED_IDs) — research **DEFERRED** for founder review |
| Webhound default | Escalation / learning tool (L5) — not default hotel research |

## 2. Four-hotel learnings (inputs to factory)

- Adjacent-asset / similar-name collision screens  
- Announced ≠ current brand conversion  
- Operator ≠ owner; PropCo candidate ≠ deed  
- MX listed pubco playbooks (KGPV)  
- Private opaque + government tourism order (Cambridge)  
- MX private PropCo via adhesion/PROFECO (Sheraton — CANDIDATE)  
- Same-asset reflag + package owner ≠ PropCo (voco)  
- Native reproduction today: **~30% YES / 45% PARTIAL / 25% NO**

## 3. Research architecture (target)

```
CALA Census hotel_id
  → Canonical hotel resolution
  → Hotel Explorer Assembler (shared)
  → Domain resolvers
  → CompletenessEvaluator (domain statuses)
  → planHotelResearch (L0–L4 native first)
  → Webhound escalation only if material + authorized (L5)
  → LearningRegistry CANDIDATE → eval → playbook promotion
```

Webhound prompts must only come from `ResearchInvestigationSpec` → `compileWebhoundPrompt`.

## 4. Playbooks

| Class | Status |
|-------|--------|
| COMMON_RESEARCH_PRIMITIVES | Encoded across negative screens, preamble, planner triggers |
| MX-PUBCO | READY patterns from KGPV |
| PRIVATE_OPAQUE / tourism order | Coded triggers; Cambridge CANDIDATE |
| MX-PRIVATE-PROPCO (PROFECO) | CANDIDATE — promote after eval |
| SAME_ASSET_REFLAG / package owner | CANDIDATE |
| CHANGE_OPPORTUNITY conversion slip | CANDIDATE |
| Learning auto-promote | **FORBIDDEN** |

## 5. Owner / operator / market reuse strategy

Before hotel-by-hotel deep research:

1. Cluster census by owner, operator, brand, market  
2. Research high-leverage organizations once  
3. Cascade Organization / Operator / Brand / Market / People intelligence  
4. Property-specific edges only at hotel grain  

Pilot cluster seeds (strategy only — see Packet 2.8A reuse analysis): Aimbridge LATAM, Alliance/HNF voco package, Faranda Colombia, ALG/Hyatt AI, Iberostar/Barceló–Occidental DR, soft-brand Caribbean/CA ring.

## 6. Scale waves

| Wave | Scope | Goal | Gate |
|------|-------|------|------|
| **WAVE 0** | All 5,956 | Hotel Explorer shell from structured Census + existing Dealality data | Always on |
| **WAVE 1** | High-confidence branded | Max domains from existing data / Brand Explorer / relationships | Completeness snapshot |
| **WAVE 2** | Organization clustering | Research major owners/operators once; cascade | Reuse KPIs |
| **WAVE 3** | Native gap fill | Ownership / operator / people / development via L1–L4 | Native resolution rate |
| **WAVE 4** | External escalation | Hard/opaque material gaps only | Founder-authorized queue |

**Do not** require Full Intelligence dossiers for every Census hotel before Explorer is useful.

## 7. Checkpoint 25 (Packet 2.8A)

| Item | Status |
|------|--------|
| Stratified selection | **DONE** — 25/25 CONFIRMED_ID |
| Golden four excluded | **YES** |
| Generic assembly pass | **DEFERRED** |
| Native research pass | **DEFERRED** |
| Webhound escalations | **0** (empty queue pending founder) |

## 8. Checkpoint 250

Only after 25-hotel gate passes. Measure: field completeness, critical-claim precision, false merges, contamination, cost, escalation rate, time/hotel, owner/market/people reuse.

## 9. Checkpoint 2,500

Only after 250 stable. Focus: batch reliability, queue recovery, cost controls, source caching, rate limits, QA sampling.

## 10. Full CALA (remaining after 2,500)

Hotel Explorer for every Census hotel. Completeness varies by evidence. **No false 100%.**

## 11. External research strategy

| Rule | Policy |
|------|--------|
| Default | Native L0–L4 |
| Webhound | Exception — material unresolved after native fail, or customer-purchased Deep Research |
| Budget class | $5 hard cap on standard external investigation |
| Learning | Every escalation → CANDIDATE learning → native reproduction attempt |

## 12. Cost model (Census = 5,956)

| Scenario | Escalation rate | Projected external spend |
|----------|----------------:|-------------------------:|
| A. $5 every hotel | 100% | **$29,780** |
| B. 25% | 25% | **$7,445** |
| C. 10% | 10% | **$2,978** |
| D. 5% | 5% | **$1,489** |
| E. Org-level hard archetypes only | ~3–5% target | **~$894–$1,489** |

**Recommended target:** org-level escalation **~3–5%**, with native + reuse absorbing the rest. Objective = maximum reliable intelligence per research dollar — not minimum spend alone.

Avoided spend vs scenario A at 5%: **~$28,291**.

## 13. QA / safety gates (every batch)

`NO_CROSS_HOTEL_CONTAMINATION` · `PROPERTY_IDENTITY_EXACT` · `OWNER_ROLE_PRECISION` · `OPERATOR_ROLE_PRECISION` · `CURRENT_HISTORICAL_SEPARATION` · `BRAND_CURRENT_ANNOUNCED_SEPARATION` · `NO_FALSE_PROPCO_MERGE` · `NO_PERSON_FALSE_MATCH` · `SOURCE_PROVENANCE` · `NO_GUESSED_PROFILE` · `NO_INTERNAL_REPORT_METADATA`

Golden regression fixtures: KGPV · Cambridge · Sheraton GDL · voco/Real Inn.

## 14. Completeness tiers (product)

| Tier | Domains |
|------|---------|
| **A — Foundational** | Identity, fundamentals, brand, operator, market, area, demand, access |
| **B — Intelligence** | Ownership, organization, relationships, people, sources |
| **C — Deep** | Transactions, development, capital, decision authority, deep dossier |

UI: one Hotel Explorer template; limited evidence → explicit empty / limited states — not alternate layouts.

## 15. Hard stop

```
NO hotel #5 research
NO Webhound in Packet 2.8 / 2.8A
NO 25-hotel research execution until founder approval
NO 250 / 2,500 / full-CALA launch from this packet
```

Next human gate: review learning comparison → pilot selection → economics → then optional Packet **2.8B** bounded escalation.
