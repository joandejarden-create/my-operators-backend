# RESEARCH_TEMPLATE_REGISTRY

> Packet 2.8A · Canonical research template registry documentation  
> Code SoT: `lib/hotel-intelligence/research/templates.js`  
> Registry version: `hotel-intelligence-research-templates-v2`  
> Gate: `npm run test:hotel-intelligence-prompt-compiler-2-8a` **PASSES**

## Principle

There is **one** versioned ResearchTemplateRegistry. Customer / UI / agents must not invent ad-hoc Webhound prose. Specs compile via `compileWebhoundPrompt`.

## Customer / Deep Research templates

| template_id | Alias(es) | Version | Report type | Budget class | Purpose |
|-------------|-----------|---------|-------------|--------------|---------|
| `FULL_HOTEL_INTELLIGENCE` | Full HI Investigation | 1.0.0 | `FULL_INVESTIGATION` | $5 max | Baseline Full Hotel Intelligence |
| `CHANGE_OPPORTUNITY` | — | 1.1.0 | `RESEARCH_ADDENDUM` | $5 max | Why Now / risks / conversion & opportunity |
| `DECISION_AUTHORITY` | Decision Authority & Contact Path | (registry) | Addendum | $5 max | Control / signing / contact paths |
| `BRAND_OPERATOR_AGREEMENT` | `BRAND_FRANCHISE_OPERATOR` | (registry) | Addendum | $5 max | Brand / franchise / management / license |
| `OWNERSHIP_CAPITAL_EVENTS` | `OWNERSHIP_CAPITAL` | (registry) | Addendum | $5 max | PropCo / economic owner / capital events |
| `REPOSITIONING_DEVELOPMENT` | — | (registry) | Addendum | $5 max | Renovation / PIP / development |
| `OWNER_PORTFOLIO` | Owner Portfolio Multi-Asset | (registry) | Addendum | $5 max | Org portfolio / multi-asset relationships |

Each template includes: `customer_question`, `research_objective`, lanes/scope, preferred sources, extraction targets, validation rules, negative screens, stop/escalation conditions, confirmation UI fields, `claim_handoff.auto_promote: false`.

## INTERNAL_GAP_FILL templates (not customer reports)

Narrow, cheaper than Full HI — used by planner for Hotel Explorer completion:

| template_id | Domain focus |
|-------------|--------------|
| `PROPERTY_FUNDAMENTALS_GAP` | Rooms / identity fundamentals |
| `OWNERSHIP_GAP` | Ownership chain gaps |
| `PROPCO_GAP` | PropCo / legal vehicle |
| `OPERATOR_GAP` | Operator / management |
| `PERSON_PROFILE_GAP` | People / professional profiles |
| `PORTFOLIO_GAP` | Org / portfolio edges |
| `BRAND_HISTORY_GAP` | Current / historical / announced brand |
| `DEVELOPMENT_GAP` | Renovation / development |
| `TRANSACTION_GAP` | Transactions / capital events |

`listTemplates({ includeInternal: true })` returns customer + internal.

## Do not use Full HI to fill one field

If only room count / PropCo / profile is missing → use the matching INTERNAL gap-fill (or native L1–L4), **not** a $5 Full Investigation.

## Eligible providers

Default strategy in registry: `SIMULATION` until live external enabled. Live path: `WEBHOUND` via provider adapter that **must** call `compileWebhoundPrompt`.

## Learning tags

Templates are tagged for learning registry join (archetype / jurisdiction / problem class). Learnings never auto-edit template text — version bump + regression required.
