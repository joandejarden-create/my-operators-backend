# WEBHOUND_PROMPT_STANDARD

> Packet 2.8A · Standardized Webhound / provider prompt contract  
> Compiler: `lib/hotel-intelligence/research/compile-webhound-prompt.js` (`webhound-prompt-compiler-v1`)  
> Provider: `lib/hotel-intelligence/research/providers.js` — sole compile path; persists `prompt_hash`

## Pipeline

```
ResearchTemplateRegistry
  → ResearchInvestigationSpec (provider-neutral)
  → compileWebhoundPrompt(spec)
  → versioned provider prompt + output_instructions + prompt_hash
```

**Forbidden:** `promptForKGPV` / hotel-specific prompt builders / ad-hoc UI strings sent to Webhound.

## Required sections in every compiled prompt

1. Title — Dealality Deep Research + template display name  
2. **Shared Dealality research contract** (`common-preamble.js` / `hi-common-preamble-v1`)  
3. **Source hierarchy** (default 1–10; jurisdiction playbooks may reorder)  
4. **Hotel identity (authoritative)** — canonical ID, name, location, brand, owner/operator knowns, collisions/guards  
5. **Research template** — id, version, compiler version, objective, lanes, negative screens, validation, **budget hard cap**  
6. **Known facts** — verify conflicts; not unquestionable  
7. **Unresolved questions** — concentrate effort  
8. Template-specific blocks (e.g. Change & Opportunity special requirements)  
9. **Structured output contract** (`output-contracts.js`)

## Common preamble rules (must)

- Exact hotel/entity; resolve aliases before conclusions  
- CURRENT / HISTORICAL / ANNOUNCED / SUPERSEDED  
- Owner ≠ PropCo ≠ sponsor ≠ operator ≠ brand ≠ developer ≠ lender ≠ asset manager  
- Do not infer ownership from operation or operation from brand  
- Do not infer UBO from title  
- No adjacent/similar-name merge  
- Announced conversion ≠ completed without current evidence  
- Preserve conflicts; explicit open questions; URLs; profile URLs or NOT_FOUND  
- Structured findings for Dealality normalization  
- No Airtable/Census/internal product mechanics in findings

## Negative screens

Unified registry: `lib/hotel-intelligence/research/prompts/negative-screens-registry.js`  
Includes (non-exhaustive): `WRONG_PROPERTY`, `ADJACENT_ASSET`, `SIMILAR_NAME_COLLISION`, `OPERATOR_NOT_OWNER`, `BRAND_NOT_OPERATOR`, `HISTORICAL_NOT_CURRENT`, `ANNOUNCED_NOT_CURRENT`, `SUPERSEDED_EVIDENCE`, `INSUFFICIENT_IDENTITY_MATCH`, `TITLE_NOT_AUTHORITY`, `SPONSOR_NOT_DEED_UBO`, `ORG_CAPABILITY_NOT_PROPERTY_RELATIONSHIP`, `RESIDUAL_PORTFOLIO_NOT_CURRENT_OPERATOR_PROOF`, `SEARCH_SNIPPET_NOT_DECISIVE_EVIDENCE`, `GUEST_PATTERN_NOT_STRUCTURAL_PROOF`, plus invented-people / unverified-UBO class screens.

## QA gates (`validateCompiledWebhoundPrompt`)

`SUBJECT_IDENTITY_PRESENT` · `TEMPLATE_VERSION_PRESENT` · `OBJECTIVE_PRESENT` · `KNOWN_FACTS_PRESENT` · `UNRESOLVED_QUESTIONS_PRESENT` · `SOURCE_HIERARCHY_PRESENT` · `NEGATIVE_SCREENS_PRESENT` · `TEMPORAL_RULES_PRESENT` · `OUTPUT_CONTRACT_PRESENT` · `BUDGET_PRESENT` · `PROMPT_HASH_PRESENT` · `COMPILER_VERSION_PRESENT`

## Cost guards

- $5 hard maximum for standard external investigation (where configured)  
- No automatic paid retry  
- Batch escalation requires founder authorization  
- Packet 2.8A Webhound runs: **0**

## Example previews

- `prompt-previews/WEBHOUND_PROMPT_PREVIEW_kgpv_FULL_HOTEL_INTELLIGENCE.md`  
- `prompt-previews/WEBHOUND_PROMPT_PREVIEW_kgpv_CHANGE_OPPORTUNITY.md`  

Live compile note: hash computed at compile time via SHA-256 of `prompt + "---" + output_instructions`.
