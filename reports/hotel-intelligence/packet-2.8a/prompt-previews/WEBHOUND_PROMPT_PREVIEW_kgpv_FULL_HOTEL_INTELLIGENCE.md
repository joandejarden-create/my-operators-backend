# WEBHOUND_PROMPT_PREVIEW — KGPV Full Hotel Intelligence

> Live compile via `compileWebhoundPrompt` (`lib/hotel-intelligence/research/compile-webhound-prompt.js`).
> `prompt_hash` is SHA-256 of `prompt + "---" + output_instructions` at compile time.

| Field | Value |
|--------|--------|
| template_id | FULL_HOTEL_INTELLIGENCE |
| template_version | 1.0.0 |
| prompt_compiler_version | webhound-prompt-compiler-v1 |
| common_preamble_version | hi-common-preamble-v1 |
| negative_screens_registry_version | hi-negative-screens-v1 |
| output_contract_version | hi-research-output-contract-v1 |
| prompt_hash | `ca4818920fc3a10fbe645ba196aa969d430681331dc3904e9e3aec5b28f59e96` |
| budget_usd | 5 |
| hotel_id | recUNycnMwOVFX0hc |

---

## Compiled prompt

# Dealality Deep Research — Full Hotel Intelligence Investigation

## Shared Dealality research contract
- Research the EXACT hotel/entity specified. Resolve aliases/former names before conclusions.
- Distinguish CURRENT, HISTORICAL, ANNOUNCED, and SUPERSEDED facts.
- Distinguish owner, PropCo, parent, sponsor, operator, brand, developer, lender, and asset manager.
- Do not infer ownership from operation.
- Do not infer operation from brand.
- Do not infer beneficial ownership from executive title.
- Do not treat adjacent/similarly named hotels as the subject property.
- Do not treat an announced conversion as completed without current evidence.
- Prefer strongest available current sources (see source hierarchy).
- Preserve conflicting evidence; do not silently pick a winner.
- Explicitly identify unresolved questions.
- Provide URLs for every substantive source.
- Capture person-level professional-profile URLs when discovered; else mark NOT_FOUND.
- Return structured findings suitable for Dealality normalization.
- Do not invent Airtable/Census/internal product mechanics in findings.

## Source hierarchy (prefer earlier when applicable)
1. Government / legal / registry / securities filings
2. Current hotel first-party
3. Owner / sponsor first-party
4. Operator first-party
5. Brand first-party
6. Lender / capital-provider first-party
7. Credible transaction / trade / local press
8. Professional profiles
9. Guest / reputation evidence (signal only — not structural proof)
10. Secondary / discovery sources

## Hotel identity (authoritative)
- Canonical hotel ID: recUNycnMwOVFX0hc
- Current name: Krystal Grand Puerto Vallarta
- Address / location: Puerto Vallarta, Jalisco, Mexico
- Market: Puerto Vallarta
- Country: Mexico
- Current brand: Krystal Grand
- Parent / owner (known, verify): Grupo Hotelero Santa Fe
- Operator (known, verify): Grupo Hotelero Santa Fe
- Operating model hint: owner-operated (self-operated) — verify
- Announced / contemplated names: Breathless Puerto Vallarta (announced conversion — completion unverified)

## HARD PROPERTY IDENTITY GUARD
Krystal Grand Puerto Vallarta ≠ undefined
Krystal Grand Puerto Vallarta ≠ Krystal Resort Puerto Vallarta
Do not mix evidence between these two hotels.
- Collision: do not use "Krystal Resort Puerto Vallarta" — Adjacent Chartwell-owned asset; GSF-managed — not the subject hotel.

## Research template
- Template ID: FULL_HOTEL_INTELLIGENCE
- Template version: 1.0.0
- Prompt compiler version: webhound-prompt-compiler-v1
- Customer question: What do we know — and what remains unresolved — about this hotel?
- Research objective: Establish the baseline Full Hotel Intelligence Investigation for a hotel with source-backed findings and explicit open questions.
- Lanes: ownership, corporate_structure, brand_operator, people, development_intelligence, portfolio, material_changes
- Negative screens: WRONG_PROPERTY, ADJACENT_ASSET, SIMILAR_NAME_COLLISION, OPERATOR_NOT_OWNER, BRAND_NOT_OPERATOR, HISTORICAL_NOT_CURRENT, ANNOUNCED_NOT_CURRENT, SUPERSEDED_EVIDENCE, INSUFFICIENT_IDENTITY_MATCH, TITLE_NOT_AUTHORITY, SPONSOR_NOT_DEED_UBO, ORG_CAPABILITY_NOT_PROPERTY_RELATIONSHIP, RESIDUAL_PORTFOLIO_NOT_CURRENT_OPERATOR_PROOF, SEARCH_SNIPPET_NOT_DECISIVE_EVIDENCE, GUEST_PATTERN_NOT_STRUCTURAL_PROOF, INVENTED_PEOPLE, UNVERIFIED_UBO
- Validation rules: source_backed_findings, explicit_open_questions, no_title_equals_authority
- Maximum provider budget: $5.00 USD (HARD CAP — do not exceed).

## Known facts (verify material conflicts; do not treat as unquestionable)
- GSF is owner-operator (self-operated) — verify; do not invent a third-party management dispute without evidence.
- Breathless conversion was announced; current trading identity remains Krystal Grand pending verification.

## Unresolved questions (concentrate effort here)
- Natural-person UBO / legal signatory for the property PropCo
- Deed/title confirmation for PropCo candidate IHVSF

## Structured output contract
Where possible, organize the report so Dealality can normalize into:
- SUBJECT_IDENTITY
- SOURCES
- FINDINGS
- CLAIMS
- ENTITIES
- RELATIONSHIPS
- PEOPLE
- EVENTS
- PROPERTY_FACTS
- CONFLICTS
- OPEN_QUESTIONS
- RESEARCH_NOTES

Every finding should include: finding_key, statement, status, confidence, source_urls, temporal_status.
Every relationship: subject, predicate, object, current/historical, evidence.
Every person: name, title, organization, role, professional_profile_url (or NOT_FOUND), source.
Do not treat final prose as the only deliverable — structured blocks are required.

FULL HI should cover: property identity, ownership chain, PropCo, economic owner/sponsor, parent/control, operator, current/historical/announced brand, development/renovation, organization/portfolio, people/decision authority, transactions/capital, commercial pursuit, open questions, sources.
Unresolved items must be labeled UNRESOLVED — do not invent.

---

## Output instructions

Structure the final report so Dealality can normalize it. Prefer explicit section headings matching the investigation template. Cite sources inline. Distinguish verified facts from unresolved questions. Do not claim Dealality product canonical status. Include SUBJECT_IDENTITY, SOURCES, FINDINGS (with finding_key), PEOPLE (with professional_profile_url or NOT_FOUND), RELATIONSHIPS, OPEN_QUESTIONS. Include Executive Answer, Key Findings (finding_key each), Ownership, Operator, Brand chronology, Organization/Portfolio, People table, Transactions/Capital, Commercial pursuit, Open Questions, Sources.
