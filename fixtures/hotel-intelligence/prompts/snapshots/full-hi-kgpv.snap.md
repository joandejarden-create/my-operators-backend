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
- Collision: do not use "undefined" — Krystal Grand Puerto Vallarta ≠ Krystal Resort Puerto Vallarta

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
- GSF is owner-operator (self-operated) — do not invent a third-party management company dispute without evidence.
- Full Hotel Intelligence Investigation already completed historically — this is a follow-up addendum investigation.

## Unresolved questions (concentrate effort here)
- Investigate according to the template lanes and produce open questions where unresolved.

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