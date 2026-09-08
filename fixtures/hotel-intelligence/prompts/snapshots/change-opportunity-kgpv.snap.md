# Dealality Deep Research — Change & Opportunity Investigation

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
- Template ID: CHANGE_OPPORTUNITY
- Template version: 1.1.0
- Prompt compiler version: webhound-prompt-compiler-v1
- Customer question: Why might this hotel be worth pursuing now — and what risks could matter?
- Research objective: Identify why the hotel may be actionable now; surface physical asset/product risk, deferred product investment, operating-quality patterns, and operator/management stability signals — then produce Deal Risk Flags and What to Verify Next. Does not invent Development Signals product surfaces.
- Lanes: change_triggers, ownership, financing, brand, operator, leadership, renovation, development, transaction_activity, market_supply, physical_asset_condition, product_investment, deferred_capex, operating_quality, management_response, operator_stability, operator_change, deal_risk_flags
- Scope groups:
  - Change & Triggers: Ownership, Financing, Brand, Operator, Leadership, Development
  - Asset & Product: Condition, Renovation, Product investment, Maintenance, Capex signals
  - Operations: Service consistency, Management execution, Maintenance response, Leadership stability
  - Risk & Opportunity: Why Now, Deal risk flags, Operator-change signals, What to verify next
- Required report sections: executive_answer, opportunity_thesis, asset_product_risk, operating_quality_management_risk, operator_management_stability, deal_risk_flags, what_to_verify_next, open_questions, sources_evidence
- Negative screens: WRONG_PROPERTY, ADJACENT_ASSET, SIMILAR_NAME_COLLISION, OPERATOR_NOT_OWNER, BRAND_NOT_OPERATOR, HISTORICAL_NOT_CURRENT, ANNOUNCED_NOT_CURRENT, SUPERSEDED_EVIDENCE, INSUFFICIENT_IDENTITY_MATCH, TITLE_NOT_AUTHORITY, SPONSOR_NOT_DEED_UBO, ORG_CAPABILITY_NOT_PROPERTY_RELATIONSHIP, RESIDUAL_PORTFOLIO_NOT_CURRENT_OPERATOR_PROOF, SEARCH_SNIPPET_NOT_DECISIVE_EVIDENCE, GUEST_PATTERN_NOT_STRUCTURAL_PROOF, BRAND_EVENT_NOT_OWNER_SPECIFIC
- Validation rules: consistent_theme_not_single_anecdote, temporal_relevance_required, property_identity_guard, physical_vs_operating_vs_operator_change_separate, no_review_counting_theater, no_dollar_capex_invention
- Maximum provider budget: $5.00 USD (HARD CAP — do not exceed).

## Known facts (verify material conflicts; do not treat as unquestionable)
- GSF is owner-operator (self-operated) — do not invent a third-party management company dispute without evidence.
- Full Hotel Intelligence Investigation already completed historically — this is a follow-up addendum investigation.

## Unresolved questions (concentrate effort here)
- Investigate according to the template lanes and produce open questions where unresolved.

## Change & Opportunity special requirements
Cover WHY NOW, physical asset/product risk, product investment/deferred capex,
operating quality, management/operator stability, deal risk flags, and what to verify next.
Keep physical condition, operating quality, and operator-change risk conceptually separate.
Single guest reviews are NOT findings — require consistent themes (REPEATED/PERSISTENT/MULTI-SOURCE).
No invented dollar capex estimates. No fake 0–100 risk scores.

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

CHANGE_OPPORTUNITY required sections: Executive Answer (Why Now / What Could Derail / What Looks Stable / What Needs Verification), Opportunity Thesis, Asset & Product Risk, Product Investment / Capex, Operating Quality & Management Risk, Operator / Management Stability, Deal Risk Flags, What to Verify Next, Open Questions, Sources.
Guest evidence is a SIGNAL only — not automatic structural proof (GUEST_PATTERN_NOT_STRUCTURAL_PROOF).