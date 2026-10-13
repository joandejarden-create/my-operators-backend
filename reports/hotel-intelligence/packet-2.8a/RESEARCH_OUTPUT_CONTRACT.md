# RESEARCH_OUTPUT_CONTRACT

> Packet 2.8A · Provider → Dealality structured output contract  
> Code: `lib/hotel-intelligence/research/prompts/output-contracts.js`  
> Version: `hi-research-output-contract-v1`

## Principle

Provider prose is **evidence / research material**.  
**Structured Dealality objects** drive Hotel Explorer.  
The Full Investigation report is a **presentation artifact**, not the canonical database.

Preserve raw provider artifacts immutably; normalize into Dealality schema.

## Required structured blocks

| Block | Role |
|-------|------|
| `SUBJECT_IDENTITY` | Exact hotel / entity researched |
| `SOURCES` | URL-backed sources |
| `FINDINGS` | Key findings with keys |
| `CLAIMS` | Atomic claims for normalization |
| `ENTITIES` | Orgs / vehicles / brands |
| `RELATIONSHIPS` | Typed edges |
| `PEOPLE` | People with profile status |
| `EVENTS` | Material events |
| `PROPERTY_FACTS` | Rooms / product / fundamentals |
| `CONFLICTS` | Preserved disagreements |
| `OPEN_QUESTIONS` | Explicit unresolved |
| `RESEARCH_NOTES` | Method / caveats |

## Finding shape

Every finding should include:

- `finding_key`  
- `statement`  
- `status`  
- `confidence`  
- `source_urls`  
- `temporal_status`  

Customer report compiler additionally binds `finding_id` → rationale / evidence / citations (R6 migration **COMPLETE** on golden corpus).

## Relationship shape

- `subject` · `predicate` · `object`  
- `current` / `historical`  
- `evidence`

## Person shape

- `name` · `title` · `organization` · `role`  
- `professional_profile_url` **or** `NOT_FOUND`  
- `source`  
- Do not equate title with signing authority

## Template overlays

### FULL_HOTEL_INTELLIGENCE

Cover when evidence allows: property identity, ownership chain, PropCo, economic owner/sponsor, parent/control, operator, current/historical/announced brand, development/renovation, organization/portfolio, people/decision authority, transactions/capital, commercial pursuit, open questions, sources.  
Label gaps **UNRESOLVED** — do not invent.

### CHANGE_OPPORTUNITY

Required customer sections: Executive Answer (Why Now / What Could Derail / What Looks Stable / What Needs Verification), Opportunity Thesis, Asset & Product Risk, Product Investment / Capex, Operating Quality & Management Risk, Operator / Management Stability, Deal Risk Flags, What to Verify Next, Open Questions, Sources.  
Guest evidence = **SIGNAL only** (`GUEST_PATTERN_NOT_STRUCTURAL_PROOF`). No invented dollar capex; no fake 0–100 risk scores.

## Normalization

Webhound adapter must normalize into Dealality schema even when prose is imperfect.  
Do not scrape the compiled PDF/HTML report to rebuild Explorer state.
