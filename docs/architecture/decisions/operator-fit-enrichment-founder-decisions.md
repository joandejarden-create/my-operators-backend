# ADR — Operator Fit Enrichment Founder Decisions

**Status:** Accepted  
**Date:** 2026-08-03  
**Scope:** Enrichment readiness / founder QA (no Airtable writes; no production enablement)  
**Related:** `docs/architecture/decisions/operator-fit-phase-1-2-founder-decisions.md`

---

## 2.1 Enrichment ownership

Dealality owns the normalized operator baseline.

Operators may later submit structured information, but operator-submitted information remains classified as **operator-reported** until independently supported.

Brands may confirm:

- Approved operators
- Existing brand relationships
- Brand-management availability
- Regional applicability

Owners and advisors may contribute references or project-specific experience, but they are **not** responsible for researching operator profiles.

## 2.2 Enrichment priority

Enrich in this order:

1. Eligibility-critical information  
2. Project-differentiating experience  
3. Brand–operator relationships  
4. Comparable properties  
5. Evidence sources  
6. Ownership and governance model  
7. Regional resources and execution capacity  
8. Economics collected during project-specific outreach  
9. Post-selection performance outcomes  

Do **not** prioritize generic offered-service checklists as primary ranking data.

## 2.3 Brand-managed availability

A brand-managed candidate is confirmed only when supported by one or more of:

- Official brand information  
- Verified Dealality relationship information  
- Direct brand confirmation  
- Documented comparable assignments  

Strategic preference, general brand behavior, or assumed market practice does **not** confirm that brand management will be offered for a specific project.

Where availability is plausible but unconfirmed, classify it as:

`Eligible With Conditions`

Return the validation item:

`Confirm whether the brand will offer direct management for this project.`

Do **not** award positive compatibility points for unconfirmed brand-management availability.

## 2.4 Shortlist architecture

Do **not** use Operator Deal Requests as shortlist storage.

Maintain the conceptual separation between:

- Target List  
- Shortlist  
- Outreach or Deal Request  

No new shortlist persistence is authorized in this phase.

## 2.5 Production ranking readiness

An operator may enter the production Top-5 ranking only when:

- Active status is known  
- Geography is known or conditionally supportable  
- Operating-structure compatibility is known  
- Hotel segment or chain-scale relevance is known  
- At least one meaningful project-experience dimension is known  
- Material positive claims have at least one identified source  
- No unresolved critical eligibility conflict exists  
- Project-specific Data Coverage is at least **50%**

An otherwise relevant operator below this threshold may appear as:

`Additional Candidate Requiring Research`

Do not force an operator into the Top 5 merely to return five results.
