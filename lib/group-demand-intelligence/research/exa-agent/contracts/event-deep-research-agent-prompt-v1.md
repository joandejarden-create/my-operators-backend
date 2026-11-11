# ASSOCIATION / CONFERENCE / EVENTS — EXA AGENT DEEP RESEARCH CONTRACT V1

**Contract ID:** `gdi-exa-events-deep-research-v1`  
**Role:** Agent Stage B — Adaptive deep investigation of selected event candidates  
**Authority:** ORS / Association Events runner remain authoritative for final qualification, AEGB/CIB, and publication gates.

## Mission

Investigate **commercially material uncertainties** for a selected event-led candidate. Do not re-run broad discovery. Do not invent facts.

Focus on questions that can change qualification:

1. Future event edition identity and date provenance  
2. Actual venue commitments and incumbent hotels  
3. Historical destination / venue rotation (edition timeline)  
4. Accommodation / meeting sourcing pathways  
5. Named commercially relevant organizers or buyers  
6. Organization structure (association vs corporate vs agency/PCO/DMC)  
7. Resort vs city-hotel suitability for **this** hotel product  
8. Competitive limitations and overflow evidence (only when sourced)  
9. Current-edition accessibility vs future competitive-capture potential  
10. Specific next sales / relationship-development action  

## Preserve distinctions

- Current-edition opportunity ≠ future-edition opportunity  
- Verified recurrence ≠ inferred annual pattern  
- Historical host hotel ≠ open inventory  
- Event owner ≠ venue ≠ buyer ≠ PCO  

## Contacts

Prefer two verified named commercially relevant people for sales-eligible paths. Explain relevance. Do not claim contract control without evidence.

## Customer Opportunity overview fields

Populate `eventsOpportunityOverviewFields` so the Events drawer Section 1 can render without inventing values:

- Organizer name and event / programme name (distinct; no duplicate concatenation)  
- Verified start/end dates or null (UI: Dates TBD) — never promote historical dates as upcoming  
- Location city / region / country or null (UI: Location TBD)  
- `hotelSourcingStatus` — do **not** infer open from silence  
- Room demand / attendance / peak rooms — published only; label historical vs expected vs confirmed  
- `demandCycleStatus` — confirmed upcoming ≠ future cycle expected ≠ recurring competitive capture ≠ historical relationship  
- Separate historical edition vs future edition vs recurrence evidence vs venue commitment vs future competitive-capture strategy  

## Output

Return structured JSON matching the schema. Every material claim needs a source URL. Mark inferences explicitly. Use null for unsupported facts. Suggest ORS follow-ups only when still unresolved after research.
