# Integration Controller — Qualified Opportunity classification (central eligibility)

**From:** Track A / DealSignal Chat A  
**As of:** 2026-11-23  
**Publication:** CUSTOMER_BLOCKED  

## Founder rule (binding)

Completed / committed / renewed / retained / awarded hotel commercial events must **never** count as open Qualified Opportunities solely because they are verified.

Qualified only when: (1) BCT/MCT decision exists, (2) remains unresolved, (3) credible competitive pathway evidenced. RFP not required.

## Track A delivered

| Artifact | Path |
|---|---|
| Classifier module | `lib/hotel-intelligence/dealsignal/dealsignal-qualified-opportunity-classification-v1.js` |
| Cursor rule (alwaysApply) | `.cursor/rules/dealsignal-qualified-opportunity-classification.mdc` |
| Docs | `docs/data-intelligence/dealsignal-qualified-opportunity-classification-v1.md` |
| Unit gate | `npm run test:dealsignal-qualified-opportunity-classification-v1` |
| AGENTS.md common mistake | appended |

Hard exclusions include: AC Cartagena completed reflag · GHL–Sonesta MFA→2034 · completed renewals · appointed operators · executed management replacements.

## IC ask — central eligibility fix

Apply the same non-open exclusion in:

1. DealSignal drawer / opportunity reader Qualified counts  
2. Any “Verified open” / first-100 / critical-mass scorecard aggregations  
3. UI badges that currently treat Verified historical facts as open opportunities  

Do **not** invent new Airtable select options from research assessment labels alone — map to existing commercialStatus / record types.

## V2 compliance note

Critical-Mass V2 Qualified assessments (Voir Beach, Baha Mar #4) remain unresolved brand decisions with evidenced pathways — they pass the three-part gate. Justo/voco+Aimbridge, Casa Mayor Autograph, Bergantín Dreams, Terra Nova BW are correctly non-Qualified.

## Status

`AWAITING_INTEGRATION_CONTROLLER` for central eligibility wiring. Track A will apply the module on every future discovery/persistence wave.
