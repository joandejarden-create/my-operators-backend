# Operator Intelligence — Research Operating Process

**Date:** 2026-08-03  
**Principle:** Methodology + exception review — not founder field-by-field approval.

---

## Research-wave creation

1. Select operators by pipeline relevance, CALA coverage, segment/model diversity, public-source feasibility, Ranking Ready gap.  
2. Prioritize Conditionally Rankable → Research Required with high pipeline value.  
3. Cap wave size (6–8) for quality.  

## Source discovery

Follow `docs/data/operator-intelligence-source-policy.md`. Capture URL, publisher, date, type. Deduplicate by normalized URL. Never use snippets/AI as evidence.

## Claim extraction

Create structured claims (`docs/data/operator-intelligence-claim-model.md`). Assign scopes, evidence class, publication class.

## Automated validation

Required fields · controlled vocab · source URL/date · duplicates · conflicts · overclaims · stale dates · presence-type consistency.

## Publication

Resolve via `lib/operator-intelligence/publication-policy.js`: Auto-Publish · Publish With Label · Internal Only · Human Review · Rejected · Stale · Conflicted · Insufficient Evidence.

## Quality control

- Automated gates  
- Exception queue  
- Periodic random sample (founder)  
- High-impact score-change review  
- Post-publication audit  
- Scheduled refresh (wave-triggered; no calendar automation in this phase)

## Founder review cadence (recommendation)

| Trigger | Cadence |
| ------- | ------- |
| Methodology / vocab / threshold changes | Before each change |
| Exception queue High severity | Weekly or per wave close |
| Random sample of auto-published Class 1 | Per wave (e.g. 10%) |
| Production release wave | Explicit approval |
| Material Top-5 shift on live deals | Before My Deals enablement |

Do not create calendar automations in-repo in this assignment.
