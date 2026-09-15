# Canonical Contact Merge Policy

**Version:** canonical-contact-merge-policy-v1  
**Module:** `lib/hotel-intelligence/contact-intelligence/canonical-merge-policy.js`  
**Default write mode:** `REVIEW_REQUIRED` (code supports `DRY_RUN` / `AUTO_ACCEPT_SAFE` disabled for production)

## Principles

1. **Dealality owns WHO.** Surfe/providers may only contribute HOW TO REACH.
2. **Identity gate first** — never merge AMBIGUOUS / REJECTED / NOT_FOUND.
3. **Field ownership second** — accepted identity ≠ every field is theirs.
4. **Precedence** — never overwrite stronger evidence with weaker.
5. **Person ≠ opportunity** — event roles stay on relationship records.
6. **GDI Operating Law** — reusable logic in this module; hotel facts in data/config.

## Source class precedence (high → low)

HOTEL/USER VALIDATED → OFFICIAL DIRECT → CANONICAL INTERNAL → PROVIDER VERIFIED → PROVIDER ACCEPTED → OFFICIAL FUNCTIONAL → PROVIDER CORROBORATION → INFERRED

## Merge actions

| Action | Meaning |
|---|---|
| ACCEPT_NEW_FIELD | Fill missing field |
| REPLACE_WEAKER_FIELD | Stronger source replaces weaker (often TIER_2) |
| CORROBORATE_EXISTING | Same value; provenance only |
| HOLD_FOR_REVIEW | Equal-strength conflict / limited evidence |
| REJECT_FIELD | Identity/ownership/feedback block |
| NO_INCREMENTAL_VALUE | Weaker or duplicate main line |

## Safety tiers

| Tier | Examples | Behavior |
|---|---|---|
| TIER_1_SAFE | Missing email + ACCEPTED identity; exact corroboration | Eligible for future AUTO_ACCEPT_SAFE |
| TIER_2_REVIEW | Role→direct upgrade; limited evidence | REVIEW_REQUIRED |
| TIER_3_BLOCK | Ambiguous identity; phone collision; former employee | Never write |

## Write modes

| Mode | Effect |
|---|---|
| DRY_RUN | Propose + audit; in-memory PROPOSED_DRY_RUN only |
| REVIEW_REQUIRED | Default — human approval before write |
| AUTO_ACCEPT_SAFE | Code path exists; **disabled** until founder approval |

## Hotel / user feedback

States: CONFIRMED_CORRECT, CONFIRMED_WRONG, OUTDATED, LEFT_ORGANIZATION, WRONG_ROLE, WRONG_PERSON, VALID_BUT_NOT_DECISION_MAKER

Wrong / left-org → `reuseBlocked` (history retained).

## Regression

```bash
npm run test:canonical-contact-merge-policy
```
