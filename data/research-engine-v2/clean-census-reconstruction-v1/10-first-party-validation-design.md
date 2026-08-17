# First-Party Brand/Operator Validation Design

## Workflow (future — not built)

```
DEALALITY PREPARES PROFILE
→ BRAND/OPERATOR REVIEWS
→ CONFIRM / CORRECT / ADD
→ FIRST-PARTY VALIDATION PACK
→ DEALALITY STEWARD REVIEW
→ GOVERNANCE GATES
→ APPROVED SoT UPDATE
```

## Capture per submission

validating_organization, validating_person, title_role, validation_date, fields_reviewed, records_reviewed, values_confirmed, corrections_supplied, hotels_added, hotels_removed, evidence_documents, approved_source_urls, image_permissions, validation_scope, notes

## Resulting classification

**First-Party Validated** / **FIRST-PARTY CONFIRMED** — separate from Independently Researched.

## Authority (claim-specific)

| Claim type | First-party authority |
|------------|----------------------|
| Brand positioning / loyalty / development appetite | Strong |
| Operator managed portfolio | Strong |
| Owner identity (when owner validates) | Strong |
| Hotel open/pipeline status | Strong but preserve conflicts with official directory |
| Keys / amenities | Strong if operator/brand supplied |
| Regulatory / licensing facts | Do **not** silently override other authorities |

First-party confirmation does **not** retroactively authorize legacy STR/client values.
