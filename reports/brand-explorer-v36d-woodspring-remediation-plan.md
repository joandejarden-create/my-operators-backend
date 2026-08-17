# WoodSpring Suites Remediation Plan (v36D)

- Action: **remediation_apply**
- Patch items: 37

## Prior known issues

- property/gallery image render readiness
- visual/valueOwners blockers
- generic internal/fallback copy
- six-gallery rule
- owner-facing copy quality

Do not treat prior Final QA / required-section scores as sufficient — v36C calibrated score gates apply

## Patch summary

```json
{
  "total": 37,
  "safeForGenericApply": 3,
  "requiresFounderApproval": 6,
  "requiresCodePatch": 3,
  "ownerVisible": 15,
  "byType": {
    "populate_modal_fields": 3,
    "rewrite_external_copy": 2,
    "materialize_image": 1,
    "promote_registry_asset": 1,
    "no_write_investigation": 26,
    "renderer_patch_required": 3,
    "patch_presentation_row": 1
  }
}
```