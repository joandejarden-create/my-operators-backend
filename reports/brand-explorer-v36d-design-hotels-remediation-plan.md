# Design Hotels Remediation Apply Plan (v36D)

- Action: **remediation_apply**
- Mode: **dry-run** — apply blocked in v36D
- Active approval: **NOT RECOMMENDED**
- Calibrated score: 43
- Patch items: 11

## Patch summary

```json
{
  "total": 11,
  "safeForGenericApply": 1,
  "requiresFounderApproval": 7,
  "requiresCodePatch": 0,
  "ownerVisible": 9,
  "byType": {
    "materialize_image": 1,
    "promote_registry_asset": 1,
    "rewrite_external_copy": 1,
    "no_write_investigation": 1,
    "patch_presentation_row": 1,
    "populate_modal_fields": 3,
    "create_presentation_row": 3
  }
}
```

## Checklist

- **design-hotels:property_example_render_not_ready:footprint.openings** — `materialize_image` @ visual_asset_materialization — generic=false founder=false code=false
- **design-hotels:registry_only_images:global** — `promote_registry_asset` @ registry_traceability — generic=false founder=false code=false
- **design-hotels:wrong_model_language:global** — `rewrite_external_copy` @ copy_governance — generic=false founder=false code=false
- **design-hotels:draft_applied_with_defects** — `no_write_investigation` @ visual_asset_materialization — generic=false founder=true code=false
- **design-hotels:presentation_plan_invalid** — `patch_presentation_row` @ draft_build — generic=true founder=false code=false
- **design-hotels:modal_placeholder:Wake BioHotel** — `populate_modal_fields` @ visual_asset_materialization — generic=false founder=true code=false
- **design-hotels:modal_placeholder:Condesa DF** — `populate_modal_fields` @ visual_asset_materialization — generic=false founder=true code=false
- **design-hotels:modal_placeholder:Carlota** — `populate_modal_fields` @ visual_asset_materialization — generic=false founder=true code=false
- **design-hotels:tab_coverage_gap:standards.requirement** — `create_presentation_row` @ draft_build — generic=false founder=true code=false
- **design-hotels:tab_coverage_gap:loyalty.owner_lens** — `create_presentation_row` @ draft_build — generic=false founder=true code=false
- **design-hotels:tab_coverage_gap:economics.intro** — `create_presentation_row` @ draft_build — generic=false founder=true code=false

## Must address
- property example row-level image matching
- modal placeholders on Wake BioHotel, Condesa DF, Carlota
- wrong affiliation model language in 3 rows
- standards table owner-readiness
- loyalty tab coverage
- economics / fee affiliation fit
- source/internal language if still present
- fallback risk where renderer masks missing rows

## Allowed command (future)
```
npm run brand-explorer-v36d-action-router -- --brands design-hotels --apply-remediation --approve-brand-explorer-active-profile-remediation --confirm-no-company-validation-claim --confirm-no-active-profile-approval --confirm-no-summary-url-field --confirm-external-owner-copy-clean --confirm-render-readiness-contract --confirm-brand-only
```