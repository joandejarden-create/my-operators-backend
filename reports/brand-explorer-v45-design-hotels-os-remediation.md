# v45 Design Hotels OS-Guided Remediation

Generated: 2026-07-21T18:45:24.064Z

OS-routed Presentation remediation for Design Hotels. No unlock. No Company Validated. No released-brand writes.

## Summary

- OS apply_remediation: **true**
- Patches: **12** (unsafe=0)
- Projected founder_review_ready: **true**
- Projected active_profile_ready: **false**
- Property examples: **true**
- Internal preview projected clean: **true**
- Baseline protection: **true**
- Dry-run clean: **true**
- Apply executed: **true**

## OS confirmation

- State: `draft_applied_with_defects`
- Action: `apply_remediation`
- Full profile: **false**
- Company Validated: **false**
- Pass: **true**

## Defect families

- `visible_source_url_internal_evidence`: detected (liveHits=1)
- `modal_placeholders_property_examples`: not_detected_in_live_scan (liveHits=0)
- `standards_table_owner_readiness`: not_detected_in_live_scan (liveHits=0)
- `loyalty_coverage_bonvoy_caveats`: not_detected_in_live_scan (liveHits=0)
- `economics_obligations_affiliation_fit`: not_detected_in_live_scan (liveHits=0)
- `generic_fallback_language`: not_detected_in_live_scan (liveHits=0)
- `wrong_franchise_softbrand_boilerplate`: not_detected_in_live_scan (liveHits=0)
- `empty_or_thin_cards`: not_detected_in_live_scan (liveHits=0)
- `mechanical_diligence_copy`: not_detected_in_live_scan (liveHits=0)
- `source_notes_in_owner_fields`: detected (liveHits=12)
- `renderer_chrome_issues`: not_detected_in_live_scan (liveHits=0)
- `property_example_row_image_matching`: not_detected_in_live_scan (liveHits=0)

## Property examples (CALA trio)

Section label: **Curated CALA examples · Not a full directory**
- Wake BioHotel: present=true imageUrl=true modalComplete=true
- Condesa DF: present=true imageUrl=true modalComplete=true
- Carlota: present=true imageUrl=true modalComplete=true

## Patch plan (sample)

Total patches: 12
- `footprint.openings` / Body / rec59aTn7CDtoZN7O · safe=true · residual_owner_copy
- `footprint.openings` / Body / rec5sNCVcRGZfTwbV · safe=true · residual_owner_copy
- `footprint.openings` / Case Summary Brand Relevance / rec5sNCVcRGZfTwbV · safe=true · residual_owner_copy
- `footprint.momentum` / Body / recEExDl1cq6bWusn · safe=true · residual_owner_copy
- `footprint.openings` / Body / recLtxEB4hSVkLuWl · safe=true · residual_owner_copy
- `footprint.momentum` / Body / recYRLzyijFkSzt2c · safe=true · residual_owner_copy
- `footprint.momentum` / Body / recdkxeVhs4fGb5GK · safe=true · residual_owner_copy
- `footprint.openings` / Case Summary Owner Objective / rec59aTn7CDtoZN7O · safe=true · chain_prototype
- `footprint.openings` / Case Summary Owner Objective / rec5sNCVcRGZfTwbV · safe=true · chain_prototype
- `footprint.openings` / Case Summary Owner Objective / recLtxEB4hSVkLuWl · safe=true · chain_prototype
- `valueOwners.overview` / Body / recSru1JvJaFt1waV · safe=true · chain_prototype
- `overview.why_value` / Body / recY4kgl4BvTQjW60 · safe=true · chain_prototype

## Projection

- Projected state: **founder_review_ready**
- Founder review ready: **true**
- Active profile ready: **false**
- Rationale: Projected owner-copy clean + CALA examples intact + external locked → founder_review_ready. Active release still blocked until founder OK + v43.

## Exact apply command (not auto-executed)
```
npm run brand-explorer-v45-design-hotels-os-remediation -- --brand design-hotels --apply \
  --approve-brand-explorer-v45-design-hotels-os-remediation \
  --confirm-no-company-validation-claim \
  --confirm-no-active-profile-approval \
  --confirm-no-source-library-changes \
  --confirm-no-registry-changes \
  --confirm-no-image-field-changes \
  --confirm-external-profile-remains-locked \
  --confirm-internal-preview-owner-copy-clean \
  --confirm-released-golden-brands-unchanged \
  --confirm-design-hotels-only
```

## Guardrails

- airtableWritesDefault: false
- activeRelease: false
- companyValidatedChanges: false
- releasedBrandContentChanges: false
- sourceLibraryChanges: false
- registryChanges: false
- imageFieldChanges: false
- incompleteBrandUnlock: false
- otherIncompleteProcessed: false
- designHotelsOnly: true
