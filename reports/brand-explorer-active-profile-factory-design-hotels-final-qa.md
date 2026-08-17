# Brand Explorer Active Profile Founder Review v34D

- Brand: **Design Hotels** (`design-hotels`)
- Generated: 2026-07-14T17:38:22.637Z
- Factory pass: **no**

## Visual readiness
- Gallery (6 visible + imageUrl): **PASS** (6/6)
- Property examples: **FAIL**
- Scenario cards (no IMAGE placeholder): **PASS**
- Registry traceability: **FAIL**
- UI fallback risk: **PASS**
- Copy safety: **PASS**
- Standard Detail governance: **PASS**

## Property cards (asset pack)

## Gallery image pack

## Scenario cards

## Proof cards

## What will be applied (dry-run draft)
- Presentation patches: **0**
- Registry creates: **0**
- Sections: (none)

## Remains human-reviewed

## Blockers
- non_hotel_photography:rec0o2XPGiafhcwLJ
- non_hotel_photography:rec59aTn7CDtoZN7O
- non_hotel_photography:rec5sNCVcRGZfTwbV
- non_hotel_photography:recdD7rNZzhj8YpQH
- non_hotel_photography:recLtxEB4hSVkLuWl
- non_hotel_photography:recXNKKIqeI7Vvi0C
- missing_registry_row:materials.gallery.1
- missing_registry_row:overview.scenario.1
- missing_registry_row:materials.gallery.2
- missing_registry_row:overview.scenario.2
- missing_registry_row:materials.gallery.3
- missing_registry_row:overview.scenario.3
- missing_registry_row:materials.gallery.4
- missing_registry_row:materials.gallery.5
- missing_registry_row:materials.gallery.6
- missing_registry_row:footprint.openings

## Staged apply workflow (v34D)

### Stage 1 — Draft apply (preview writes)
_Run after dry-runs are clean. Does not approve active profile._
```bash
npm run brand-explorer-active-profile-apply-draft -- --brand design-hotels --apply --approve-brand-explorer-active-profile-draft --approve-brand-explorer-active-profile-copy-governance --founder-approved-active-profile-visual-review --confirm-no-company-validation-claim --confirm-no-summary-url-field --confirm-brand-only --confirm-official-source-images-only --confirm-minimum-six-visible-gallery-images --confirm-property-examples-have-hotel-images --confirm-no-logo-lifestyle-property-images --confirm-standard-detail-governance-reviewed
```

### Stage 2 — Founder visual review
```bash
npm run brand-explorer-active-profile-founder-review -- --brand design-hotels --dry-run
```

### Stage 3 — Active approval
_Blocked until founder visual review passes._
