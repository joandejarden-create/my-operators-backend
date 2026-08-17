# Brand Explorer Active Profile Founder Review v34D

- Brand: **Tribute Portfolio** (`tribute-portfolio`)
- Generated: 2026-07-14T12:49:52.925Z
- Factory pass: **no**

## Visual readiness
- Gallery (6 visible + imageUrl): **PASS** (6/6)
- Property examples: **PASS**
- Scenario cards (no IMAGE placeholder): **PASS**
- Registry traceability: **FAIL**
- UI fallback risk: **PASS**
- Copy safety: **FAIL**
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
- missing_registry_row:footprint.openings
- missing_registry_row:overview.scenario.3
- missing_registry_row:overview.scenario.2
- missing_registry_row:overview.scenario.1
- missing_registry_row:overview.hero
- missing_registry_row:materials.gallery.1
- missing_registry_row:materials.gallery.2
- missing_registry_row:materials.gallery.3
- missing_registry_row:materials.gallery.4
- missing_registry_row:materials.gallery.5
- missing_registry_row:materials.gallery.6
- risky_copy:net_contribution:commercial.kpi.lens
- risky_copy:net_contribution:valueOwners.overview
- risky_copy:net_contribution:valueOwners.watchouts
- risky_copy:net_contribution:commercial.lever.distribution

## Staged apply workflow (v34D)

### Stage 1 — Draft apply (preview writes)
_Run after dry-runs are clean. Does not approve active profile._
```bash
npm run brand-explorer-active-profile-apply-draft -- --brand tribute-portfolio --apply --approve-brand-explorer-active-profile-draft --approve-brand-explorer-active-profile-copy-governance --founder-approved-active-profile-visual-review --confirm-no-company-validation-claim --confirm-no-summary-url-field --confirm-brand-only --confirm-official-source-images-only --confirm-minimum-six-visible-gallery-images --confirm-property-examples-have-hotel-images --confirm-no-logo-lifestyle-property-images --confirm-standard-detail-governance-reviewed
```

### Stage 2 — Founder visual review
```bash
npm run brand-explorer-active-profile-founder-review -- --brand tribute-portfolio --dry-run
```

### Stage 3 — Active approval
_Blocked until founder visual review passes._
