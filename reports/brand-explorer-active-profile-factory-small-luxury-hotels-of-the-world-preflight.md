# Brand Explorer Active Profile Founder Review v34D

- Brand: **Small Luxury Hotels of the World** (`small-luxury-hotels-of-the-world`)
- Generated: 2026-07-14T14:15:19.078Z
- Factory pass: **no**

## Visual readiness
- Gallery (6 visible + imageUrl): **FAIL** (0/6)
- Property examples: **FAIL**
- Scenario cards (no IMAGE placeholder): **FAIL**
- Registry traceability: **PASS**
- UI fallback risk: **FAIL**
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
- need_6_visible_gallery_imageUrl_got_0
- no_visible_scenario_cards_in_api
- atelier_hardcoded_scenario_fallback_risk:overview.scenario.1
- atelier_hardcoded_scenario_fallback_risk:overview.scenario.2
- atelier_hardcoded_scenario_fallback_risk:overview.scenario.3

## Staged apply workflow (v34D)

### Stage 1 — Draft apply (preview writes)
_Run after dry-runs are clean. Does not approve active profile._
```bash
npm run brand-explorer-active-profile-apply-draft -- --brand small-luxury-hotels-of-the-world --apply --approve-brand-explorer-active-profile-draft --approve-brand-explorer-active-profile-copy-governance --founder-approved-active-profile-visual-review --confirm-no-company-validation-claim --confirm-no-summary-url-field --confirm-brand-only --confirm-official-source-images-only --confirm-minimum-six-visible-gallery-images --confirm-property-examples-have-hotel-images --confirm-no-logo-lifestyle-property-images --confirm-standard-detail-governance-reviewed
```

### Stage 2 — Founder visual review
```bash
npm run brand-explorer-active-profile-founder-review -- --brand small-luxury-hotels-of-the-world --dry-run
```

### Stage 3 — Active approval
_Blocked until founder visual review passes._
