# Brand Explorer Active Profile Founder Review v34D

- Brand: **Small Luxury Hotels of the World** (`small-luxury-hotels-of-the-world`)
- Generated: 2026-07-21T22:48:11.340Z
- Factory pass: **no**

## Visual readiness
- Gallery (6 visible + imageUrl): **PASS** (6/6)
- Property examples: **PASS**
- Scenario cards (no IMAGE placeholder): **PASS**
- Registry traceability: **PASS**
- UI fallback risk: **PASS**
- Copy safety: **PASS**
- Standard Detail governance: **PASS**

## Property cards (asset pack)
- **Coral Reef Club**: image ✓ | needs_materialization
- **Quinta da Comporta**: image ✓ | needs_materialization
- **Hôtel San Régis**: image ✓ | needs_materialization

## Gallery image pack
- `materials.gallery.1`: source found | ready
- `materials.gallery.2`: source found | ready
- `materials.gallery.3`: source found | ready
- `materials.gallery.4`: source found | ready
- `materials.gallery.5`: source found | ready
- `materials.gallery.6`: source found | ready

## Scenario cards
- `overview.scenario.1`: image ✓ | copy package available
- `overview.scenario.2`: image ✓ | copy package available
- `overview.scenario.3`: image ✓ | copy package available

## Proof cards
- `overview.proof.1`: needs_review
- `overview.proof.2`: needs_review
- `overview.proof.3`: needs_review
- `overview.proof.4`: needs_review
- `overview.proof.5`: needs_review
- `overview.proof.6`: needs_review

## Pending governance gates
- standard_detail_governance_review
- founder_visual_review

## What will be applied (dry-run draft)
- Presentation patches: **12**
- Registry creates: **9**
- Sections: materials.gallery, footprint.openings, overview.scenario

## Remains human-reviewed
- Company Validated field — never auto-written
- Summary URL field — never auto-written
- Standard Detail governance sign-off
- Founder visual review approval

## Blockers
- modal_placeholders:recFpCS6PaPaOoRiz
- modal_placeholders:recf6NiXAAXoLaI3x
- modal_placeholders:reckWzWVLdoVbzZzQ

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
