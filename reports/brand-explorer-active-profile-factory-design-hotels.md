# Brand Explorer Active Profile Founder Review v34D

- Brand: **Design Hotels** (`design-hotels`)
- Generated: 2026-07-15T08:54:58.696Z
- Factory pass: **yes**

## Visual readiness
- Gallery (6 visible + imageUrl): **PASS** (6/6)
- Property examples: **PASS**
- Scenario cards (no IMAGE placeholder): **PASS**
- Registry traceability: **PASS**
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

## Staged apply workflow (v34D)

### Stage 1 — Draft apply
```bash
npm run brand-explorer-active-profile-apply-draft -- --brand design-hotels --apply --approve-brand-explorer-active-profile-draft --approve-brand-explorer-active-profile-copy-governance --founder-approved-active-profile-visual-review --confirm-no-company-validation-claim --confirm-no-summary-url-field --confirm-brand-only --confirm-official-source-images-only --confirm-minimum-six-visible-gallery-images --confirm-property-examples-have-hotel-images --confirm-no-logo-lifestyle-property-images --confirm-standard-detail-governance-reviewed
```

### Stage 2 — Founder visual review
```bash
npm run brand-explorer-active-profile-founder-review -- --brand design-hotels --dry-run
```

### Stage 3 — Active approval
_After founder visual review passes._
```bash
npm run brand-explorer-active-profile-apply-approved -- --brand design-hotels --apply --approve-brand-explorer-active-profile --confirm-founder-visual-review-passed --confirm-no-company-validation-claim --confirm-brand-only
```
