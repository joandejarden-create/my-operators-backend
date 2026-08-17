# Brand Explorer — AI-Assisted Profile Footnote Standardization

> Binding: every Brand Explorer profile (public-full + factory-preview + future factory waves) must always render the AI-Assisted Profile trust footnote.

## Required visible format

```
AI-Assisted Profile
Last Reviewed: [MMM D, YYYY] · Source Basis: [Source Basis] · Region: [Region Basis]
```

## Implementation

- Module: `lib/partner-intelligence/brand-explorer-ai-assisted-footnote.js`
- API enricher: `api/brand-library.js` → `applyBrandExplorerAiAssistedFootnote`
- Hero renderer: `public/js/brand-explorer-gold-detail.js` + `profile-governance-trust-chip.js`
- Gate id: `ai_assisted_profile_footnote_visible` (PVQL + factory rules)

## Rules

- Default label: **AI-Assisted Profile** (even when Source Basis is Company Materials)
- Use Company-Validated / Brand Verified wording only when Company Validated is truly true
- Do not invent CALA-specific without source support
- Prefer computed fallbacks over Airtable Presentation / governance writes
- Footnote must not depend on a per-brand Presentation row or External Display Status = Show Trust Label

## Commands

```bash
npm run brand-explorer-ai-assisted-footnote-standardization -- --dry-run
npm run brand-explorer-ai-assisted-footnote-standardization -- --audit
npm run brand-explorer-ai-assisted-footnote-standardization -- --apply \
  --approve-ai-assisted-footnote-standardization \
  --confirm-global-rendering-requirement \
  --confirm-every-brand-explorer-profile \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-brand-status-changes \
  --confirm-no-release-field-writes \
  --confirm-no-presentation-content-rewrites \
  --confirm-no-image-writes \
  --confirm-no-cala-claims-without-source-support \
  --confirm-no-brand-verified-wording-unless-company-validated
```

## Acceptance token

`ai_assisted_profile_footnote_standardized_globally`
