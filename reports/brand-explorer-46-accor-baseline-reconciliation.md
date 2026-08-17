# Protected 46 Accor baseline reconciliation

Generated: 2026-07-28T19:49:20.864Z

## Ready state

**protected_46_accor_baseline_reconciled_wave14_may_resume**

- Wave 14 post-image cleanup may resume: **true**
- Airtable writes: **0**
- Active universe probe: **46**
- Quality freeze count: **46** / 46

## Root cause

Wave 13 Accor Active brands were dropped from identity maps when FACTORY_PREVIEW became Wave-14-only. Without resolveActiveUniverseRecordId(slug), PVQL/footnote/quality fall back to slug-as-name → brand_not_found → PVQL blocker → quality remediation_required (40 vs 46). Not a Wave 14 Stage 5 write defect.

## Affected brands

| Brand | Slug | Record ID |
| --- | --- | --- |
| Mama Shelter | mama-shelter | recXCZCK05XXYX7Q8 |
| Mercure | mercure | recevrLJ3m6rIug3S |
| Novotel | novotel | recQE2lSSSSyuUrMQ |
| Pullman | pullman | recFW9kfqKfOjv7Z1 |
| Fairmont | fairmont-hotels-and-resorts | recJhPaDVU3YUDQUt |
| SO/ | so-hotels-and-resorts | recTJdPlr4mDs9app |

## Code paths fixed

- `lib/partner-intelligence/brand-explorer-wave13-active-identity-anchors.js`
- `lib/partner-intelligence/brand-explorer-active-universe.js (EXTRA_ACTIVE_IDENTITY_ANCHORS + alias resolve)`
- `lib/partner-intelligence/brand-explorer-46-active-public-full-baseline.js (BASELINE_46_WAVE13_PUBLIC_SEVEN via getWave13ActiveIdentityBySlug)`
- `lib/partner-intelligence/brand-explorer-ai-assisted-footnote.js (WAVE13_REGION_BASIS fairmont/so aliases)`
- `lib/partner-intelligence/brand-explorer-public-visibility-quality-lock.js (prefer recordId / active-universe resolve)`
- `lib/partner-intelligence/brand-explorer-ai-assisted-footnote-standardization.js (prefer recordId)`
- `lib/partner-intelligence/brand-explorer-image-role-match.js (ignore Accor DAM false positives on Airtable CDN hashes)`

## Protections

- No Airtable / Presentation / image / Brand Status / release / CV / Source / Registry / Wave 14 writes
- Gates not weakened; footnote enriched path preserved

## Accor quality refresh

| Slug | Recommendation | Composite | Blockers | PVQL |
| --- | --- | --- | --- | --- |
| mama-shelter | approve_for_baseline_freeze | 97 | 0 | pass |
| mercure | approve_for_baseline_freeze | 97 | 0 | pass |
| ibis | approve_for_baseline_freeze | 97 | 0 | pass |
| novotel | approve_for_baseline_freeze | 97 | 0 | pass |
| pullman | approve_for_baseline_freeze | 98 | 0 | pass |
| fairmont-hotels-and-resorts | approve_for_baseline_freeze | 97 | 0 | pass |
| so-hotels-and-resorts | approve_for_baseline_freeze | 97 | 0 | pass |

