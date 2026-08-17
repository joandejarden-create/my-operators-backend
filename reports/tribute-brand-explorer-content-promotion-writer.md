# Tribute Brand Explorer Content Promotion Writer v11

Generated: 2026-07-08T18:03:31.177Z
Mode: **dry-run** · Airtable modified: **no**
Brand: Tribute Portfolio `recCvV0PuZOi8c3hC`

## Root cause (prior apply failure)
v11 previously PATCHed Brand Setup - Brand Basics using display labels (Brand Profile Analysis, Brand Standards, Questions Owners Should Ask) that do not exist in live Airtable. Completed brands store this copy in Brand Explorer Presentation slots (overview.typical_use_case, standards.intro, standards.questions).

## Schema preflight
Preflight passed: **no**
- `idealAssetProfile` · Brand Profile Analysis → Brand Setup - Brand Explorer Presentation · slot `overview.typical_use_case` · field **Body** (`fldkBC9QelUQs4rKO`) · writable: **no**
  - Note: Nonblank existing Body preserved (no overwrite).
  - Current: Best suited for independent boutique, lifestyle, and leisure hotels with a clear local point of view, where owners want Marriott Bonvoy, distribution, and comm...
  - Proposed: Best suited for independent boutique, lifestyle, and leisure hotels with a clear local point of view, where owners want Marriott Bonvoy, distribution, and comm...
- `standards` · Brand Standards → Brand Setup - Brand Explorer Presentation · slot `standards.intro` · field **Body** (`fldkBC9QelUQs4rKO`) · writable: **no**
  - Note: Nonblank existing Body preserved (no overwrite).
  - Current: Owners should expect a soft-brand path with more flexibility than a prototype-led flag, but still with Marriott brand, quality, systems, loyalty, and operating...
  - Proposed: Owners should expect a soft-brand path with more flexibility than a prototype-led flag, but still with Marriott brand, quality, systems, loyalty, and operating...
- `questionsOwnersShouldAsk` · Questions Owners Should Ask → Brand Setup - Brand Explorer Presentation · slot `standards.questions` · field **Body** (`fldkBC9QelUQs4rKO`) · writable: **no**
  - Note: Nonblank existing Body preserved (no overwrite).
  - Current: Which elements of the hotel’s identity, design, F&B, and local programming can remain unique under Tribute Portfolio? What brand, systems, quality, and Bonvoy ...
  - Proposed: Which elements of the hotel’s identity, design, F&B, and local programming can remain unique under Tribute Portfolio? What brand, systems, quality, and Bonvoy ...

## Sections targeted
- idealAssetProfile
- standards

## Current gaps (from v10)
- idealAssetProfile: Generic/demo-like
- standards: Complete/comparable
- sourceLinks: Complete/comparable (excluded from v11 writes)

## Proposed updates
- None.

## Apply blockers
- Schema preflight failed: one or more target presentation fields are missing or not writable.
- idealAssetProfile (overview.typical_use_case): Nonblank existing Body preserved (no overwrite).
- standards (standards.intro): Nonblank existing Body preserved (no overwrite).
- questionsOwnersShouldAsk (standards.questions): Nonblank existing Body preserved (no overwrite).

## Guardrails
- Brand Website untouched: **yes** (current: https://tribute-portfolio.marriott.com/)
- sourceLinks excluded: **yes**
- Images untouched: **yes**
- Unrelated presentation rows untouched: **yes**
- Company Validated fields untouched: **yes**

## Exact apply command (if approved)

```bash
npm run tribute-brand-explorer-content-promotion-writer -- --apply --approve-tribute-brand-explorer-content-promotion --allow-human-review-copy
```

Expected completion score after apply: **97/100**
Reaches completed-brand content parity after apply: **not yet**
