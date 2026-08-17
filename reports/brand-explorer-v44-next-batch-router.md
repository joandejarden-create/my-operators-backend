# v44 Next Batch Router

Generated: 2026-07-21T15:34:27.590Z

Routes incomplete brands using live OS state. Does **not** unlock. Does **not** process released golden brands.

## Recommendation

**Preferred: A** — Design Hotels is the only incomplete on apply_remediation — process it alone first to avoid mixing Presentation apply with visual-pack image work.

## Options

### A. Design Hotels alone (apply_remediation)

- Feasible: **true**
- Brands: design-hotels
- Why: Single brand on apply_remediation — likely fastest path to move one incomplete forward without mixing image work.

### B. MGallery + Hotel Indigo (image_remediation)

- Feasible: **true**
- Brands: mgallery-collection, hotel-indigo
- Why: Shared image_remediation action; keep Design Hotels / SLH out of this batch.

### C. MGallery + SLH (lifestyle/collection)

- Feasible: **true**
- Brands: mgallery-collection, small-luxury-hotels-of-the-world
- Why: Lifestyle/collection pairing on image_remediation; Hotel Indigo deferred.

### D. All four incomplete as one batch

- Feasible: **true**
- Brands: hotel-indigo, mgallery-collection, design-hotels, small-luxury-hotels-of-the-world
- Why: Only if OS actions stay separated per brand (image vs apply). Higher coordination cost; avoid if Design Hotels apply can land alone first.

## Incomplete brand routing

| Brand | State | Exact blocker | Next action | Expected | Batch OK | Exact next command |
|---|---|---|---|---|---|---|
| hotel-indigo | draft_applied_with_defects | source_coverage_ready | image_remediation | image_remediation | true | `npm run brand-explorer-active-profile-factory -- --brand hotel-indigo --stage visual-pack --dry-run` |
| mgallery-collection | draft_applied_with_defects | source_coverage_ready | image_remediation | image_remediation | true | `npm run brand-explorer-active-profile-factory -- --brand mgallery-collection --stage visual-pack --dry-run` |
| design-hotels | draft_applied_with_defects | no_visible_source_urls | apply_remediation | apply_remediation | true | `npm run brand-explorer-v40c-economics-chrome-remediation -- \ --brands design-hotels \ --apply \ --approve-brand-explorer-v40C-economics-chrome-remediation \ --confirm-no-company-validation-claim \ --confirm-no-active-profile-approval \ --confirm-no-source-library-changes \ --confirm-no-registry-changes \ --confirm-no-image-field-changes \ --confirm-external-profiles-remain-locked \ --confirm-internal-preview-owner-copy-clean \ --confirm-brand-only` |
| small-luxury-hotels-of-the-world | draft_applied_with_defects | source_coverage_ready | image_remediation | image_remediation | true | `npm run brand-explorer-active-profile-factory -- --brand small-luxury-hotels-of-the-world --stage visual-pack --dry-run` |

## Per-brand detail

### hotel-indigo
- Current state: `draft_applied_with_defects` (display=`hidden_incomplete`)
- Full profile: **false** (must stay false)
- Exact blocker: source_coverage_ready
- Allowed next action: **image_remediation**
- Blocked actions: apply_active_release, apply_draft, founder_visual_review, external_full_render
- Batch processing possible: **true**
- Rationale: Live gallery/property imageUrl gates incomplete.
```
npm run brand-explorer-active-profile-factory -- --brand hotel-indigo --stage visual-pack --dry-run
```

### mgallery-collection
- Current state: `draft_applied_with_defects` (display=`hidden_incomplete`)
- Full profile: **false** (must stay false)
- Exact blocker: source_coverage_ready
- Allowed next action: **image_remediation**
- Blocked actions: apply_active_release, apply_draft, founder_visual_review, external_full_render
- Batch processing possible: **true**
- Rationale: Live gallery/property imageUrl gates incomplete.
```
npm run brand-explorer-active-profile-factory -- --brand mgallery-collection --stage visual-pack --dry-run
```

### design-hotels
- Current state: `draft_applied_with_defects` (display=`draft_applied_with_defects`)
- Full profile: **false** (must stay false)
- Exact blocker: no_visible_source_urls
- Allowed next action: **apply_remediation**
- Blocked actions: apply_active_release, founder_visual_review, external_full_render
- Batch processing possible: **true**
- Rationale: v40C residual Presentation patches pending. Internal preview may look clean via renderer scrub, but Airtable copy still needs apply.
```
npm run brand-explorer-v40c-economics-chrome-remediation -- \
  --brands design-hotels \
  --apply \
  --approve-brand-explorer-v40C-economics-chrome-remediation \
  --confirm-no-company-validation-claim \
  --confirm-no-active-profile-approval \
  --confirm-no-source-library-changes \
  --confirm-no-registry-changes \
  --confirm-no-image-field-changes \
  --confirm-external-profiles-remain-locked \
  --confirm-internal-preview-owner-copy-clean \
  --confirm-brand-only
```

### small-luxury-hotels-of-the-world
- Current state: `draft_applied_with_defects` (display=`hidden_incomplete`)
- Full profile: **false** (must stay false)
- Exact blocker: source_coverage_ready
- Allowed next action: **image_remediation**
- Blocked actions: apply_active_release, apply_draft, founder_visual_review, external_full_render
- Batch processing possible: **true**
- Rationale: Live gallery/property imageUrl gates incomplete.
```
npm run brand-explorer-active-profile-factory -- --brand small-luxury-hotels-of-the-world --stage visual-pack --dry-run
```

## Actions by group

```json
{
  "image_remediation": [
    "hotel-indigo",
    "mgallery-collection",
    "small-luxury-hotels-of-the-world"
  ],
  "apply_remediation": [
    "design-hotels"
  ]
}
```

## Guardrails

- Do not unlock incomplete brands
- Do not change Company Validated
- Do not change released brand content
- Only process brands routed by OS
