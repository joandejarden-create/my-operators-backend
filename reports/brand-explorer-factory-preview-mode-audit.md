# Brand Explorer — Factory Preview Mode Audit

> Read-only. Generated `2026-07-23T22:35:55.946Z`. No Airtable writes.

## Summary

| Check | Result |
|---|---|
| Active universe count | **27** (expected 24) |
| Active universe drift | YES |
| Drift slugs | tapestry-collection-by-hilton, dazzler-by-wyndham, trademark-collection-by-wyndham |
| Factory display state | `factory_preview_internal` |
| Invariant (preview ≠ universe) | PASS |
| Recommendation | `revert_factory_candidates_to_under_review_and_use_factory_preview` |

## Candidates

### Tapestry Collection by Hilton (`tapestry-collection-by-hilton`)

| Field | Value |
|---|---|
| Record ID | `reccXxMHEh7NNRhIE` |
| Current Brand Status | **Active** |
| Prior expected status | Under Review |
| Presentation row count | 106 |
| displayState (public) | `draft_applied_with_defects` |
| displayState (factory preview) | `factory_preview_internal` |
| shouldRenderFullProfile (public) | false |
| canRenderFactoryPreview | true |
| In active universe | true |
| Baseline fails because of it | true |
| PVQL publicFullProfile | false |
| Factory banner in preview HTML | true |
| Public HTML preparation gate | true |
| Preview URL | `/brand-explorer-combined.html?brandId=reccXxMHEh7NNRhIE&beInternalPreview=1&factoryPreview=1` |

### Dazzler by Wyndham (`dazzler-by-wyndham`)

| Field | Value |
|---|---|
| Record ID | `rec5CNMM4ZUD7ZHlM` |
| Current Brand Status | **Active** |
| Prior expected status | Under Review |
| Presentation row count | 0 |
| displayState (public) | `hidden_incomplete` |
| displayState (factory preview) | `factory_preview_internal` |
| shouldRenderFullProfile (public) | false |
| canRenderFactoryPreview | false |
| In active universe | true |
| Baseline fails because of it | true |
| PVQL publicFullProfile | false |
| Factory banner in preview HTML | false |
| Public HTML preparation gate | true |
| Preview URL | `/brand-explorer-combined.html?brandId=rec5CNMM4ZUD7ZHlM&beInternalPreview=1&factoryPreview=1` |
| Note | **No Presentation rows yet** — Factory Preview eligibility is true, but full atelier render waits on Presentation materialization. |

### Trademark Collection by Wyndham (`trademark-collection-by-wyndham`)

| Field | Value |
|---|---|
| Record ID | `recob7tgHRryRSbeO` |
| Current Brand Status | **Active** |
| Prior expected status | Under Review |
| Presentation row count | 0 |
| displayState (public) | `hidden_incomplete` |
| displayState (factory preview) | `factory_preview_internal` |
| shouldRenderFullProfile (public) | false |
| canRenderFactoryPreview | false |
| In active universe | true |
| Baseline fails because of it | true |
| PVQL publicFullProfile | false |
| Factory banner in preview HTML | false |
| Public HTML preparation gate | true |
| Preview URL | `/brand-explorer-combined.html?brandId=recob7tgHRryRSbeO&beInternalPreview=1&factoryPreview=1` |
| Note | **No Presentation rows yet** — Factory Preview eligibility is true, but full atelier render waits on Presentation materialization. |

## Baseline failure sample

- `active_universe_count_changed:27_expected_24`
- `unexpected_active_brand:dazzler-by-wyndham`
- `unexpected_active_brand:tapestry-collection-by-hilton`
- `unexpected_active_brand:trademark-collection-by-wyndham`
- `excluded_brand_became_active_without_baseline_revision:tapestry-collection-by-hilton`
- `excluded_brand_present_in_active_universe:tapestry-collection-by-hilton`

## How to preview locally

1. Keep Brand Status as Draft / Under Review (do **not** set Active for visual QA).
2. Open: `/brand-explorer-combined.html?brandId=<recordId>&beInternalPreview=1&factoryPreview=1`
3. Optional env: `BRAND_EXPLORER_FACTORY_PREVIEW=1` and `BRAND_EXPLORER_FACTORY_PREVIEW_SLUGS=...`

Banner must read: **Factory Preview — Not Public / Not Active Baseline**

