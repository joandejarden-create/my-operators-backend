# Brand Explorer — Factory Preview Status Correction Plan

> **Plan only — do not apply automatically.** No Brand Status writes in Factory Preview Mode task.
> Generated `2026-07-23T22:35:55.946Z`.

## Context

Founder temporarily set factory candidates to **Active** for local visual review. That correctly broke the protected 24-brand baseline (`excluded_brand_became_active_without_baseline_revision`).

**Factory Preview Mode** now allows full internal profile render without Brand Status Active/Live.

## Default recommendation

Revert all three candidates to **Under Review** and use Factory Preview Mode for visual QA until factory pass + founder approval.

| Slug | Current | Recommended | Rationale |
|---|---|---|---|
| `tapestry-collection-by-hilton` | Active | **Under Review** | revert to Under Review (unless founder intentionally wants production Active now) |
| `dazzler-by-wyndham` | Active | **Under Review** | revert to Under Review (unless founder intentionally wants production Active now) |
| `trademark-collection-by-wyndham` | Active | **Under Review** | revert to Under Review (unless founder intentionally wants production Active now) |

## Per-candidate decision checklist

### Tapestry Collection by Hilton

- [ ] **Option A (recommended):** Revert Brand Status → **Under Review**; preview via Factory Preview Mode.
- [ ] **Option B:** Keep **Active/Live** only if founder intentionally wants this brand in the production active universe **now** — then run intentional baseline revision (24→N) before merge.
- Current status: `Active`
- Prior expected at freeze: `Under Review`
- In active universe today: true
- Baseline failure attributed: true
- Factory preview URL: `/brand-explorer-combined.html?brandId=reccXxMHEh7NNRhIE&beInternalPreview=1&factoryPreview=1`

### Dazzler by Wyndham

- [ ] **Option A (recommended):** Revert Brand Status → **Under Review**; preview via Factory Preview Mode.
- [ ] **Option B:** Keep **Active/Live** only if founder intentionally wants this brand in the production active universe **now** — then run intentional baseline revision (24→N) before merge.
- Current status: `Active`
- Prior expected at freeze: `Under Review`
- In active universe today: true
- Baseline failure attributed: true
- Factory preview URL: `/brand-explorer-combined.html?brandId=rec5CNMM4ZUD7ZHlM&beInternalPreview=1&factoryPreview=1`

### Trademark Collection by Wyndham

- [ ] **Option A (recommended):** Revert Brand Status → **Under Review**; preview via Factory Preview Mode.
- [ ] **Option B:** Keep **Active/Live** only if founder intentionally wants this brand in the production active universe **now** — then run intentional baseline revision (24→N) before merge.
- Current status: `Active`
- Prior expected at freeze: `Under Review`
- In active universe today: true
- Baseline failure attributed: true
- Factory preview URL: `/brand-explorer-combined.html?brandId=recob7tgHRryRSbeO&beInternalPreview=1&factoryPreview=1`

## After correction

```bash
npm run test:brand-explorer-24-active-public-full-baseline
npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only
npm run test:brand-explorer-factory-preview-mode
```

Expected after revert: Active/Live count returns to **24**; baseline PASS; factory preview still works for Under Review candidates.

## Explicit non-goals

- Do not write Company Validated / Source Library / Registry.
- Do not mark `active_profile_ready` or public-full via preview.
- Do not evolve the 24-brand baseline unless Option B is chosen intentionally.

