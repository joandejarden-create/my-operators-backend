# Choice Legacy Mini-Batch Extraction v1

Generated: 2026-07-06T23:19:25.274Z
Mode: **apply**
Airtable modified: **yes**

## Executive summary

| Metric | Count |
|--------|------:|
| Brands | 4 |
| Sources in scope | 11 |
| Proposed facts (total) | 28 |
| High-confidence facts | 28 |
| Facts needing review | 0 |
| Brands ready for batch apply | 4 |
| Brands to split out | 0 |
| Duplicate warnings | 16 |

### Proposed facts by brand

- **Country Inn & Suites by Choice**: 6
- **Radisson by Choice**: 7
- **Radisson Individuals by Choice**: 7
- **Radisson RED by Choice**: 8

### Batch apply command

```bash
npm run choice-legacy-batch-extract -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-extract
```

## Country Inn & Suites by Choice

- Record: `recaayt9u7YYg8h7Y`
- Sources in scope: `recPqQVEe01xl3aQ6`, `recOx0YuUUOfaLPBe`
- Proposed facts: **6**
- Skipped candidates: 3
- Extraction quality: **good_for_steward_review**
- Apply recommended: **yes**
- Split out: **no**
- Governance readiness (after fact approval): likely_eligible_for_stewardship_recompute_and_publish_readiness_audit

### Sources

- `recPqQVEe01xl3aQ6` — Country Inn & Suites by Choice — prototype brochure (local) (Development Brochure) · extraction=Yes
- `recOx0YuUUOfaLPBe` — Country Inn & Suites by Choice — Choice consumer brand page (Brand Page) · extraction=Yes

### Proposed facts

| Field | Source | Value preview | Confidence |
|-------|--------|---------------|------------|
| `be.identity.parentCompany` | Country Inn & Suites by Choice — Choice consumer brand page | Choice Hotels International | High |
| `be.positioning.summary` | Country Inn & Suites by Choice — prototype brochure (local) | Country Inn & Suites® Prototype Guide 2 3 Country Inn & Suites by Radisson: Gene | High |
| `be.overview.developmentModel` | Country Inn & Suites by Choice — prototype brochure (local) | Development types include franchise development (per Choice brand materials). | High |
| `be.overview.whyValue` | Country Inn & Suites by Choice — prototype brochure (local) | owners to guests, Country Inn & Suites properties are built to embody the refres | High |
| `be.loyalty.programName` | Country Inn & Suites by Choice — Choice consumer brand page | Choice Privileges | High |
| `be.positioning.guestPromise` | Country Inn & Suites by Choice — Choice consumer brand page | comfortable beds help you drift off into sound slumber. | High |

### Duplicate warnings

- `be.identity.parentCompany` from `recPqQVEe01xl3aQ6`
- `be.positioning.summary` from `recOx0YuUUOfaLPBe`
- `be.loyalty.programName` from `recPqQVEe01xl3aQ6`

### Risks / caveats

- Facts remain Pending until human stewardship approval.
- Do not treat extracted positioning as Company Validated.
- Consumer HTML may include booking boilerplate — review evidence quotes.

## Radisson by Choice

- Record: `recywbx1YQSTCPqW1`
- Sources in scope: `recLsN4M2G1z0rJBa`, `recsnDjbEjI5yCxmm`, `recdOL9QhOIrAxYRP`
- Proposed facts: **7**
- Skipped candidates: 2
- Extraction quality: **good_for_steward_review**
- Apply recommended: **yes**
- Split out: **no**
- Governance readiness (after fact approval): likely_eligible_for_stewardship_recompute_and_publish_readiness_audit

### Sources

- `recLsN4M2G1z0rJBa` — Radisson by Choice — brand book (local) (Development Brochure) · extraction=Yes
- `recsnDjbEjI5yCxmm` — Radisson by Choice — Choice consumer brand page (Brand Page) · extraction=Yes
- `recdOL9QhOIrAxYRP` — Radisson by Choice — Choice press kit / media center (Press Release) · extraction=Yes

### Proposed facts

| Field | Source | Value preview | Confidence |
|-------|--------|---------------|------------|
| `be.positioning.guestPromise` | Radisson by Choice — brand book (local) | guest experience pillars: (1) Brilliant Basics (2) Memorable Moments (3) Local E | High |
| `be.overview.whyValue` | Radisson by Choice — brand book (local) | value proposition. | High |
| `be.overview.typicalUseCase` | Radisson by Choice — brand book (local) | travelers worldwide. | High |
| `be.identity.parentCompany` | Radisson by Choice — Choice press kit / media center | Choice Hotels International | High |
| `be.positioning.summary` | Radisson by Choice — Choice press kit / media center | Radisson exists to champion the enduring spirit of hospitality, innovating with  | High |
| `be.loyalty.programName` | Radisson by Choice — Choice consumer brand page | Choice Privileges | High |
| `be.footprint.geoIntro` | Radisson by Choice — Choice press kit / media center | Americas as of September 30, 2024. | High |

### Duplicate warnings

- `be.identity.parentCompany` from `recsnDjbEjI5yCxmm`
- `be.positioning.summary` from `recsnDjbEjI5yCxmm`

### Risks / caveats

- Facts remain Pending until human stewardship approval.
- Do not treat extracted positioning as Company Validated.
- Consumer HTML may include booking boilerplate — review evidence quotes.

## Radisson Individuals by Choice

- Record: `recRyvM8OmLlDj9G7`
- Sources in scope: `recin2kwFrIlQNKmp`, `recgDFeovQZZuiXZ8`, `reccfAdMmZI5XmRJK`
- Proposed facts: **7**
- Skipped candidates: 5
- Extraction quality: **good_for_steward_review**
- Apply recommended: **yes**
- Split out: **no**
- Governance readiness (after fact approval): likely_eligible_for_stewardship_recompute_and_publish_readiness_audit

### Sources

- `recin2kwFrIlQNKmp` — Radisson Individuals by Choice — pitch deck (local) (Development Brochure) · extraction=Yes
- `recgDFeovQZZuiXZ8` — Radisson Individuals by Choice — Choice consumer brand page (Brand Page) · extraction=Yes
- `reccfAdMmZI5XmRJK` — Radisson Individuals by Choice — Choice press kit / media center (Press Release) · extraction=Yes

### Proposed facts

| Field | Source | Value preview | Confidence |
|-------|--------|---------------|------------|
| `be.identity.parentCompany` | Radisson Individuals by Choice — Choice press kit / media center | Choice Hotels International | High |
| `be.positioning.summary` | Radisson Individuals by Choice — pitch deck (local) | Radisson Individuals is primed to deliver on the benefits of the needs of today’ | High |
| `be.positioning.guestPromise` | Radisson Individuals by Choice — pitch deck (local) | guest experience and identify opportunities for operational efficiencies. | High |
| `be.overview.developmentModel` | Radisson Individuals by Choice — pitch deck (local) | Development types include conversions (per Choice brand materials). | High |
| `be.overview.whyValue` | Radisson Individuals by Choice — pitch deck (local) | value proposition helping to drive franchisee performance and top-line growth fo | High |
| `be.loyalty.programName` | Radisson Individuals by Choice — Choice consumer brand page | Choice Privileges | High |
| `be.footprint.geoIntro` | Radisson Individuals by Choice — Choice press kit / media center | 15 hotels with a combined 1,732 rooms in operation across the Americas as of Sep | High |

### Duplicate warnings

- `be.identity.parentCompany` from `recin2kwFrIlQNKmp`
- `be.positioning.summary` from `recgDFeovQZZuiXZ8`
- `be.loyalty.programName` from `recin2kwFrIlQNKmp`
- `be.identity.parentCompany` from `recgDFeovQZZuiXZ8`
- `be.positioning.summary` from `reccfAdMmZI5XmRJK`

### Risks / caveats

- Facts remain Pending until human stewardship approval.
- Do not treat extracted positioning as Company Validated.
- Consumer HTML may include booking boilerplate — review evidence quotes.

## Radisson RED by Choice

- Record: `recmKqo7M7mLZgRqQ`
- Sources in scope: `recz8fmzzxvsP6V6J`, `recPrdF1bltJtq4JS`, `rechXybBKQsqTIsCz`
- Proposed facts: **8**
- Skipped candidates: 6
- Extraction quality: **good_for_steward_review**
- Apply recommended: **yes**
- Split out: **no**
- Governance readiness (after fact approval): likely_eligible_for_stewardship_recompute_and_publish_readiness_audit

### Sources

- `recz8fmzzxvsP6V6J` — Radisson RED by Choice — Upscale by Choice brand overview (local) (Development Brochure) · extraction=Yes
- `recPrdF1bltJtq4JS` — Radisson RED by Choice — Choice consumer brand page (Brand Page) · extraction=Yes
- `rechXybBKQsqTIsCz` — Radisson RED by Choice — Choice press kit / media center (Press Release) · extraction=Yes

### Proposed facts

| Field | Source | Value preview | Confidence |
|-------|--------|---------------|------------|
| `be.identity.parentCompany` | Radisson RED by Choice — Choice press kit / media center | Choice Hotels International | High |
| `be.positioning.summary` | Radisson RED by Choice — Upscale by Choice brand overview (local) | Radisson Blu brings distinctive design to top urban and resort markets, appealin | High |
| `be.positioning.guestPromise` | Radisson RED by Choice — Upscale by Choice brand overview (local) | consistently dependable. | High |
| `be.overview.developmentModel` | Radisson RED by Choice — Upscale by Choice brand overview (local) | Development types include new construction, conversions, adaptive reuse (per Cho | High |
| `be.overview.whyValue` | Radisson RED by Choice — Upscale by Choice brand overview (local) | developers and owners the reliability they need, and the flexibility they want. | High |
| `be.overview.typicalUseCase` | Radisson RED by Choice — Upscale by Choice brand overview (local) | traveler while offering compelling investment fundamentals for astute franchisee | High |
| `be.loyalty.programName` | Radisson RED by Choice — Choice consumer brand page | Choice Privileges | High |
| `be.footprint.geoIntro` | Radisson RED by Choice — Choice press kit / media center | Americas as of September 30, 2023. | High |

### Duplicate warnings

- `be.identity.parentCompany` from `recz8fmzzxvsP6V6J`
- `be.positioning.summary` from `recPrdF1bltJtq4JS`
- `be.loyalty.programName` from `recz8fmzzxvsP6V6J`
- `be.identity.parentCompany` from `recPrdF1bltJtq4JS`
- `be.positioning.summary` from `rechXybBKQsqTIsCz`
- `be.positioning.guestPromise` from `rechXybBKQsqTIsCz`

### Risks / caveats

- Facts remain Pending until human stewardship approval.
- Do not treat extracted positioning as Company Validated.
- Consumer HTML may include booking boilerplate — review evidence quotes.

## Does not do

- Rebuild Brand Explorer content or overwrite Brand Setup fields
- Approve facts automatically
- Publish governance or set Company Validated
- Extract from development JS-shell pages or RHG/global sources
- Create gap facts
- Change UI, scoring, BAS, OAS, OCS, Deal Readiness, or schema
