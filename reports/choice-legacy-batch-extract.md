# Choice Legacy Mini-Batch Extraction v1

Generated: 2026-07-06T22:19:46.156Z
Mode: **dry_run**
Airtable modified: **no**

## Executive summary

| Metric | Count |
|--------|------:|
| Brands | 3 |
| Sources in scope | 9 |
| Proposed facts (total) | 0 |
| High-confidence facts | 0 |
| Facts needing review | 0 |
| Brands ready for batch apply | 0 |
| Brands to split out | 0 |
| Duplicate warnings | 23 |

### Proposed facts by brand

- **Comfort Inn & Suites**: 0
- **Everhome Suites**: 0
- **Quality Inn**: 0

### Batch apply command

```bash
npm run choice-legacy-batch-extract -- --apply --approve-choice-legacy-batch-extract
```

## Comfort Inn & Suites

- Record: `recOzH5iAE1xEjyD0`
- Sources in scope: `recZFPfGRo5C9FF2Q`, `recxm2Jxqvi2n2I8K`, `recRbi8CjS8BVt4Z3`
- Proposed facts: **0**
- Skipped candidates: 14
- Extraction quality: **weak_needs_more_sources_or_curation**
- Apply recommended: **no**
- Split out: **no**
- Governance readiness (after fact approval): blocked_until_more_approved_facts

### Sources

- `recZFPfGRo5C9FF2Q` — Comfort Inn & Suites — Choice development brochure (local) (Development Brochure) · extraction=Yes
- `recxm2Jxqvi2n2I8K` — Comfort Inn & Suites — Choice consumer brand page (Brand Page) · extraction=Yes
- `recRbi8CjS8BVt4Z3` — Comfort Inn & Suites — Choice press kit / media center (Press Release) · extraction=Yes

### Proposed facts

_None._

### Duplicate warnings

- `be.positioning.summary` from `recxm2Jxqvi2n2I8K`
- `be.overview.developmentModel` from `recxm2Jxqvi2n2I8K`
- `be.identity.brandName` from `recxm2Jxqvi2n2I8K`
- `be.identity.parentCompany` from `recxm2Jxqvi2n2I8K`
- `be.positioning.summary` from `recRbi8CjS8BVt4Z3`
- `be.overview.developmentModel` from `recRbi8CjS8BVt4Z3`
- `be.loyalty.programName` from `recxm2Jxqvi2n2I8K`

### Risks / caveats

- Facts remain Pending until human stewardship approval.
- Do not treat extracted positioning as Company Validated.
- Consumer HTML may include booking boilerplate — review evidence quotes.

## Everhome Suites

- Record: `recqkkrsevi4r9ibj`
- Sources in scope: `rechRqlbx7DF4YCCV`, `rec28KQ9ubpynVfTq`, `rechbWISi8BQwTqGb`
- Proposed facts: **0**
- Skipped candidates: 16
- Extraction quality: **weak_needs_more_sources_or_curation**
- Apply recommended: **no**
- Split out: **no**
- Governance readiness (after fact approval): blocked_until_more_approved_facts

### Sources

- `rechRqlbx7DF4YCCV` — Everhome Suites — franchise development presentation (local) (Development Brochure) · extraction=Yes
- `rec28KQ9ubpynVfTq` — Everhome Suites — Choice consumer brand page (Brand Page) · extraction=Yes
- `rechbWISi8BQwTqGb` — Everhome Suites — Choice press kit / media center (Press Release) · extraction=Yes

### Proposed facts

_None._

### Duplicate warnings

- `be.identity.brandName` from `rechRqlbx7DF4YCCV`
- `be.identity.parentCompany` from `rechRqlbx7DF4YCCV`
- `be.positioning.summary` from `rec28KQ9ubpynVfTq`
- `be.loyalty.programName` from `rechRqlbx7DF4YCCV`
- `be.identity.brandName` from `rec28KQ9ubpynVfTq`
- `be.identity.parentCompany` from `rec28KQ9ubpynVfTq`
- `be.positioning.summary` from `rechbWISi8BQwTqGb`
- `be.overview.developmentModel` from `rechbWISi8BQwTqGb`

### Risks / caveats

- Facts remain Pending until human stewardship approval.
- Do not treat extracted positioning as Company Validated.
- Consumer HTML may include booking boilerplate — review evidence quotes.

## Quality Inn

- Record: `recd8o4k1JddhkRWW`
- Sources in scope: `recmEnl9wcLfSA4Mk`, `recpsFcGtpvib16s0`, `recfh3rpBaKo0U0H1`
- Proposed facts: **0**
- Skipped candidates: 16
- Extraction quality: **weak_needs_more_sources_or_curation**
- Apply recommended: **no**
- Split out: **no**
- Governance readiness (after fact approval): blocked_until_more_approved_facts

### Sources

- `recmEnl9wcLfSA4Mk` — Quality Inn — Choice development brochure (local) (Development Brochure) · extraction=Yes
- `recpsFcGtpvib16s0` — Quality Inn — Choice consumer brand page (Brand Page) · extraction=Yes
- `recfh3rpBaKo0U0H1` — Quality Inn — Choice press kit / media center (Press Release) · extraction=Yes

### Proposed facts

_None._

### Duplicate warnings

- `be.identity.parentCompany` from `recmEnl9wcLfSA4Mk`
- `be.positioning.summary` from `recpsFcGtpvib16s0`
- `be.overview.developmentModel` from `recpsFcGtpvib16s0`
- `be.identity.brandName` from `recpsFcGtpvib16s0`
- `be.identity.parentCompany` from `recpsFcGtpvib16s0`
- `be.positioning.summary` from `recfh3rpBaKo0U0H1`
- `be.overview.developmentModel` from `recfh3rpBaKo0U0H1`
- `be.loyalty.programName` from `recpsFcGtpvib16s0`

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
