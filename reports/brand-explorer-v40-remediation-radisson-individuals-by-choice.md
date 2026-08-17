# v40 Remediation — radisson-individuals-by-choice

- Blockers: 18
- Patches: 16 (16 copy, 0 hide)
- Safe for generic apply: 16
- Property review: keep_all_visible_extras — 3 visible property examples with imageUrl (minimum 3; extras allowed).

## Blocker types
- `founder_review_missing`: 1
- `active_approval_missing`: 1
- `render_contract_issue`: 1
- `loi_language`: 5
- `net_contribution_language`: 3
- `visible_url`: 4
- `fee_stack_language`: 3

## DOM projection
- forbidden strings: 20 → 0
- visible URLs: 4 → 0
- internal notes: 0 → 0
- empty cards: 0 → 0
- expected displayState after remediation: `draft_applied_with_defects`
- still blocked by founder review: **yes**
- still blocked by active approval: **yes**
- unlock in v40: **no**

## Sample patches (first 15)
### economics.fee.join · Body (`rec0ev2ZO9ZgVL1DY`)
- Reason: Owner-copy scrub: loi_language
- Before: Application and entry fees; training and opening support; initial franchise fee; technology implementation; plan review and inspection. Basis varies by keys, market, and new build 
- After: Application and entry fees; training and opening support; initial franchise fee; technology implementation; plan review and inspection. Basis varies by keys, market, and new build 
- Checks: owner=pass forbidden=pass safeApply=true

### commercial.kpi.lens · Body (`rec0poafahDnIeDoG`)
- Reason: Owner-copy scrub: net_contribution_language
- Before: Net contribution after fees and channel costs
- After: owner economics after fees and channel costs
- Checks: owner=pass forbidden=pass safeApply=true

### materials.caseStudy · Body (`rec27BNoxQUZm86k7`)
- Reason: Owner-copy scrub: visible_url
- Before: Urban, Colombia, CALA, Soft collection

Medellín, Colombia (El Poblado context)

Upper-upscale collection · lifestyle

CALA urban lifestyle

V Grand / Individuals Medellín cited in
- After: Urban, Colombia, CALA, Soft collection
Medellín, Colombia (El Poblado context)
Upper-upscale collection · lifestyle
CALA urban lifestyle
V Grand / Individuals Medellín cited in Cho
- Checks: owner=pass forbidden=pass safeApply=true

### operations.operator_compat.fit · Body (`rec7bMWfw1feB77Fw`)
- Reason: Owner-copy scrub: net_contribution_language
- Before: Strong fit when operator has upper-upscale soft collection depth, can meet prototype/PIP, and models net contribution after 6.0% royalty on gross room revenues (Radisson-family CHI
- After: Strong fit when operator has upper-upscale soft collection depth, can meet prototype/PIP, and models owner economics after 6.0% royalty on gross room revenues (Radisson-family CHI 
- Checks: owner=pass forbidden=pass safeApply=true

### materials.caseStudy · Body (`recIV2MhFScfuUoOn`)
- Reason: Owner-copy scrub: visible_url
- Before: Urban, Colombia, CALA, Soft collection

Medellín, Colombia (El Poblado context)

Upper-upscale collection · lifestyle

CALA urban lifestyle

V Grand / Individuals Medellín cited in
- After: Urban, Colombia, CALA, Soft collection
Medellín, Colombia (El Poblado context)
Upper-upscale collection · lifestyle
CALA urban lifestyle
V Grand / Individuals Medellín cited in Cho
- Checks: owner=pass forbidden=pass safeApply=true

### economics.cash.steadystate · Body (`recNVQ41VqdxZ2Ddl`)
- Reason: Owner-copy scrub: fee_stack_language
- Before: Owner funds: recurring fee stack (upper-upscale soft collection) plus program participation once stabilized.

Brand provides: Choice distribution, QA cadence, and benchmarks—not pr
- After: Owner funds: recurring participation cost categories (upper-upscale soft collection) plus program participation once stabilized.
Brand provides: Choice distribution, QA cadence, an
- Checks: owner=pass forbidden=pass safeApply=true

### economics.legal · Title (`recV9vvghknIzsRQB`)
- Reason: Owner-copy scrub: loi_language
- Before: LOI & process
- After: letter of intent or commercial proposal & process
- Checks: owner=pass forbidden=pass safeApply=true

### economics.legal · Body (`recV9vvghknIzsRQB`)
- Reason: Owner-copy scrub: loi_language
- Before: Clarify binding vs exploratory LOI terms and design approval gates early.
- After: Clarify binding vs exploratory letter of intent or commercial proposal terms and design approval gates early.
- Checks: owner=pass forbidden=pass safeApply=true

### insight.summary · Body (`recXgUmzgCJNLHrEa`)
- Reason: Owner-copy scrub: fee_stack_language
- Before: Radisson Individual (Choice) fits when you want upper-upscale soft collection Choice distribution with clear prototype economics and Choice Privileges participation—not a mismatche
- After: Radisson Individual (Choice) fits when you want upper-upscale soft collection Choice distribution with clear prototype economics and Choice Privileges participation—not a mismatche
- Checks: owner=pass forbidden=pass safeApply=true

### economics.intro · Body (`recYz8HoVymxGSM5G`)
- Reason: Owner-copy scrub: loi_language
- Before: Radisson Individual (Choice) (upper-upscale soft collection) economics are illustrative only—not a quote or substitute for the franchise terms, LOI, or advisors.
- After: Radisson Individual (Choice) (upper-upscale soft collection) economics are illustrative only—not a quote or substitute for the franchise terms, letter of intent or commercial propo
- Checks: owner=pass forbidden=pass safeApply=true

### materials.caseStudy · Body (`recag9AjmqFvKe8cN`)
- Reason: Owner-copy scrub: visible_url
- Before: Urban, Colombia, CALA, Soft collection

Medellín, Colombia (El Poblado context)

Upper-upscale collection · lifestyle

CALA urban lifestyle

V Grand / Individuals Medellín cited in
- After: Urban, Colombia, CALA, Soft collection
Medellín, Colombia (El Poblado context)
Upper-upscale collection · lifestyle
CALA urban lifestyle
V Grand / Individuals Medellín cited in Cho
- Checks: owner=pass forbidden=pass safeApply=true

### loyalty.owner_lens · Body (`receHFqVaro9K9p2B`)
- Reason: Owner-copy scrub: net_contribution_language
- Before: Model loyalty as net contribution—member discounts, fulfillment, and franchise terms-reported room mix from Choice Privileges in
- After: Model loyalty as owner economics—member discounts, fulfillment, and franchise terms-reported room mix from Choice Privileges in
- Checks: owner=pass forbidden=pass safeApply=true

### footprint.geo.summary · Body (`recgzgae0RkrAyrQo`)
- Reason: Owner-copy scrub: loi_language
- Before: Radisson Individual (Choice): upper-upscale soft collection · Choice Americas focus · confirm international authorization in LOI
- After: Radisson Individual (Choice): upper-upscale soft collection · Choice Americas focus · confirm international authorization in letter of intent or commercial proposal
- Checks: owner=pass forbidden=pass safeApply=true

### economics.opening.financials · Body (`recilN5DRafMLI6IN`)
- Reason: Owner-copy scrub: fee_stack_language
- Before: Financial planning themes (no deal-specific amounts):

Front-loaded standards, FF&E, and technology

Working capital through ramp

Fee stack stepping from opening-weighted to stabi
- After: Financial planning themes (no deal-specific amounts):
Front-loaded standards, FF&E, and technology
Working capital through ramp
participation cost categories stepping from opening-
- Checks: owner=pass forbidden=pass safeApply=true

### materials.caseStudy · Body (`recrFHKf76tznXSEd`)
- Reason: Owner-copy scrub: visible_url
- Before: Urban, Colombia, CALA, Soft collection

Medellín, Colombia (El Poblado context)

Upper-upscale collection · lifestyle

CALA urban lifestyle

V Grand / Individuals Medellín cited in
- After: Urban, Colombia, CALA, Soft collection
Medellín, Colombia (El Poblado context)
Upper-upscale collection · lifestyle
CALA urban lifestyle
V Grand / Individuals Medellín cited in Cho
- Checks: owner=pass forbidden=pass safeApply=true
