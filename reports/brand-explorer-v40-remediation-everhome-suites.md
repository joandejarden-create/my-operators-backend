# v40 Remediation — everhome-suites

- Blockers: 14
- Patches: 14 (14 copy, 0 hide)
- Safe for generic apply: 14
- Property review: extras_acceptable_founder_confirm — 4 visible examples exceed minimum 3; UI should handle extras. Founder confirm sort order is clean. No hide required.

## Blocker types
- `founder_review_missing`: 1
- `active_approval_missing`: 1
- `render_contract_issue`: 1
- `net_contribution_language`: 4
- `fee_stack_language`: 6
- `loi_language`: 1

## DOM projection
- forbidden strings: 14 → 0
- visible URLs: 0 → 0
- internal notes: 0 → 0
- empty cards: 0 → 0
- expected displayState after remediation: `draft_applied_with_defects`
- still blocked by founder review: **yes**
- still blocked by active approval: **yes**
- unlock in v40: **no**

## Sample patches (first 15)
### loyalty.owner_lens · Body (`rec0Y4xmaV0kvGoig`)
- Reason: Owner-copy scrub: net_contribution_language
- Before: Model loyalty as net contribution—member discounts, fulfillment, and disclosed room mix from Choice Privileges.
- After: Model loyalty as owner economics—member discounts, fulfillment, and disclosed room mix from Choice Privileges.
- Checks: owner=pass forbidden=pass safeApply=true

### economics.opening.financials · Body (`rec0cQbTo7drQs6c8`)
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

### economics.kpi.fee_stack · Title (`rec3bOVNqdlPo6KSc`)
- Reason: Owner-copy scrub: fee_stack_language
- Before: Fee Stack Diligence
- After: participation cost categories Diligence
- Checks: owner=pass forbidden=pass safeApply=true

### economics.kpi.fee_stack · Body (`rec3bOVNqdlPo6KSc`)
- Reason: Owner-copy scrub: fee_stack_language
- Before: Owners should map franchise, marketing, and technology fees during underwriting. Dealality summarizes considerations only—confirm fee stack details with Choice development represen
- After: Owners should map franchise, marketing, and technology fees during underwriting. Dealality summarizes considerations only—confirm participation cost categories details with Choice 
- Checks: owner=pass forbidden=pass safeApply=true

### economics.cash.steadystate · Body (`rec8VUQKpFUwbNgPE`)
- Reason: Owner-copy scrub: fee_stack_language
- Before: Owner funds: recurring fee stack (midscale extended-stay) plus program participation once stabilized.

Brand provides: Choice distribution, QA cadence, and benchmarks—not property 
- After: Owner funds: recurring participation cost categories (midscale extended-stay) plus program participation once stabilized.
Brand provides: Choice distribution, QA cadence, and bench
- Checks: owner=pass forbidden=pass safeApply=true

### economics.legal · Title (`recG1ZdbAq7YhxmYe`)
- Reason: Owner-copy scrub: loi_language
- Before: LOI & process
- After: letter of intent or commercial proposal & process
- Checks: owner=pass forbidden=pass safeApply=true

### economics.legal · Body (`recG1ZdbAq7YhxmYe`)
- Reason: Owner-copy scrub: loi_language
- Before: Clarify binding vs exploratory LOI terms and design approval gates early.
- After: Clarify binding vs exploratory letter of intent or commercial proposal terms and design approval gates early.
- Checks: owner=pass forbidden=pass safeApply=true

### commercial.kpi.lens · Body (`recJPrv6m4Ij2gIbn`)
- Reason: Owner-copy scrub: net_contribution_language
- Before: Net contribution after fees and channel costs
- After: owner economics after fees and channel costs
- Checks: owner=pass forbidden=pass safeApply=true

### insight.summary · Body (`recOefkiB0zPhOSfZ`)
- Reason: Owner-copy scrub: fee_stack_language
- Before: Everhome Suites fits when you want midscale extended-stay Choice distribution with purpose-built prototype economics and Choice Privileges participation—not a mismatched tier (e.g.
- After: Everhome Suites fits when you want midscale extended-stay Choice distribution with purpose-built prototype economics and Choice Privileges participation—not a mismatched tier (e.g.
- Checks: owner=pass forbidden=pass safeApply=true

### footprint.geo_intro · Body (`recXdqCz2oV87JUhr`)
- Reason: Owner-copy scrub: net_contribution_language
- Before: Everhome Suites is Choice's midscale extended-stay brand—introduced in 2020 as the first new core midscale brand in nearly a decade. U.S. development is the primary growth story in
- After: Everhome Suites is Choice's midscale extended-stay brand—introduced in 2020 as the first new core midscale brand in nearly a decade. U.S. development is the primary growth story in
- Checks: owner=pass forbidden=pass safeApply=true

### footprint.growth_fit · Body (`recfR4dX74ajXYjqd`)
- Reason: Owner-copy scrub: fee_stack_language
- Before: employment and medical-corridor greenfield or conversion
suburban extended-stay with project and relocation demand
operators using professional third-party management
owners compar
- After: employment and medical-corridor greenfield or conversion
suburban extended-stay with project and relocation demand
operators using professional third-party management
owners compar
- Checks: owner=pass forbidden=pass safeApply=true

### footprint.openings · Body (`recl7HK2wkO08l3vA`)
- Reason: Owner-copy scrub: fee_stack_language
- Before: Suburban, New Jersey, U.S., Extended stay

A New York metro extended-stay example that illustrates Everhome's apartment-style prototype in a suburban market context, useful for own
- After: Suburban, New Jersey, U.S., Extended stay
A New York metro extended-stay example that illustrates Everhome's apartment-style prototype in a suburban market context, useful for owne
- Checks: owner=pass forbidden=pass safeApply=true

### footprint.openings · Case Summary Interpretation (`recl7HK2wkO08l3vA`)
- Reason: Owner-copy scrub: fee_stack_language
- Before: Metro access and weekly corporate demand mix drive performance—validate fee stack and parking/utility costs locally.
- After: Metro access and weekly corporate demand mix drive performance—validate participation cost categories and parking/utility costs locally.
- Checks: owner=pass forbidden=pass safeApply=true

### operations.operator_compat.fit · Body (`recs7bynFUCej1kd9`)
- Reason: Owner-copy scrub: net_contribution_language
- Before: Strong fit when operator has midscale extended-stay depth, can meet prototype/PIP, works with professional third-party management, and models net contribution after 6% royalty on r
- After: Strong fit when operator has midscale extended-stay depth, can meet prototype/PIP, works with professional third-party management, and models owner economics after 6% royalty on ro
- Checks: owner=pass forbidden=pass safeApply=true
