# Brand Explorer Factory Gap Matrix Audit v27A

Generated: 2026-07-10T01:54:03.005Z
Mode: **dry-run** · Airtable modified: **no**
Company Validated untouched: **yes**

## Why five brands score Contract 63
- All five non-Tribute brands fail the same three contract sections (Portfolio Context, Standard Detail, Demand Scenario), yielding identical 5/8 = 63 scores.
- Formula: round(readySections / 8 * 100) => 5/8 = 63

### Shared failing sections

## Gap matrix
### Tribute Portfolio (`tribute-portfolio`)
- Contract: **100** (8/8 ready)
- Final QA: **ready** (95)
- Active-profile ready: **yes**
- Critical/high defects: 0/0
- Visual defects: 1 (score 100)
- Carryover defects: 0
- Governance defects: 2; governed ready: yes
- Presentation rows: 163 (125 slot keys)
- Failing sections:
  - none

### Curio Collection by Hilton (`curio-collection`)
- Contract: **100** (8/8 ready)
- Final QA: **blocked** (61)
- Active-profile ready: **no**
- Critical/high defects: 1/4
- Visual defects: 7 (score 47)
- Carryover defects: 0
- Governance defects: 3; governed ready: no
- Presentation rows: 211 (159 slot keys)
- Failing sections:
  - none

### Kimpton Hotels (`kimpton`)
- Contract: **100** (8/8 ready)
- Final QA: **blocked** (46)
- Active-profile ready: **no**
- Critical/high defects: 1/11
- Visual defects: 7 (score 42)
- Carryover defects: 0
- Governance defects: 1; governed ready: no
- Presentation rows: 197 (152 slot keys)
- Failing sections:
  - none

### Radisson Blu by Choice (`radisson-blu`)
- Contract: **88** (7/8 ready)
- Final QA: **blocked** (54)
- Active-profile ready: **no**
- Critical/high defects: 2/10
- Visual defects: 8 (score 32)
- Carryover defects: 0
- Governance defects: 1; governed ready: yes
- Presentation rows: 218 (161 slot keys)
- Failing sections:
  - Standard Detail / Where Available: `required_backfill + founder_review_required` (10/1)

### Radisson by Choice (`radisson`)
- Contract: **88** (7/8 ready)
- Final QA: **blocked** (52)
- Active-profile ready: **no**
- Critical/high defects: 2/10
- Visual defects: 7 (score 35)
- Carryover defects: 0
- Governance defects: 1; governed ready: yes
- Presentation rows: 200 (149 slot keys)
- Failing sections:
  - Standard Detail / Where Available: `required_backfill + founder_review_required` (10/1)

### Ascend Hotel Collection (`ascend`)
- Contract: **100** (8/8 ready)
- Final QA: **blocked** (58)
- Active-profile ready: **no**
- Critical/high defects: 2/10
- Visual defects: 8 (score 32)
- Carryover defects: 0
- Governance defects: 1; governed ready: yes
- Presentation rows: 211 (162 slot keys)
- Failing sections:
  - none

## Real blockers vs false positives
- real_visual_gap: 28
- tribute_calibrated_false_negative: 3
- copy_carryover_risk: 2

## Recommended next brand
- **Ascend Hotel Collection** (`ascend`) — rank score 85
- Best balance of reference completeness, non-Marriott parent (if Hilton/Choice), governance cleanliness, and carryover risk after contract generalization

## Recommended next build path
- Generalize contract first: **yes**
- Three of three shared contract failures are Tribute/Marriott-calibrated (portfolio ladder, standards governance, demand completion shape). Generalize before second-brand end-to-end apply.
- Next writer: **brand-explorer-required-section-contract-generalization-writer (v27B)**
  - Replace Marriott-specific portfolio context gate with parent-company ladder resolver (Marriott/Hilton/Choice)
  - Extract brand-agnostic standards approval evaluator from tribute-standard-detail-review-approval-writer
  - Relax demand row completion to brand-neutral owner-implication fields
  - Stop emitting Tribute recordId/name in contract report.brand for non-Tribute loads
- Multi-brand apply-approved safe: **no**
- No brands passed apply-approved safety; batch aggregate reports 0 safe for apply-approved

## Hardcoded assumption scan (sample)
- `lib/partner-intelligence/brand-explorer-required-section-population-contract.js:23` — Hardcoded Tribute record ID
- `lib/partner-intelligence/brand-explorer-required-section-population-contract.js:241` — Hardcoded tribute-portfolio slug
- `lib/partner-intelligence/brand-explorer-required-section-population-contract.js:242` — Hardcoded tribute-portfolio slug
- `lib/partner-intelligence/brand-explorer-required-section-population-contract.js:529` — Hardcoded tribute-portfolio slug
- `lib/partner-intelligence/brand-explorer-required-section-population-contract.js:530` — Hardcoded tribute-portfolio slug
- `lib/partner-intelligence/brand-explorer-required-section-population-contract.js:510` — Marriott-specific string
- `lib/partner-intelligence/brand-explorer-required-section-population-contract.js:6` — Tribute-prefixed writer reference
- `lib/partner-intelligence/brand-explorer-required-section-population-contract.js:410` — Tribute-prefixed writer reference
- `lib/partner-intelligence/brand-explorer-required-section-population-contract.js:492` — Tribute-prefixed writer reference
- `lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js:13` — Hardcoded tribute-portfolio slug
- `lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js:102` — Hardcoded tribute-portfolio slug
- `lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js:547` — Hardcoded tribute-portfolio slug
- `lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js:559` — Hardcoded tribute-portfolio slug
- `lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js:749` — Hardcoded tribute-portfolio slug
- `lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js:812` — Hardcoded tribute-portfolio slug
- `lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js:816` — Hardcoded tribute-portfolio slug
- `lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js:818` — Hardcoded tribute-portfolio slug
- `lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js:909` — Hardcoded tribute-portfolio slug
- `lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js:58` — Bonvoy-specific string
- `lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js:267` — FDD-specific rule
- … and 158 more