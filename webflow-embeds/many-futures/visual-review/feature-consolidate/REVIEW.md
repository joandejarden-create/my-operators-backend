# Feature consolidation — local review

**Webflow not updated. Not published. Click-only unchanged.**

## Goal
Reduce vertical length by limiting feature reuse and combining related libraries.

## Mapping (2 features per question)

| Q | Features |
|---|----------|
| 01 Rebrand | Brand Explorer, Smart Matching |
| 02 Operators | Operator Explorer, Smart Matching |
| 03 Affiliation | Deal Readiness, Fee Estimator |
| 04 Residences | Opportunity Review, Brand Explorer |
| 05 Confidential | Outreach Setup, Opportunity Review |
| 06 Market | Market Alerts, Dealality Radar |
| 07 Actions | Action Tracking, Submit Proposal |
| 08 Proposals | Deal Compare, Fee Estimator |
| 09 Clarify | Deal Readiness, **Clause & Financial Libraries** (combined) |

## Reuse cap (≤2)

- Brand Explorer, Smart Matching, Opportunity Review, Deal Readiness, Fee Estimator → 2×
- All others → 1×
- Radar was 4× → now **1×** (market only)
- Deal Compare / Submit Proposal were each 2× overlapping → now **1×** each

## Removed from panels (to cut height / duplication)

- Dealality Radar from rebrand, affiliation, residences
- Deal Compare from actions (kept on proposals)
- Submit Proposal from proposals (kept on actions)
- Proof & Track Record (operators) — unique card dropped to keep operators at 2
- Footprint & Growth (market) — unique card dropped to keep market at 2
- Separate Clause Library + Financial Term Library → one combined card

## QA

- Hotel / questions: 568 / 568
- Click-only: hover does not select; click sticks
- All panels `two-panel`; workspace heights ~717–876px (was often 1100+)
