# Cumulative Proposal Assembly — Gate 1 correction

**Status:** PASS — screenshot regressions repaired in cumulative PROPOSED

## Contract
- LEFT = CURRENT saved profile
- RIGHT = CURRENT baseline + only complete non-regressive staged changes
- Incomplete factory drafts are held out (not publication)

## Key Differentiators
- Identity bullets proposed: **4** (required 4; raw candidate 1)
- Commercial bullets proposed: **4** (required 4; raw candidate 1)

## Loyalty Benefits / Program Strengths
- Proposed populated cards: **6** (required 6; raw candidate nonempty 0)
- Titles: 9,000+ Hotels · No Blackout Dates · Fifth Night Free · 2026 Tier Refresh · Confirmable Upgrades · Digital Key

## Provenance counts
```json
{
  "RETAINED_FROM_CURRENT": 125,
  "MODIFIED_EXISTING": 63,
  "NEWLY_RESEARCHED": 0,
  "NEW_COMPONENT": 35,
  "INCOMPLETE_DRAFT": 102,
  "RESEARCH_REQUIRED": 23
}
```

## Hashes
- CURRENT blocks: `6482c62c9cad72f0a0ccfdc677a4007e56dc6f4e8f7755394852326742fe761e`
- Raw candidate: `c9e0b818d49847771f69840bc64b62872c39a7df58d243fc51ed2f9c54f1a4d8`
- Cumulative PROPOSED: `12f87e9a275e7bc7cd71f885f3079dbe0d1bed14479b45ae4e515c53f98db20c`

## Permanent URL
http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1

## Browser verification (permanent URL)

URL: http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1

Observed live banner:
- LEFT = Canonical live Presentation (225 blocks)
- RIGHT = BE_CUMULATIVE_PROPOSAL_ASSEMBLER_V1 (proposal hash `6fc50c0851ba164b…`)
- Raw candidate held as change-set only (`phase2h-component-parity`, 214 blocks) — **not** wholesale RIGHT column
- Older fallback used: **NO**
- Incomplete drafts held out: **125**
- Provenance legend visible (RETAINED / MODIFIED / NEW / INCOMPLETE DRAFT / RESEARCH REQUIRED)

Screenshot regressions repaired:
- Key Differentiators: LEFT and RIGHT both show full identity + commercial bullets (no empty pads from weak candidate)
- Key Benefits & Program Strengths: LEFT and RIGHT both show **6** populated cards (9,000+ Hotels, No Blackout Dates, Fifth Night Free, 2026 Tier Refresh, Confirmable Upgrades, Digital Key) — no “Loyalty proof points” placeholder

Screenshots: `screenshots/after-overview-key-differentiators.png`, `screenshots/after-loyalty-key-benefits.png`

## Root cause (corrected)

`loadRightBrand` previously applied `applyCandidateOverlayToBrand`, which **replaced all Presentation blocks** with the incomplete Phase 2H factory candidate. That is why RIGHT looked worse than LEFT.

## Corrected path

CURRENT saved brand shell + `BeCumulativeProposalAssembler.buildCumulativeProposedBrand` → only complete non-regressive staged slot groups apply; incomplete drafts stay in the held-out list.

No Airtable writes. No Gate 2 regeneration. Curio/Ascend not approved.
