# Wave 13 — SO/ Status Promotion + Public Release

SO/ (`so-hotels-and-resorts`, `recTJdPlr4mDs9app`) **Brand Status → Active** and **release fields applied**.
Active universe: **45 → 46**. Founder accepted cleanly-unavailable steward posture for
`snapshot.*` scale fields and `footprint.primary_regions` (not invented).

## Ready statement

**Writes complete:** `wave13_so_status_and_release_fields_applied_universe_46`

**Baseline freeze:** **not ready** until PVQL public-full-only passes.

**Blocker:** `section_pattern_parity` fails tab-factory / PVQL (`dated_cards_below_min:0`, empty MEA region panel, `growth_priorities_not_brand_specific`). No content patches in this status/release task.

## Founder acceptance

- `founder_accepts_cleanly_unavailable_steward_posture`: **true**
- `promotion_recommendation`: **approve_for_status_promotion_and_public_release**
- Reports: `reports/brand-explorer-wave13-so-founder-acceptance.{json,md}`

## Writes applied (SO/ only)

### Stage 9 — Brand Status

- Under Review → **Active**
- Report: `reports/brand-explorer-wave13-so-status-promotion.{json,md}`

### Stage 10 — Public release

- Active Profile Approved / Ready for Active Profile / Active Profile Approved Date / Founder Visual Review Pass
- Intentional public restore registry: `so-hotels-and-resorts` added
- Report: `reports/brand-explorer-wave13-so-public-release.{json,md}`

## Read-path fix (no Airtable content write)

Basics Brand Name is **SO/** while factory Presentation Brand Name is **SO/ Hotels & Resorts**.
`api/brand-library.js` now merges presentation Brand Name aliases so public display loads the full cohort (107 blocks) and `shouldRenderFullProfile=true`.

## Validation (post-release)

| Gate | Result |
| --- | --- |
| Active universe count | **46** (`reconcilesTo46=true`) |
| SO/ shouldRenderFullProfile | **true** (`active_profile_ready`) |
| Completeness (SO/) | PASS |
| Golden (SO/) | PASS |
| Evidence (SO/) | PASS |
| Image uniqueness (SO/) | PASS |
| Image role-match (SO/) | PASS |
| No-empty (SO/) | retest after 429 (rate limit) |
| PVQL public-full-only | **FAIL** — SO/ `tab_factory_audit` (section_pattern_parity) |
| Tab-factory (SO/) | completeness 80/80 PASS; overall auditPass **false** (pattern parity) |
| Protected fields | CV / Source / Registry / active 45 / House / Morgans / Radisson untouched |

## Guardrails honored

- No presentation body / image rewrites in Stage 9–10
- No Company Validated / Source Library / Registry approval changes
- Active 45 brands untouched
- House of Originals excluded · Morgans Originals untouched · Radisson Collection excluded

## Next task (required before 46 baseline freeze)

Content micro-cleanup for SO/ section-pattern parity only:

1. Recent Momentum — ensure dated cards meet min (dates detectable in card titles/bodies)
2. Geographic footprint — suppress or fill empty MEA region panel; keep brand-specific geo_intro
3. Growth priorities — brand-specific growth themes / editorial bar

Then re-run:

```bash
npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only
npm run brand-explorer-24-tab-section-quality-audit -- --dry-run
npm run test:brand-explorer-mandatory-release-gates
```

Only after those pass: freeze protected **46** Active/Live public-full baseline.

## Commands

```bash
npm run brand-explorer-wave13-factory -- --stage so-status-promotion --dry-run
npm run brand-explorer-wave13-factory -- --stage so-public-release --dry-run
```

Last updated: 2026-07-28
