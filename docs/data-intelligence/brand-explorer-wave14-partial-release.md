# Wave 14 — Partial Status Promotion + Public Release

> **Ready (write stages):** `wave14_eight_brand_partial_release_complete_flex_held`  
> **Post-release dated momentum:** `wave14_dated_momentum_cleanup_applied_ready_for_pvql_recheck` — quiet PVQL **54/54 PASS**  
> **Freeze 54:** still prefer wait for Flex → 55, or explicit interim-54 decision (protected baseline remains **46**)

## What shipped

Stages **9–10** promoted and publicly released **eight** founder-approved Marriott brands only.

| Brand | Slug | Brand Status | Release fields |
| --- | --- | --- | --- |
| Marriott Hotels | `marriott-hotels` | Active | yes |
| Sheraton | `sheraton` | Active | yes |
| Westin | `westin` | Active | yes |
| Residence Inn by Marriott | `residence-inn-by-marriott` | Active | yes |
| SpringHill Suites by Marriott | `springhill-suites-by-marriott` | Active | yes |
| TownePlace Suites by Marriott | `towneplace-suites-by-marriott` | Active | yes |
| Aloft Hotels | `aloft-hotels` | Active | yes |
| StudioRes | `studiores` | Active | yes |

## Held / excluded

| Brand | Treatment |
| --- | --- |
| Four Points Flex by Sheraton (`four-points-flex-by-sheraton`) | **Held** — Brand Status remains **Under Review**; no release fields; not in intentional restore registry; not in Active universe. Founder A/B/C/D still open. |
| The House of Originals | Excluded (unchanged) |
| Morgans Originals | Untouched |
| Radisson Collection | Untouched |

## Universe

- Before Stage 9: **46** Active/Live
- After Stage 9–10: **54** Active/Live
- Public-full render: **54/54** `shouldRenderFullProfile=true` / `publicFullProfile=true`
- Four Points Flex: outside active universe

## Stage writes (allowed only)

**Stage 9 — Brand Status only** on the eight: `Under Review` → `Active`

**Stage 10 — release fields only** on the eight:

- Active Profile Approved
- Ready for Active Profile
- Active Profile Approved Date
- Founder Visual Review Pass
- Intentional public restore registry inclusion (eight slugs; Flex not added)

## Protected-field safety

- Company Validated / Company Validation Date: untouched
- Source Library status: untouched
- Registry approval/status: untouched
- Presentation content / images: **no** Stage 9–10 rewrites
- Four Points Flex: **no** status or release writes
- Protected original 46 Basics: untouched
- House of Originals / Morgans Originals / Radisson Collection: untouched

## Validation results

| Gate | Result |
| --- | --- |
| Active universe SoT | **54** |
| Quiet PVQL (54 scoped) | **FAIL** — 8 Wave 14 brands fail `tab_factory_audit` only; **0** not-public-full |
| PVQL failure detail | `section_pattern_parity` / Recent Momentum `dated_cards_below_min:0` (cards present; datedCount=0) |
| 24-tab section quality | **do_not_freeze_remediation_required** — 8 Wave 14 `remediation_required` (1 blocker each); 45 approve_for_baseline_freeze |
| Tab-factory audit (eight) | failFindings=0 · empty=0 · **auditPass=false** (`field_complete_after_patch`) |
| Rendered field completeness (eight) | **PASS** 8/8 |
| No-empty rendered components (eight) | **PASS** 8/8 |
| Golden content quality (eight) | **PASS** 8/8 |
| Image uniqueness (eight) | **PASS** 8/8 |
| Image role-match (eight) | **PASS** 8/8 |
| AI-Assisted footnote (enriched) | **PASS** (55/55) |
| Recent momentum evidence (permanent targets) | **PASS** |
| Mandatory release gates | **PASS** |
| Brand Explorer OS `--stage release-readiness --skip-regression` | Ran (exit 0); consolidation path scoped / not a full-54 PVQL substitute |

## May we freeze a 54-brand baseline now?

**No.** Prefer waiting for:

1. Post-release Recent Momentum **dated-card** section-pattern remediation on the eight so PVQL `--public-full-only` and 24-tab quality can pass, and  
2. Four Points Flex founder A/B/C/D + promotion (→ **55**), **or** an explicit interim-54 founder decision.

Protected baseline remains **`frozen_46_active_public_full_baseline`** until then.

## Reports

- `reports/brand-explorer-wave14-partial-status-promotion.{json,md}`
- `reports/brand-explorer-wave14-partial-public-release.{json,md}`
- `reports/brand-explorer-public-visibility-quality-lock-quiet.json`
- `reports/brand-explorer-tab-factory-audit.{json,md}` (eight-brand scope)
- Active-universe SoT / footnote / mandatory-gates under `reports/`

## Next

Do **not** promote Four Points Flex in this packet. Queue a dedicated Wave 14 post-release **Recent Momentum dated-card** remediation for the eight public brands (content write path — out of Stage 9–10 scope), then re-run quiet PVQL / `--public-full-only` and 24-tab quality before any 54 baseline freeze.
