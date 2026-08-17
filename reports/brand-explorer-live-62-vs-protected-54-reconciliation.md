# Brand Explorer — Live 62 vs Protected 54 Reconciliation

Generated: 2026-08-05T00:23:57.331Z
Mode: **read-only** (no Airtable / Brand Explorer / Brand Status / release / VIC sandbox writes)

## Acceptance status

**`live_62_is_intentional_wave15_release_needs_medium_cleanup`**

Recommended branch: **Path A — Wave 15 was intentionally promoted/released**

VIC sandbox patch: **HELD** — hold_sandbox_until_live_semantic_universe_matches_protected_expectations — currently live=62 vs frozen=54; Path A requires Medium cleanup + 62 freeze before sandbox

## Executive summary

- Protected freeze remains `frozen_54_active_public_full_baseline_semantic_clean_flex_held` (expected Active = 54).
- Live Active/Live universe = **62** (Brand Basics Brand Status Active/Live SoT).
- The **8** live additions are **exactly** Wave 15 Hilton brands — intentional status promotion + public release (2026-08-04).
- Four Points Flex remains **held** (`Under Review`; not Active).
- Protected 54 per-brand baseline checks **PASS**; aggregate FAIL is universe count only (expected after Wave 15).
- Semantic C/H/M = **0 / 0 / 7** — all Medium findings on Wave 15 Home2 / Homewood / Spark.
- No unintended non-Wave15 Active drift.

## 1. Protected 54 vs live 62

| Check | Result |
|-------|--------|
| Freeze decision | `frozen_54_active_public_full_baseline_semantic_clean_flex_held` |
| Protected expected Active | 54 |
| Live Active count | 62 |
| Wave 15 plan expected final | 62 |
| Protected 54 per-brand regression | PASS (all 54) |
| Aggregate 54 baseline test | FAIL (count 62 + 8 Wave 15 unexpected) |
| Four Points Flex held | yes (Under Review) |

## 2. Eight live additions (Wave 15)

| Brand | Slug | Record ID | Protected 54? | Live Active? | Brand Status | Release fields | Founder Review | SoT path | Classification |
|-------|------|-----------|---------------|--------------|--------------|----------------|----------------|----------|----------------|
| DoubleTree by Hilton | `doubletree-by-hilton` | `rechVYWQ5ikRnr99B` | no | yes | Active | Approved=true, Ready=true | true | Brand Status Active/Live | `intentional_wave15_release_confirmed` |
| Hampton by Hilton | `hampton-by-hilton` | `rectRvOWQPaL6FkzZ` | no | yes | Active | Approved=true, Ready=true | true | Brand Status Active/Live | `intentional_wave15_release_confirmed` |
| Hilton Garden Inn | `hilton-garden-inn` | `recrvdAjRlXxPvPPF` | no | yes | Active | Approved=true, Ready=true | true | Brand Status Active/Live | `intentional_wave15_release_confirmed` |
| Hilton Hotels & Resorts | `hilton-hotels-and-resorts` | `recWubG3rhiS1BaWi` | no | yes | Active | Approved=true, Ready=true | true | Brand Status Active/Live | `intentional_wave15_release_confirmed` |
| Home2 Suites by Hilton | `home2-suites-by-hilton` | `reccZ4zV6wMav7a2i` | no | yes | Active | Approved=true, Ready=true | true | Brand Status Active/Live | `intentional_wave15_release_confirmed` |
| Homewood Suites by Hilton | `homewood-suites-by-hilton` | `recZjYI4nYflGHFNR` | no | yes | Active | Approved=true, Ready=true | true | Brand Status Active/Live | `intentional_wave15_release_confirmed` |
| Spark by Hilton | `spark-by-hilton` | `recfv66er4Ch2vJDO` | no | yes | Active | Approved=true, Ready=true | true | Brand Status Active/Live | `intentional_wave15_release_confirmed` |
| Tru by Hilton | `tru-by-hilton` | `recJLiMTv4W8VgO9L` | no | yes | Active | Approved=true, Ready=true | true | Brand Status Active/Live | `intentional_wave15_release_confirmed` |

Wave 15 evidence:
- Status promotion apply: **true** (Under Review → Active for all 8)
- Public release apply: **true** · ready: `wave15_eight_brand_release_complete_ready_for_62_freeze_or_post_release_cleanup`

## 3. Medium semantic findings (7)

| # | Brand | Slug | Section | Field / failure | Current text (excerpt) | Issue type | Owner-facing risk | Proposed fix | Requires write? |
|---|-------|------|---------|-----------------|------------------------|------------|-------------------|--------------|-----------------|
| 1 | Home2 Suites by Hilton | `home2-suites-by-hilton` | Geographic Footprint | `cala_label_without_support` | Verified CALA Home2 Suites operating examples are not yet available in official brand materials at this review pass. Do | stale geography label | Owner may read process/review-hold language as live CALA footprint | Rewrite footprint.region.cala to steward-approved CALA-absent owner copy (no review-pass process language) | yes |
| 2 | Home2 Suites by Hilton | `home2-suites-by-hilton` | Where This Brand Creates the Most Value | `weak_owner_value_cues_overview.scenario.3` | (empty / title cue) | weak owner-facing copy | Scenario card does not meet owner-value bar (Kimpton/Curio/Design Hotels) | Rewrite scenario to owner-value topic with distinct image | yes |
| 3 | Homewood Suites by Hilton | `homewood-suites-by-hilton` | Geographic Footprint | `cala_label_without_support` | Verified CALA Homewood Suites operating examples are not yet available in official brand materials at this review pass. | stale geography label | Owner may read process/review-hold language as live CALA footprint | Rewrite footprint.region.cala to steward-approved CALA-absent owner copy (no review-pass process language) | yes |
| 4 | Homewood Suites by Hilton | `homewood-suites-by-hilton` | Where This Brand Creates the Most Value | `weak_owner_value_cues_overview.scenario.2` | (empty / title cue) | weak owner-facing copy | Scenario card does not meet owner-value bar (Kimpton/Curio/Design Hotels) | Rewrite scenario to owner-value topic with distinct image | yes |
| 5 | Homewood Suites by Hilton | `homewood-suites-by-hilton` | Where This Brand Creates the Most Value | `weak_owner_value_cues_overview.scenario.3` | (empty / title cue) | weak owner-facing copy | Scenario card does not meet owner-value bar (Kimpton/Curio/Design Hotels) | Rewrite scenario to owner-value topic with distinct image | yes |
| 6 | Homewood Suites by Hilton | `homewood-suites-by-hilton` | Value Creation Scenarios | `sentence_case_title_valueOwners.scenario.4` | (empty / title cue) | owner-facing terminology issue | Title casing/terminology reads unpolished vs gold bar | Fix scenario title to sentence-case owner-value topic | yes |
| 7 | Spark by Hilton | `spark-by-hilton` | Where This Brand Creates the Most Value | `sentence_case_title_overview.scenario.3` | (empty / title cue) | owner-facing terminology issue | Title casing/terminology reads unpolished vs gold bar | Rewrite scenario to owner-value topic with distinct image | yes |

## 4. Gate / audit checklist

| Check | Result |
|-------|--------|
| `protected54BaselineTest` | **FAIL_expected_universe_drift** — Intentional Wave 15 expansion — not accidental drift. Protected 54 identity/quality still pass. |
| `activeUniverseSourceOfTruth` | **PASS_explained** — active=62 |
| `globalActiveSemanticAudit` | **PASS_with_universe_mismatch** — active=62 |
| `quietSequentialPvql` | **PASS** — Quiet/live PVQL covers Active=62 with overallPass; all 8 Wave 15 lockPass=true. Baseline also forced live PVQL. |
| `quietSequentialQuality` | **WAVE15_SUBSET_RUN_FULL_62_STILL_NEEDED_BEFORE_FREEZE** — Wave 15 quiet quality: 7 approve_for_baseline_freeze + Spark remediation_required (placeholder overview.featured_application). Full 62 quiet quality still required before 62 freeze. |
| `footnoteAudit` | **PASS_enriched_62** — Enriched footnote audit pass=62 fail=0 (covers live Active universe). |
| `recentMomentumEvidence` | **PASS** — npm run test:brand-explorer-recent-momentum-evidence-quality — All 3 brand(s) passed |
| `mandatoryReleaseGates` | **PASS** — npm run test:brand-explorer-mandatory-release-gates |
| `globalActiveSemanticAuditFreshNote` | **USED_EXISTING_FRESH_REFRESH** — Did not re-run --fresh in this pass after baseline live PVQL to avoid 429 thrash; refresh already from Wave15 post-release window with Active=62 C/H/M=0/0/7. |

## 5. Decision

**Selected: Path A** — Wave 15 was intentionally promoted/released.

Not selected:
- Path B — unapproved Wave 15 (rejected)
- Path C — unintended non-Wave15 brands (rejected; unexpectedOutsideWave15 = [])
- Path D — SoT reporting mismatch (rejected; live 62 is real Brand Status expansion)

### Path A next steps

- Do not run Mexico VIC sandbox patch yet
- Remediate 7 Medium semantic findings (Home2 / Homewood / Spark presentation content)
- Remediate Spark overview.featured_application placeholder blocker (quality gate)
- Re-run global active semantic audit expecting 62
- Run full quiet sequential quality for all 62 Active brands
- Confirm quiet sequential PVQL still PASS for 62
- Freeze new protected 62 Active/Live public-full baseline
- Then reconsider VIC sandbox patch decision

## 6. VIC sandbox posture

**Remain held.** Do not apply Mexico VIC → BE sandbox patch until Medium cleanup completes and a new **62** protected baseline is frozen.

## Data contract / impact

- Tables: Brand Basics (Status/release read); Brand Explorer Presentation (Medium findings surface)
- Active SoT: `OR({Brand Status}='Active', {Brand Status}='Live')`
- **Change impact: Low** (read-only diagnostic). Path A remediation later = High.
- Fields touched this diagnostic: none.

