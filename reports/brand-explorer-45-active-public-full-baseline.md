# Brand Explorer — Protected 45 Active/Live Public-Full Baseline

Version: `45-active-public-full-baseline-v1` · Generated: 2026-07-28T00:49:58.939Z
Baseline type: **active_live_public_full**
Freeze decision: **frozen_45_active_public_full_baseline** · frozen=true
Writes: Airtable=false · Presentation=false · Image=false · CV=false · Source=false · Registry=false · Brand Status=false

## 1. Executive summary

This freeze locks the **45** Brand Basics Active/Live Brand Explorer profiles that are public-full, PVQL-clean, and quality `approve_for_baseline_freeze` after Wave 13 partial release (public six) + value-scenario + geo/recent-momentum cleanup. Ready statement upstream: `wave13_public_six_geo_momentum_clean_ready_for_45_or_so_decision`.

| Metric | Value |
|--------|-------|
| Active/Live count | 45 |
| Public-full | 45 |
| shouldRenderFullProfile | 45 |
| PVQL pass | 45 |
| approve_for_baseline_freeze | 45 |
| remediation_required | 0 |
| Evidence quality (mandatory wave) | true |
| Image uniqueness pass | 45 |
| Image role-match pass | 45 |
| Cross-brand image reuse | 0 |
| Wave 13 public six included | 6/6 |
| SO/ held Under Review | true |
| Company Validated = true | 0 |
| Held / excluded probes | 4 |

## 2. Active universe source of truth

- **Name:** Brand Basics Brand Status Active/Live
- **Table:** Brand Setup - Brand Basics
- **Formula:** `OR({Brand Status}='Active', {Brand Status}='Live')`
- **Loader:** `lib/partner-intelligence/brand-explorer-active-universe.js`
- **Version:** active-universe-v1
- **Not the universe:** PRIMARY_RELEASE_SLUGS, prior_23_reconciliation, legacy_23_active_list, FACTORY_SUPPORTED_SLUGS_as_universe, stale_39_brand_list_as_universe, stale_27_brand_list_as_universe, stale_24_brand_list_as_universe
- **Note:** PRIMARY_RELEASE_SLUGS is an operational overlay, not the Active/Live universe (45).

### Predecessor freezes (history, not current enforcement)

- 24 public-full: `brand-explorer-24-active-public-full-baseline.json` (24-active-public-full-baseline-v1)
- 25 Tapestry wave: `brand-explorer-25-active-public-full-baseline.json` (25-active-public-full-baseline-v1)
- Interim 27 Active/Live-only: `brand-explorer-27-active-universe-interim-baseline.json`
- Protected 27 public-full (pre-Wave 12): `brand-explorer-27-active-public-full-baseline.json` (27-active-public-full-baseline-v2)
- Protected 39 public-full (pre-Wave 13): `brand-explorer-39-active-public-full-baseline.json` (39-active-public-full-baseline-v1)

## 3. 45-brand baseline table

| Brand | Slug | Record ID | Status | Full | Display | PVQL | Quality | Uniq | Role | Evidence | OS | Gallery | Scenario | Property | Rows | CV |
|-------|------|-----------|--------|------|---------|------|---------|------|------|----------|----|---------|----------|----------|------|----|
| AC Hotels by Marriott | `ac-hotels-by-marriott` | `rec9aZp7GHtzUEg0c` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 107 | false |
| Ascend Hotel Collection | `ascend` | `reclkgOzvAcBheUSo` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 4 | 213 | false |
| Autograph Collection | `autograph-collection` | `recEJCTDj1zrsjPM6` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 112 | false |
| avid hotels | `avid-hotels` | `recoEarnE8T6sDjZq` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 106 | false |
| Bunkhouse Hotels | `bunkhouse-hotels` | `recGv268Wda31PlSZ` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 107 | false |
| BW Premier Collection | `bw-premier-collection` | `recwXZ5gVZ8ZH8ekA` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 4 | 101 | false |
| BW Signature Collection | `bw-signature-collection` | `recdeh1NsP4gjrv80` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 4 | 101 | false |
| Canopy by Hilton | `canopy-by-hilton` | `recsggfbKlJbjeRP9` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 107 | false |
| City Express by Marriott | `city-express-by-marriott` | `recucEzAS6724tOYA` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 107 | false |
| Comfort Inn & Suites | `comfort-inn-suites` | `recOzH5iAE1xEjyD0` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 5 | 217 | false |
| Country Inn & Suites by Choice | `country-inn-suites` | `recaayt9u7YYg8h7Y` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 9 | 3 | 3 | 219 | false |
| Courtyard by Marriott | `courtyard-by-marriott` | `rec6hye5H8zJmAGv3` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 106 | false |
| Curio Collection by Hilton | `curio-collection` | `receQkxgjlezsc1xg` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 5 | 215 | false |
| Dazzler by Wyndham | `dazzler-by-wyndham` | `rec5CNMM4ZUD7ZHlM` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | pass | — | 6 | 3 | 3 | 108 | false |
| Design Hotels | `design-hotels` | `rec02zPClpWUTCyXM` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 130 | false |
| Even Hotels | `even-hotels` | `recvvmiyReHhiKdoK` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 107 | false |
| Everhome Suites | `everhome-suites` | `recqkkrsevi4r9ibj` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 4 | 212 | false |
| Fairmont | `fairmont-hotels-and-resorts` | `recJhPaDVU3YUDQUt` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | fail | — | 6 | 3 | 3 | 107 | false |
| Handwritten Collection | `handwritten-collection` | `rec7hTXwMRC81EPqz` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 109 | false |
| Holiday Inn Express | `holiday-inn-express` | `recmGmiIqDtAsm01f` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 107 | false |
| Hotel Indigo | `hotel-indigo` | `recegXrqaPiSLGCIe` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 100 | false |
| ibis | `ibis` | `reclFXbpZ5XzLWbGP` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | pass | — | 6 | 3 | 3 | 107 | false |
| Kimpton Hotels | `kimpton` | `recCKuXCmGvxHPfb3` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 5 | 203 | false |
| Mama Shelter | `mama-shelter` | `recXCZCK05XXYX7Q8` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | fail | — | 6 | 3 | 3 | 107 | false |
| Mercure | `mercure` | `recevrLJ3m6rIug3S` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | fail | — | 6 | 3 | 3 | 107 | false |
| MGallery Collection | `mgallery-collection` | `recrWCD1LMqu864oU` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 94 | false |
| Motto by Hilton | `motto-by-hilton` | `reclt44apoi8co0e6` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 107 | false |
| Moxy Hotels | `moxy-hotels` | `recahVIW4aCx0Ao84` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 106 | false |
| Novotel | `novotel` | `recQE2lSSSSyuUrMQ` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | fail | — | 6 | 3 | 3 | 107 | false |
| Preferred Hotels & Resorts | `preferred-hotels-and-resorts` | `recwl5JOYxlChuCAr` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 4 | 101 | false |
| Pullman | `pullman` | `recFW9kfqKfOjv7Z1` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | fail | — | 6 | 3 | 3 | 107 | false |
| Quality Inn | `quality-inn` | `recd8o4k1JddhkRWW` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 208 | false |
| Radisson Blu by Choice | `radisson-blu` | `recWPEvxBQxVVzSq3` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 6 | 222 | false |
| Radisson by Choice | `radisson` | `recywbx1YQSTCPqW1` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 4 | 207 | false |
| Radisson Individuals by Choice | `radisson-individuals-by-choice` | `recRyvM8OmLlDj9G7` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 213 | false |
| Radisson RED by Choice | `radisson-red` | `recmKqo7M7mLZgRqQ` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 4 | 212 | false |
| Small Luxury Hotels of the World | `small-luxury-hotels-of-the-world` | `recjjSnY2opb8P4DG` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 99 | false |
| Suburban Studios | `suburban-studios` | `reclcjg5Foa9Vs5TC` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 209 | false |
| Tapestry Collection by Hilton | `tapestry-collection-by-hilton` | `reccXxMHEh7NNRhIE` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | pass | — | 6 | 3 | 3 | 129 | false |
| Tempo by Hilton | `tempo-by-hilton` | `recqiHq3GHKMj8Meo` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 107 | false |
| Trademark Collection by Wyndham | `trademark-collection-by-wyndham` | `recob7tgHRryRSbeO` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | pass | — | 6 | 3 | 3 | 108 | false |
| Tribute Portfolio | `tribute-portfolio` | `recCvV0PuZOi8c3hC` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 4 | 165 | false |
| Vignette Collection | `vignette-collection` | `recDwzv86TWnz2gGB` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 109 | false |
| Voco Hotels | `voco-hotels` | `recwONQTqGU1jHCsM` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 107 | false |
| WoodSpring Suites | `woodspring-suites` | `recsOd51NzRPYsMko` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 211 | false |

## 4. Wave 13 six-brand release summary

Wave 13 partially promoted **6** Accor brands from the protected **39** baseline to the **45** Active/Live public-full universe. Held: `so-hotels-and-resorts`.

| Brand | Slug | Parent | Status | Full | PVQL | Quality | Uniq | Role | Evidence |
|-------|------|--------|--------|------|------|---------|------|------|----------|
| Mama Shelter | `mama-shelter` | Accor | Active | true | pass | approve_for_baseline_freeze | pass | pass | fail |
| Mercure | `mercure` | Accor | Active | true | pass | approve_for_baseline_freeze | pass | pass | fail |
| ibis | `ibis` | Accor | Active | true | pass | approve_for_baseline_freeze | pass | pass | pass |
| Novotel | `novotel` | Accor | Active | true | pass | approve_for_baseline_freeze | pass | pass | fail |
| Pullman | `pullman` | Accor | Active | true | pass | approve_for_baseline_freeze | pass | pass | fail |
| Fairmont | `fairmont-hotels-and-resorts` | Accor | Active | true | pass | approve_for_baseline_freeze | pass | pass | fail |

## 5. Wave 13 value scenario cleanup summary

- Artifact: `brand-explorer-wave13-value-scenario-pattern-cleanup.json`
- Ready statement: **wave13_value_scenario_pattern_clean_visual_review_ready**
- Generated: —
- Scope: owner-value scenario cards for public six; no `owner-fit diligence`, no standalone Accor/Ennismore platform placeholders, no Property Fit / Support Across Lifecycle titles on Where This Brand Creates the Most Value.

## 6. Wave 13 geo / recent momentum cleanup summary

- Artifact: `brand-explorer-wave13-public-six-geo-momentum-cleanup.json`
- Ready statement: **wave13_public_six_geo_momentum_clean_ready_for_45_or_so_decision**
- Generated: —
- Scope: ≥3 geographic region cards; structured Recent Momentum (date + geography + source); no raw URLs in visible body; CALA-first openings where inventory exists.

## 7. Validation results

- Quality audit: brand-explorer-24-tab-section-quality-audit.json (ready_to_freeze_45_active_public_full_baseline)
- PVQL: brand-explorer-public-visibility-quality-lock.json (publicFull=45; overallPass=true)
- Image audit: brand-explorer-24-image-repetition-audit.json (crossBrand=0)
- OS: brand-explorer-v41-os-consolidation.json
- Recent Momentum / Openings Evidence Quality: pass=true

## 8. Evidence quality result

Gate: `npm run test:brand-explorer-recent-momentum-evidence-quality`
Mandatory wave pass: **true**
Wave 13 hard-fail gate (allows known cala_not_prioritized_first Sort Order drift): **true

Known Wave 13 soft notes:
- mama-shelter:cala_not_prioritized_first:firstRegion=International Reference
- mercure:cala_not_prioritized_first:firstRegion=International Reference
- novotel:cala_not_prioritized_first:firstRegion=International Reference
- pullman:cala_not_prioritized_first:firstRegion=International Reference
- fairmont-hotels-and-resorts:cala_not_prioritized_first:firstRegion=International Reference

- `dazzler-by-wyndham`: pass (fails=0; openings=cala_first_ok)
- `trademark-collection-by-wyndham`: pass (fails=0; openings=international_reference_labeled)
- `tapestry-collection-by-hilton`: pass (fails=0; openings=international_reference_labeled)
- `mama-shelter`: fail (fails=1; openings=cala_first_ok)
- `mercure`: fail (fails=1; openings=cala_first_ok)
- `ibis`: pass (fails=0; openings=cala_first_ok)
- `novotel`: fail (fails=1; openings=cala_first_ok)
- `pullman`: fail (fails=1; openings=cala_first_ok)
- `fairmont-hotels-and-resorts`: fail (fails=1; openings=cala_first_ok)

## 9. Image uniqueness / role-match result

| Metric | Count |
|--------|------:|
| Image uniqueness pass | 45 |
| Image role-match pass | 45 |
| Scenario repetition flagged | 2 |
| Cross-brand image reuse | 0 |

## 10. Held / excluded brands

These brands are **explicitly held or excluded** from the 45 Active/Live public-full freeze:

| Brand | Slug | Record ID | Brand Status | Category | Included |
|-------|------|-----------|--------------|----------|----------|
| Radisson Collection | `radisson-collection` | `rec2DDyPu38C6zDBC` | Draft | excluded | false |
| SO/ | `so-hotels-and-resorts` | `recTJdPlr4mDs9app` | Under Review | held | false |
| The House of Originals | `the-house-of-originals` | `rec7ZPOVYsldGmNfx` | Under Review | excluded | false |
| Morgans Originals | `morgans-originals` | `—` | — | excluded | false |

- **SO/** (`so-hotels-and-resorts`) — Under Review, held after founder review; no release fields; not in intentional restore registry.
- **The House of Originals** — excluded from Wave 13.
- **Morgans Originals** — not created / not modified.
- **Radisson Collection** — excluded unless separately promoted to Active/Live.

## 11. Protected fields

- Company Validated
- Company Validation Date
- Source Library status
- Registry approval/status
- Brand Status
- release fields
- public restore registry

Baseline freeze does **not** write any of these fields.

## 12. Regression rules

- Active/Live universe count must remain 45 unless freeze is explicitly revised
- Every Active/Live brand must remain public-full with shouldRenderFullProfile=true
- Every Active/Live brand must pass PVQL
- Every Active/Live brand must remain approve_for_baseline_freeze
- No blocker or remediation_required on Active/Live brands
- No cross-brand image reuse
- Image uniqueness and role-match must pass for Active/Live brands
- Value scenario images must remain distinct (scenarioDistinct ≥ 3)
- Value Creation Scenarios must not regress to owner-fit diligence / Accor-Ennismore platform placeholders
- Where This Brand Creates the Most Value must not use Property Fit / Support Across Lifecycle card titles
- Geographic footprint must keep ≥3 filled region cards (or accepted cleanly_unavailable)
- Recent Momentum cards must keep date, geography, structured source; no raw URLs in visible body
- raw_url_scan / forbidden_owner_facing_language / generic_copy_scan mechanical hits must stay at 0
- ADR / RevPAR / fee-stack / FDD / Item 19 / LOI must not appear in visible owner-facing copy
- Recent Momentum / Openings Evidence Quality must pass for mandatory wave brands (npm gate)
- Wave 13 public six evidence must have no hard failures (raw URL / missing date-source / thin body / wrong brand); known cala_not_prioritized_first Sort Order drift is snapshotted as a note until a separate Sort Order remediation
- Company Validated, Company Validation Date, Source Library, Registry, Brand Status must not change unexpectedly
- SO/ must remain Under Review and excluded while held
- House of Originals, Morgans Originals, and Radisson Collection must remain excluded unless separately promoted
- Stale 23/24/25/27/39-brand / PRIMARY_RELEASE lists must never replace the Active/Live universe SoT

Test: `npm run test:brand-explorer-45-active-public-full-baseline`

## 13. Rollback notes

- This freeze is report-only — no Airtable writes occurred.
- Protected 39 public-full freeze preserved at reports/brand-explorer-39-active-public-full-baseline.json
- Protected 27 / interim 27 / 24 / 25 freezes remain predecessor artifacts.
- To unfreeze: revise docs/reports and EXPECTED_ACTIVE_COUNT_45 after an explicit founder decision.
- Do not revert Brand Status / CV / Source / Registry to undo this freeze — those fields were never written.
- Future SO/ path: 45 → 46 only after separate cleanup, founder approval, status promotion, and public release.

## 14. Future SO/ path

45 → 46 only after separate SO/ cleanup, founder approval, Brand Status promotion, and public release. Do not freeze SO/ into Active/Live while Under Review.

### Future factory rules

- New Active/Live brands require a new baseline revision (count will leave 45).
- SO/ promotion is a separate Wave path — do not silently absorb into the 45 freeze.
- Do not patch baseline brands casually — use targeted cleanup + re-audit + re-freeze.
- Required gates: test:brand-explorer-45-active-public-full-baseline · test:brand-explorer-public-visibility-quality-lock --public-full-only · test:brand-explorer-recent-momentum-evidence-quality
- Prefer quiet sequential PVQL/quality audits when Airtable 429 risk is high (scripts/brand-explorer-quiet-sequential-pvql.mjs, scripts/brand-explorer-quiet-sequential-quality-audit.mjs).
- Operational cohorts (PRIMARY_RELEASE, restore lanes, factory preview) remain overlays, not universe SoT.

## Commands

```bash
npm run brand-explorer-45-active-public-full-baseline -- --dry-run
npm run test:brand-explorer-45-active-public-full-baseline
npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only
npm run test:brand-explorer-recent-momentum-evidence-quality
# Quiet sequential (avoid Airtable 429 thrash):
node scripts/brand-explorer-quiet-sequential-pvql.mjs
node scripts/brand-explorer-quiet-sequential-quality-audit.mjs
```

