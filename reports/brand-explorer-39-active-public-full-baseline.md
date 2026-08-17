# Brand Explorer — Protected 39 Active/Live Public-Full Baseline

Version: `39-active-public-full-baseline-v1` · Generated: 2026-07-25T23:34:59.619Z
Baseline type: **active_live_public_full**
Freeze decision: **frozen_39_active_public_full_baseline** · frozen=true
Writes: Airtable=false · Presentation=false · Image=false · CV=false · Source=false · Registry=false · Brand Status=false

## 1. Executive summary

This freeze locks the **39** Brand Basics Active/Live Brand Explorer profiles that are public-full, PVQL-clean, and quality `approve_for_baseline_freeze` after Wave 12 + final freeze-blocker cleanup.

| Metric | Value |
|--------|-------|
| Active/Live count | 39 |
| Public-full | 39 |
| shouldRenderFullProfile | 39 |
| PVQL pass | 39 |
| approve_for_baseline_freeze | 39 |
| remediation_required | 0 |
| Evidence quality (mandatory wave) | true |
| Image uniqueness pass | 39 |
| Image role-match pass | 39 |
| Cross-brand image reuse | 0 |
| Wave 12 included | 12/12 |
| Company Validated = true | 0 |
| Excluded non-active | 1 |

## 2. Active universe source of truth

- **Name:** Brand Basics Brand Status Active/Live
- **Table:** Brand Setup - Brand Basics
- **Formula:** `OR({Brand Status}='Active', {Brand Status}='Live')`
- **Loader:** `lib/partner-intelligence/brand-explorer-active-universe.js`
- **Version:** active-universe-v1
- **Not the universe:** PRIMARY_RELEASE_SLUGS, prior_23_reconciliation, legacy_23_active_list, FACTORY_SUPPORTED_SLUGS_as_universe, stale_27_brand_list_as_universe, stale_24_brand_list_as_universe
- **Note:** PRIMARY_RELEASE_SLUGS is an operational overlay, not the Active/Live universe (39).

### Predecessor freezes (history, not current enforcement)

- 24 public-full: `brand-explorer-24-active-public-full-baseline.json` (24-active-public-full-baseline-v1)
- 25 Tapestry wave: `brand-explorer-25-active-public-full-baseline.json` (25-active-public-full-baseline-v1)
- Interim 27 Active/Live-only: `brand-explorer-27-active-universe-interim-baseline.json`
- Protected 27 public-full (pre-Wave 12): `brand-explorer-27-active-public-full-baseline.json` (27-active-public-full-baseline-v2)

## 3. 39-brand baseline table

| Brand | Slug | Record ID | Status | Full | Display | PVQL | Quality | Uniq | Role | Evidence | OS | Gallery | Scenario | Property | Rows | CV |
|-------|------|-----------|--------|------|---------|------|---------|------|------|----------|----|---------|----------|----------|------|----|
| AC Hotels by Marriott | `ac-hotels-by-marriott` | `rec9aZp7GHtzUEg0c` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_wave12_covered_by_mandatory_gate | — | 6 | 3 | 3 | 107 | false |
| Ascend Hotel Collection | `ascend` | `reclkgOzvAcBheUSo` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 4 | 213 | false |
| Autograph Collection | `autograph-collection` | `recEJCTDj1zrsjPM6` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 112 | false |
| avid hotels | `avid-hotels` | `recoEarnE8T6sDjZq` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_wave12_covered_by_mandatory_gate | — | 6 | 3 | 3 | 106 | false |
| Bunkhouse Hotels | `bunkhouse-hotels` | `recGv268Wda31PlSZ` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_wave12_covered_by_mandatory_gate | — | 6 | 3 | 3 | 107 | false |
| BW Premier Collection | `bw-premier-collection` | `recwXZ5gVZ8ZH8ekA` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 4 | 101 | false |
| BW Signature Collection | `bw-signature-collection` | `recdeh1NsP4gjrv80` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 4 | 101 | false |
| Canopy by Hilton | `canopy-by-hilton` | `recsggfbKlJbjeRP9` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_wave12_covered_by_mandatory_gate | — | 6 | 3 | 3 | 107 | false |
| City Express by Marriott | `city-express-by-marriott` | `recucEzAS6724tOYA` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_wave12_covered_by_mandatory_gate | — | 6 | 3 | 3 | 107 | false |
| Comfort Inn & Suites | `comfort-inn-suites` | `recOzH5iAE1xEjyD0` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 5 | 217 | false |
| Country Inn & Suites by Choice | `country-inn-suites` | `recaayt9u7YYg8h7Y` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 9 | 3 | 3 | 219 | false |
| Courtyard by Marriott | `courtyard-by-marriott` | `rec6hye5H8zJmAGv3` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_wave12_covered_by_mandatory_gate | — | 6 | 3 | 3 | 106 | false |
| Curio Collection by Hilton | `curio-collection` | `receQkxgjlezsc1xg` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 5 | 215 | false |
| Dazzler by Wyndham | `dazzler-by-wyndham` | `rec5CNMM4ZUD7ZHlM` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | pass | — | 6 | 3 | 3 | 108 | false |
| Design Hotels | `design-hotels` | `rec02zPClpWUTCyXM` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 130 | false |
| Even Hotels | `even-hotels` | `recvvmiyReHhiKdoK` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_wave12_covered_by_mandatory_gate | — | 6 | 3 | 3 | 107 | false |
| Everhome Suites | `everhome-suites` | `recqkkrsevi4r9ibj` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 4 | 212 | false |
| Handwritten Collection | `handwritten-collection` | `rec7hTXwMRC81EPqz` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 109 | false |
| Holiday Inn Express | `holiday-inn-express` | `recmGmiIqDtAsm01f` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_wave12_covered_by_mandatory_gate | — | 6 | 3 | 3 | 107 | false |
| Hotel Indigo | `hotel-indigo` | `recegXrqaPiSLGCIe` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 100 | false |
| Kimpton Hotels | `kimpton` | `recCKuXCmGvxHPfb3` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 5 | 203 | false |
| MGallery Collection | `mgallery-collection` | `recrWCD1LMqu864oU` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 94 | false |
| Motto by Hilton | `motto-by-hilton` | `reclt44apoi8co0e6` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_wave12_covered_by_mandatory_gate | — | 6 | 3 | 3 | 107 | false |
| Moxy Hotels | `moxy-hotels` | `recahVIW4aCx0Ao84` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_wave12_covered_by_mandatory_gate | — | 6 | 3 | 3 | 106 | false |
| Preferred Hotels & Resorts | `preferred-hotels-and-resorts` | `recwl5JOYxlChuCAr` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 4 | 101 | false |
| Quality Inn | `quality-inn` | `recd8o4k1JddhkRWW` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 208 | false |
| Radisson Blu by Choice | `radisson-blu` | `recWPEvxBQxVVzSq3` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 6 | 222 | false |
| Radisson by Choice | `radisson` | `recywbx1YQSTCPqW1` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 4 | 207 | false |
| Radisson Individuals by Choice | `radisson-individuals-by-choice` | `recRyvM8OmLlDj9G7` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 213 | false |
| Radisson RED by Choice | `radisson-red` | `recmKqo7M7mLZgRqQ` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 4 | 212 | false |
| Small Luxury Hotels of the World | `small-luxury-hotels-of-the-world` | `recjjSnY2opb8P4DG` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 99 | false |
| Suburban Studios | `suburban-studios` | `reclcjg5Foa9Vs5TC` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 209 | false |
| Tapestry Collection by Hilton | `tapestry-collection-by-hilton` | `reccXxMHEh7NNRhIE` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | pass | — | 6 | 3 | 3 | 129 | false |
| Tempo by Hilton | `tempo-by-hilton` | `recqiHq3GHKMj8Meo` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_wave12_covered_by_mandatory_gate | — | 6 | 3 | 3 | 107 | false |
| Trademark Collection by Wyndham | `trademark-collection-by-wyndham` | `recob7tgHRryRSbeO` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | pass | — | 6 | 3 | 3 | 108 | false |
| Tribute Portfolio | `tribute-portfolio` | `recCvV0PuZOi8c3hC` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 4 | 165 | false |
| Vignette Collection | `vignette-collection` | `recDwzv86TWnz2gGB` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 109 | false |
| Voco Hotels | `voco-hotels` | `recwONQTqGU1jHCsM` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_wave12_covered_by_mandatory_gate | — | 6 | 3 | 3 | 107 | false |
| WoodSpring Suites | `woodspring-suites` | `recsOd51NzRPYsMko` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | pass | n_a_protected_prior | — | 6 | 3 | 3 | 211 | false |

## 4. Wave 12 summary

Wave 12 promoted **12** brands from the protected **27** baseline to the **39** Active/Live public-full universe.

| Brand | Slug | Parent | Status | Full | PVQL | Quality | Uniq | Role |
|-------|------|--------|--------|------|------|---------|------|------|
| EVEN Hotels | `even-hotels` | IHG | Active | true | pass | approve_for_baseline_freeze | pass | pass |
| voco | `voco-hotels` | IHG | Active | true | pass | approve_for_baseline_freeze | pass | pass |
| avid hotels | `avid-hotels` | IHG | Active | true | pass | approve_for_baseline_freeze | pass | pass |
| Holiday Inn Express | `holiday-inn-express` | IHG | Active | true | pass | approve_for_baseline_freeze | pass | pass |
| Courtyard by Marriott | `courtyard-by-marriott` | Marriott | Active | true | pass | approve_for_baseline_freeze | pass | pass |
| AC Hotels by Marriott | `ac-hotels-by-marriott` | Marriott | Active | true | pass | approve_for_baseline_freeze | pass | pass |
| City Express by Marriott | `city-express-by-marriott` | Marriott | Active | true | pass | approve_for_baseline_freeze | pass | pass |
| Moxy Hotels | `moxy-hotels` | Marriott | Active | true | pass | approve_for_baseline_freeze | pass | pass |
| Canopy by Hilton | `canopy-by-hilton` | Hilton | Active | true | pass | approve_for_baseline_freeze | pass | pass |
| Motto by Hilton | `motto-by-hilton` | Hilton | Active | true | pass | approve_for_baseline_freeze | pass | pass |
| Tempo by Hilton | `tempo-by-hilton` | Hilton | Active | true | pass | approve_for_baseline_freeze | pass | pass |
| Bunkhouse Hotels | `bunkhouse-hotels` | Hyatt | Active | true | pass | approve_for_baseline_freeze | pass | pass |

## 5. Final blocker cleanup summary

- Artifact: `brand-explorer-39-final-freeze-blocker-cleanup.json`
- Ready statement: **ready_to_freeze_39_active_public_full_baseline**
- Generated: —
- Scope: Scene7-aware image uniqueness (voco / avid / HIE / vignette) + ADR / fee-stack owner-facing scrub (including residual Active brands + valueOwners scenarios + BW Premier).
- Protected fields remained untouched during cleanup.

## 6. Validation results

- Quality audit: brand-explorer-24-tab-section-quality-audit.json (ready_to_freeze_39_active_public_full_baseline)
- PVQL: brand-explorer-public-visibility-quality-lock.json (publicFull=39; overallPass=true)
- Image audit: brand-explorer-24-image-repetition-audit.json (crossBrand=0)
- OS: brand-explorer-v41-os-consolidation.json
- Recent Momentum / Openings Evidence Quality: pass=true

## 7. Evidence quality result

Gate: `npm run test:brand-explorer-recent-momentum-evidence-quality`
Wave pass: **true**

- `dazzler-by-wyndham`: pass (fails=0; openings=cala_first_ok)
- `trademark-collection-by-wyndham`: pass (fails=0; openings=international_reference_labeled)
- `tapestry-collection-by-hilton`: pass (fails=0; openings=international_reference_labeled)

## 8. Image uniqueness / role-match result

| Metric | Count |
|--------|------:|
| Image uniqueness pass | 39 |
| Image role-match pass | 39 |
| Scenario repetition flagged | 2 |
| Cross-brand image reuse | 0 |

## 9. Excluded non-active brands

These brands are **explicitly excluded** because they are not Active/Live:

| Brand | Slug | Record ID | Brand Status | Included |
|-------|------|-----------|--------------|----------|
| Radisson Collection | `radisson-collection` | `rec2DDyPu38C6zDBC` | Draft | false |

**Radisson Collection** remains excluded because Brand Status is not Active/Live (Draft at freeze).

## 10. Protected fields

- Company Validated
- Company Validation Date
- Source Library status
- Registry approval/status
- Brand Status
- release fields
- public restore registry

Baseline freeze does **not** write any of these fields.

## 11. Regression rules

- Active/Live universe count must remain 39 unless freeze is explicitly revised
- Every Active/Live brand must remain public-full with shouldRenderFullProfile=true
- Every Active/Live brand must pass PVQL
- Every Active/Live brand must remain approve_for_baseline_freeze
- No blocker or remediation_required on Active/Live brands
- No cross-brand image reuse
- Image uniqueness and role-match must pass for Active/Live brands
- Value scenario images must remain distinct (scenarioDistinct ≥ 3)
- raw_url_scan / forbidden_owner_facing_language / generic_copy_scan mechanical hits must stay at 0
- ADR / RevPAR / fee-stack / FDD / Item 19 / LOI must not appear in visible owner-facing copy
- Recent Momentum / Openings Evidence Quality must pass for mandatory wave targets (Tapestry, Dazzler, Trademark)
- Openings property URLs must match property-distinctive title tokens; CALA-first or International Reference labels required
- Company Validated, Company Validation Date, Source Library, Registry, Brand Status must not change unexpectedly
- Radisson Collection must remain excluded unless Brand Status promoted to Active/Live
- Stale 23/24/25/27-brand / PRIMARY_RELEASE lists must never replace the Active/Live universe SoT

Test: `npm run test:brand-explorer-39-active-public-full-baseline`

## 12. Rollback notes

- This freeze is report-only — no Airtable writes occurred.
- Protected 27 public-full freeze preserved at reports/brand-explorer-27-active-public-full-baseline.json
- Interim Active/Live-only freeze preserved at reports/brand-explorer-27-active-universe-interim-baseline.json
- Historical 24/25 public-full freezes remain as predecessor artifacts.
- To unfreeze: revise docs/reports and EXPECTED_ACTIVE_COUNT_39 after an explicit founder decision.
- Do not revert Brand Status / CV / Source / Registry to undo this freeze — those fields were never written.

## 13. Future factory rules

- New Active/Live brands require a new baseline revision (count will leave 39).
- Do not patch baseline brands casually — use targeted cleanup + re-audit + re-freeze.
- Required gates: test:brand-explorer-39-active-public-full-baseline · test:brand-explorer-public-visibility-quality-lock --public-full-only · test:brand-explorer-recent-momentum-evidence-quality
- Prefer quiet sequential PVQL/quality audits when Airtable 429 risk is high (scripts/brand-explorer-quiet-sequential-pvql.mjs, scripts/brand-explorer-quiet-sequential-quality-audit.mjs).
- Promoting Radisson Collection requires Brand Status → Active/Live first, then Tab Factory + public-full + PVQL + evidence + quality freeze path.
- Operational cohorts (PRIMARY_RELEASE, restore lanes, factory preview) remain overlays, not universe SoT.

## Commands

```bash
npm run brand-explorer-39-active-public-full-baseline -- --dry-run
npm run test:brand-explorer-39-active-public-full-baseline
npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only
npm run test:brand-explorer-recent-momentum-evidence-quality
# Quiet sequential (avoid Airtable 429 thrash):
node scripts/brand-explorer-quiet-sequential-pvql.mjs
node scripts/brand-explorer-quiet-sequential-quality-audit.mjs
```

