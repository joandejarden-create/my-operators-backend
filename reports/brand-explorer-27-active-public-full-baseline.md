# Brand Explorer — Protected 27 Active/Live Public-Full Baseline

Version: `27-active-public-full-baseline-v2` · Generated: 2026-07-24T11:07:11.974Z
Baseline type: **active_live_public_full**
Freeze decision: **frozen_27_active_public_full_baseline** · frozen=true
Writes: Airtable=false · Presentation=false · Image=false · CV=false · Source=false · Registry=false · Brand Status=false

## 1. Executive summary

This freeze locks the **27** Brand Basics Active/Live Brand Explorer profiles that are public-full, PVQL-clean, quality `approve_for_baseline_freeze`, and evidence-quality clean for the new wave.

| Metric | Value |
|--------|-------|
| Active/Live count | 27 |
| Public-full | 27 |
| shouldRenderFullProfile | 27 |
| PVQL pass | 27 |
| approve_for_baseline_freeze | 27 |
| remediation_required | 0 |
| Evidence quality (wave) | true |
| Cross-brand image reuse | 0 |
| Company Validated = true | 0 |
| Excluded non-active | 1 |

## 2. Active universe source of truth

- **Name:** Brand Basics Brand Status Active/Live
- **Table:** Brand Setup - Brand Basics
- **Formula:** `OR({Brand Status}='Active', {Brand Status}='Live')`
- **Loader:** `lib/partner-intelligence/brand-explorer-active-universe.js`
- **Version:** active-universe-v1
- **Not the universe:** PRIMARY_RELEASE_SLUGS, prior_23_reconciliation, legacy_23_active_list, FACTORY_SUPPORTED_SLUGS_as_universe
- **Note:** PRIMARY_RELEASE_SLUGS is an operational overlay, not the Active/Live universe (27).

### Predecessor freezes (history, not current enforcement)

- 24 public-full: `brand-explorer-24-active-public-full-baseline.json` (24-active-public-full-baseline-v1)
- 25 Tapestry wave: `brand-explorer-25-active-public-full-baseline.json` (25-active-public-full-baseline-v1)
- Interim 27 Active/Live-only (pre public-full): `brand-explorer-27-active-universe-interim-baseline.json`

## 3. 27-brand baseline table

| Brand | Slug | Record ID | Status | Full | Display | PVQL | Quality | Evidence | OS | Gallery | Scenario | Property | Rows | CV |
|-------|------|-----------|--------|------|---------|------|---------|----------|----|---------|----------|----------|------|----|
| Ascend Hotel Collection | `ascend` | `reclkgOzvAcBheUSo` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 4 | 213 | false |
| Autograph Collection | `autograph-collection` | `recEJCTDj1zrsjPM6` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 3 | 108 | false |
| BW Premier Collection | `bw-premier-collection` | `recwXZ5gVZ8ZH8ekA` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 4 | 97 | false |
| BW Signature Collection | `bw-signature-collection` | `recdeh1NsP4gjrv80` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 4 | 97 | false |
| Comfort Inn & Suites | `comfort-inn-suites` | `recOzH5iAE1xEjyD0` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 5 | 213 | false |
| Country Inn & Suites by Choice | `country-inn-suites` | `recaayt9u7YYg8h7Y` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 9 | 3 | 3 | 215 | false |
| Curio Collection by Hilton | `curio-collection` | `receQkxgjlezsc1xg` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 5 | 211 | false |
| Dazzler by Wyndham | `dazzler-by-wyndham` | `rec5CNMM4ZUD7ZHlM` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | — | 6 | 3 | 3 | 104 | false |
| Design Hotels | `design-hotels` | `rec02zPClpWUTCyXM` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 3 | 130 | false |
| Everhome Suites | `everhome-suites` | `recqkkrsevi4r9ibj` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 4 | 212 | false |
| Handwritten Collection | `handwritten-collection` | `rec7hTXwMRC81EPqz` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 3 | 105 | false |
| Hotel Indigo | `hotel-indigo` | `recegXrqaPiSLGCIe` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 3 | 96 | false |
| Kimpton Hotels | `kimpton` | `recCKuXCmGvxHPfb3` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 5 | 203 | false |
| MGallery Collection | `mgallery-collection` | `recrWCD1LMqu864oU` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 3 | 90 | false |
| Preferred Hotels & Resorts | `preferred-hotels-and-resorts` | `recwl5JOYxlChuCAr` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 4 | 97 | false |
| Quality Inn | `quality-inn` | `recd8o4k1JddhkRWW` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 3 | 204 | false |
| Radisson Blu by Choice | `radisson-blu` | `recWPEvxBQxVVzSq3` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 6 | 222 | false |
| Radisson by Choice | `radisson` | `recywbx1YQSTCPqW1` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 4 | 207 | false |
| Radisson Individuals by Choice | `radisson-individuals-by-choice` | `recRyvM8OmLlDj9G7` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 3 | 213 | false |
| Radisson RED by Choice | `radisson-red` | `recmKqo7M7mLZgRqQ` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 4 | 208 | false |
| Small Luxury Hotels of the World | `small-luxury-hotels-of-the-world` | `recjjSnY2opb8P4DG` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 3 | 95 | false |
| Suburban Studios | `suburban-studios` | `reclcjg5Foa9Vs5TC` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 3 | 205 | false |
| Tapestry Collection by Hilton | `tapestry-collection-by-hilton` | `reccXxMHEh7NNRhIE` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | — | 6 | 3 | 3 | 125 | false |
| Trademark Collection by Wyndham | `trademark-collection-by-wyndham` | `recob7tgHRryRSbeO` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | pass | — | 6 | 3 | 3 | 104 | false |
| Tribute Portfolio | `tribute-portfolio` | `recCvV0PuZOi8c3hC` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 4 | 165 | false |
| Vignette Collection | `vignette-collection` | `recDwzv86TWnz2gGB` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 3 | 105 | false |
| WoodSpring Suites | `woodspring-suites` | `recsOd51NzRPYsMko` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | n_a_protected_prior | — | 6 | 3 | 3 | 211 | false |

## 4. New wave summary

| Brand | Slug | Wave | Status | Full | PVQL | Quality | Evidence | Openings region |
|-------|------|------|--------|------|------|---------|----------|-----------------|
| Tapestry Collection by Hilton | `tapestry-collection-by-hilton` | 25 | Active | true | pass | approve_for_baseline_freeze | pass | international_reference_labeled |
| Dazzler by Wyndham | `dazzler-by-wyndham` | 27 | Active | true | pass | approve_for_baseline_freeze | pass | cala_first_ok |
| Trademark Collection by Wyndham | `trademark-collection-by-wyndham` | 27 | Active | true | pass | approve_for_baseline_freeze | pass | international_reference_labeled |

## 5. Validation results

- Quality audit: brand-explorer-24-tab-section-quality-audit.json (ready_to_freeze_24_brand_baseline)
- PVQL: brand-explorer-public-visibility-quality-lock.json (publicFull=27)
- Image audit: brand-explorer-24-image-repetition-audit.json (crossBrand=0)
- OS: brand-explorer-v41-os-consolidation.json
- Recent Momentum / Openings Evidence Quality: pass=true

## 6. Recent Momentum / Openings Evidence Quality gate

Gate: `npm run test:brand-explorer-recent-momentum-evidence-quality`
Wave pass: **true**

- `dazzler-by-wyndham`: pass (fails=0; openings=cala_first_ok)
- `trademark-collection-by-wyndham`: pass (fails=0; openings=international_reference_labeled)
- `tapestry-collection-by-hilton`: pass (fails=0; openings=international_reference_labeled)

## 7. Excluded non-active brands

These brands are **explicitly excluded** because they are not Active/Live:

| Brand | Slug | Record ID | Brand Status | Included |
|-------|------|-----------|--------------|----------|
| Radisson Collection | `radisson-collection` | `rec2DDyPu38C6zDBC` | Draft | false |

**Radisson Collection** remains excluded because Brand Status is not Active/Live (Draft at freeze).

## 8. Protected fields

- Company Validated
- Company Validation Date
- Source Library status
- Registry approval/status
- Brand Status
- release fields
- public restore registry

Baseline freeze does **not** write any of these fields.

## 9. Regression rules

- Active/Live universe count must remain 27 unless freeze is explicitly revised
- Every Active/Live brand must remain public-full with shouldRenderFullProfile=true
- Every Active/Live brand must pass PVQL
- Every Active/Live brand must remain approve_for_baseline_freeze
- No blocker or remediation_required on Active/Live brands
- No cross-brand image reuse
- Value scenario images must remain distinct (scenarioDistinct ≥ 3)
- raw_url_scan / forbidden_owner_facing_language / generic_copy_scan mechanical hits must stay at 0
- Recent Momentum / Openings Evidence Quality must pass for Tapestry, Dazzler, Trademark
- Openings property URLs must match property-distinctive title tokens; CALA-first or International Reference labels required
- Company Validated, Company Validation Date, Source Library, Registry, Brand Status must not change unexpectedly
- Radisson Collection must remain excluded unless Brand Status promoted to Active/Live
- Stale 24/23-brand / PRIMARY_RELEASE lists must never replace the Active/Live universe SoT

Test: `npm run test:brand-explorer-27-active-public-full-baseline`

## 10. Rollback notes

- This freeze is report-only — no Airtable writes occurred.
- Interim Active/Live-only freeze preserved at reports/brand-explorer-27-active-universe-interim-baseline.json
- Historical 24/25 public-full freezes remain as predecessor artifacts.
- To unfreeze: revise docs/reports and EXPECTED_ACTIVE_COUNT_27 after an explicit founder decision.
- Do not revert Brand Status / CV / Source / Registry to undo this freeze — those fields were never written.

## 11. Future factory rules

- New Active/Live brands require a new baseline revision (count will leave 27).
- Do not patch baseline brands casually — use targeted cleanup + re-audit + re-freeze.
- Required gates: test:brand-explorer-27-active-public-full-baseline · test:brand-explorer-public-visibility-quality-lock --public-full-only · test:brand-explorer-recent-momentum-evidence-quality
- Promoting Radisson Collection requires Brand Status → Active/Live first, then Tab Factory + public-full + PVQL + evidence + quality freeze path.
- Operational cohorts (PRIMARY_RELEASE, restore lanes, factory preview) remain overlays, not universe SoT.

## Commands

```bash
npm run brand-explorer-27-active-public-full-baseline -- --dry-run
npm run test:brand-explorer-27-active-public-full-baseline
npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only
npm run test:brand-explorer-recent-momentum-evidence-quality
```

