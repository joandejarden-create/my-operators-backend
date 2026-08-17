# Brand Explorer — Protected 24 Active/Live Public-Full Baseline

Version: `24-active-public-full-baseline-v1` · Generated: 2026-07-23T18:52:22.438Z
Freeze decision: **frozen_24_active_public_full_baseline** · frozen=true
Writes: Airtable=false · Presentation=false · Image=false · CV=false · Source=false · Registry=false · Brand Status=false

## 1. Executive summary

This freeze locks the **24** Brand Basics Active/Live Brand Explorer profiles that are public-full, PVQL-clean, and `approve_for_baseline_freeze`.

| Metric | Value |
|--------|-------|
| Active/Live count | 24 |
| Public-full | 24 |
| shouldRenderFullProfile | 24 |
| PVQL pass | 24 |
| approve_for_baseline_freeze | 24 |
| remediation_required | 0 |
| Cross-brand image reuse | 0 |
| Company Validated = true | 0 |
| Excluded non-active | 2 |

## 2. Active universe source of truth

- **Name:** Brand Basics Brand Status Active/Live
- **Table:** Brand Setup - Brand Basics
- **Formula:** `OR({Brand Status}='Active', {Brand Status}='Live')`
- **Loader:** `lib/partner-intelligence/brand-explorer-active-universe.js`
- **Version:** active-universe-v1
- **Not the universe:** PRIMARY_RELEASE_SLUGS, prior_23_reconciliation, legacy_23_active_list, FACTORY_SUPPORTED_SLUGS_as_universe
- **Note:** PRIMARY_RELEASE_SLUGS is an operational overlay (7), not the Active/Live universe (24).

## 3. 24-brand baseline table

| Brand | Slug | Record ID | Status | Full | Display | PVQL | Quality | OS | Gallery | Scenario | Property | Rows | CV |
|-------|------|-----------|--------|------|---------|------|---------|----|---------|----------|----------|------|----|
| Ascend Hotel Collection | `ascend` | `reclkgOzvAcBheUSo` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 4 | 213 | false |
| Autograph Collection | `autograph-collection` | `recEJCTDj1zrsjPM6` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 3 | 108 | false |
| BW Premier Collection | `bw-premier-collection` | `recwXZ5gVZ8ZH8ekA` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 4 | 97 | false |
| BW Signature Collection | `bw-signature-collection` | `recdeh1NsP4gjrv80` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 4 | 97 | false |
| Comfort Inn & Suites | `comfort-inn-suites` | `recOzH5iAE1xEjyD0` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 5 | 213 | false |
| Country Inn & Suites by Choice | `country-inn-suites` | `recaayt9u7YYg8h7Y` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 9 | 3 | 3 | 215 | false |
| Curio Collection by Hilton | `curio-collection` | `receQkxgjlezsc1xg` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 5 | 211 | false |
| Design Hotels | `design-hotels` | `rec02zPClpWUTCyXM` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 3 | 130 | false |
| Everhome Suites | `everhome-suites` | `recqkkrsevi4r9ibj` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 4 | 212 | false |
| Handwritten Collection | `handwritten-collection` | `rec7hTXwMRC81EPqz` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 3 | 105 | false |
| Hotel Indigo | `hotel-indigo` | `recegXrqaPiSLGCIe` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 3 | 96 | false |
| Kimpton Hotels | `kimpton` | `recCKuXCmGvxHPfb3` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 5 | 203 | false |
| MGallery Collection | `mgallery-collection` | `recrWCD1LMqu864oU` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 3 | 90 | false |
| Preferred Hotels & Resorts | `preferred-hotels-and-resorts` | `recwl5JOYxlChuCAr` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 4 | 97 | false |
| Quality Inn | `quality-inn` | `recd8o4k1JddhkRWW` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 3 | 204 | false |
| Radisson Blu by Choice | `radisson-blu` | `recWPEvxBQxVVzSq3` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 6 | 222 | false |
| Radisson by Choice | `radisson` | `recywbx1YQSTCPqW1` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 4 | 207 | false |
| Radisson Individuals by Choice | `radisson-individuals-by-choice` | `recRyvM8OmLlDj9G7` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 3 | 213 | false |
| Radisson RED by Choice | `radisson-red` | `recmKqo7M7mLZgRqQ` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 4 | 208 | false |
| Small Luxury Hotels of the World | `small-luxury-hotels-of-the-world` | `recjjSnY2opb8P4DG` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 3 | 95 | false |
| Suburban Studios | `suburban-studios` | `reclcjg5Foa9Vs5TC` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 3 | 205 | false |
| Tribute Portfolio | `tribute-portfolio` | `recCvV0PuZOi8c3hC` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 4 | 165 | false |
| Vignette Collection | `vignette-collection` | `recDwzv86TWnz2gGB` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 3 | 105 | false |
| WoodSpring Suites | `woodspring-suites` | `recsOd51NzRPYsMko` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | — | 6 | 3 | 3 | 211 | false |

## 4. Validation results

- Quality audit: brand-explorer-24-tab-section-quality-audit.json (ready_to_freeze_24_brand_baseline)
- PVQL: brand-explorer-public-visibility-quality-lock.json (publicFull=24)
- Image audit: brand-explorer-24-image-repetition-audit.json (crossBrand=0)
- OS: brand-explorer-v41-os-consolidation.json

## 5. Excluded non-active brands

These brands are **explicitly excluded** because they are not Active/Live:

| Brand | Slug | Record ID | Brand Status | Included |
|-------|------|-----------|--------------|----------|
| Radisson Collection | `radisson-collection` | `rec2DDyPu38C6zDBC` | Draft | false |
| Tapestry Collection by Hilton | `tapestry-collection-by-hilton` | `reccXxMHEh7NNRhIE` | Under Review | false |

Reason: Not Active/Live — Brand Status is not Active or Live

## 6. Protected fields

- Company Validated
- Company Validation Date
- Source Library status
- Registry approval/status
- Brand Status
- release fields
- public restore registry

Baseline freeze does **not** write any of these fields.

## 7. Regression rules

- Active/Live universe count must remain 24 unless freeze is explicitly revised
- Every Active/Live brand must remain public-full with shouldRenderFullProfile=true
- Every Active/Live brand must pass PVQL
- Every Active/Live brand must remain approve_for_baseline_freeze
- No blocker or remediation_required on Active/Live brands
- No cross-brand image reuse
- Value scenario images must remain distinct (scenarioDistinct ≥ 3)
- raw_url_scan / forbidden_owner_facing_language / generic_copy_scan mechanical hits must stay at 0
- Company Validated, Company Validation Date, Source Library, Registry, Brand Status must not change unexpectedly
- Radisson Collection and Tapestry must remain excluded unless Brand Status promoted to Active/Live
- Stale 23-brand / PRIMARY_RELEASE lists must never replace the Active/Live universe SoT

Test: `npm run test:brand-explorer-24-active-public-full-baseline`

## 8. Rollback notes

- This freeze is report-only — no Airtable writes occurred.
- To unfreeze: revise docs/reports and EXPECTED_ACTIVE_COUNT contract after an explicit founder decision.
- Do not revert Brand Status / CV / Source / Registry to 'undo' this freeze — those fields were never written.

## 9. Future-work rules

- New Active/Live brands require a new baseline revision (count will leave 24).
- Do not patch baseline brands casually — use targeted cleanup + re-audit + re-freeze.
- Promoting Radisson Collection or Tapestry requires Brand Status → Active/Live first, then full public-full + PVQL + quality freeze path.
- Operational cohorts (PRIMARY_RELEASE, restore lanes) remain overlays, not universe SoT.

## Commands

```bash
npm run brand-explorer-24-active-public-full-baseline -- --dry-run
npm run test:brand-explorer-24-active-public-full-baseline
```

**Usage rules (binding):** `docs/data-intelligence/brand-explorer-protected-baseline-rules.md`