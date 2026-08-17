# Brand Explorer — Protected 62 Active/Live Public-Full Baseline

Version: `62-active-public-full-baseline-v1` · Generated: 2026-08-08T08:06:24.548Z
Baseline type: **active_live_public_full**
Freeze decision: **`frozen_62_active_public_full_baseline_quality_clean_flex_held`** · frozen=true
Quality-clean revision: **quality-clean-v1** · status=`brand_explorer_62_active_public_full_quality_clean_frozen_ready_for_child_table_validation` · accepted minors=**none**
Predecessor freeze: `frozen_62_active_public_full_baseline_semantic_clean_flex_held`
Writes: Airtable=false · Presentation=false · Image=false · CV=false · Source=false · Registry=false · Brand Status=false

## 1. Executive summary

This **quality-clean** freeze locks the **62** Brand Basics Active/Live Brand Explorer profiles that are public-full, PVQL-clean, quality `approve_for_baseline_freeze` (no accepted minors), and AI-Assisted footnote-complete. MGallery quality minor is resolved. Four Points Flex by Sheraton remains **held** (Under Review). House / Morgans / Radisson Collection remain excluded.

| Metric | Value |
|--------|-------|
| Active/Live count | 62 |
| Public-full | 62 |
| shouldRenderFullProfile | 62 |
| PVQL pass | 62 |
| approve_for_baseline_freeze | 62 |
| remediation_required | 0 |
| AI-Assisted footnote visible | 62 |
| Footnote complete (LR+SB+Region) | 62 |
| Evidence quality (mandatory wave) | true |
| Image uniqueness pass | 62 |
| Image role-match pass | 62 |
| Cross-brand image reuse | 0 |
| Wave 14 Marriott eight included | 8/8 |
| Company Validated = true | 0 |
| Held / excluded probes | 4 |

## 2. Active universe source of truth

- **Name:** Brand Basics Brand Status Active/Live
- **Table:** Brand Setup - Brand Basics
- **Formula:** `OR({Brand Status}='Active', {Brand Status}='Live')`
- **Loader:** `lib/partner-intelligence/brand-explorer-active-universe.js`
- **Version:** active-universe-v1
- **Not the universe:** PRIMARY_RELEASE_SLUGS, prior_23_reconciliation, legacy_23_active_list, FACTORY_SUPPORTED_SLUGS_as_universe, stale_46_brand_list_as_universe, stale_45_brand_list_as_universe, stale_39_brand_list_as_universe, stale_27_brand_list_as_universe, stale_24_brand_list_as_universe
- **Note:** PRIMARY_RELEASE_SLUGS is an operational overlay, not the Active/Live universe (54).

### Predecessor freezes (history, not current enforcement)

- 24 / 25 / interim 27 / protected 27 / protected 39 / protected 45 / **protected 46** — preserved as predecessor artifacts.
- Protected 46: `brand-explorer-46-active-public-full-baseline.json` (46-active-public-full-baseline-v1)

## 3. 54-brand baseline table

| Brand | Slug | Record ID | Status | Full | Display | PVQL | Quality | Footnote | Last Reviewed | Source Basis | Region | Uniq | Role | Evidence | Gallery | Scenario | Property | Rows | CV |
|-------|------|-----------|--------|------|---------|------|---------|----------|---------------|--------------|--------|------|------|----------|---------|----------|----------|------|----|
| AC Hotels by Marriott | `ac-hotels-by-marriott` | `rec9aZp7GHtzUEg0c` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 24, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 107 | false |
| Aloft Hotels | `aloft-hotels` | `recJ1GZQpttX7qHgw` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 28, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 102 | false |
| Ascend Hotel Collection | `ascend` | `reclkgOzvAcBheUSo` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 7, 2026 | Company Materials | CALA-specific | pass | pass | n_a_protected_prior | 6 | 3 | 4 | 213 | false |
| Autograph Collection | `autograph-collection` | `recEJCTDj1zrsjPM6` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 23, 2026 | Company Materials | International Reference | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 112 | false |
| avid hotels | `avid-hotels` | `recoEarnE8T6sDjZq` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 24, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 106 | false |
| Bunkhouse Hotels | `bunkhouse-hotels` | `recGv268Wda31PlSZ` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 24, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 107 | false |
| BW Premier Collection | `bw-premier-collection` | `recwXZ5gVZ8ZH8ekA` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 23, 2026 | Company Materials | International Reference | pass | pass | n_a_protected_prior | 6 | 3 | 4 | 101 | false |
| BW Signature Collection | `bw-signature-collection` | `recdeh1NsP4gjrv80` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 23, 2026 | Company Materials | International Reference | pass | pass | n_a_protected_prior | 6 | 3 | 4 | 101 | false |
| Canopy by Hilton | `canopy-by-hilton` | `recsggfbKlJbjeRP9` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 24, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 107 | false |
| City Express by Marriott | `city-express-by-marriott` | `recucEzAS6724tOYA` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 24, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 107 | false |
| Comfort Inn & Suites | `comfort-inn-suites` | `recOzH5iAE1xEjyD0` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 6, 2026 | Company Materials | CALA-specific | pass | pass | n_a_protected_prior | 6 | 3 | 5 | 217 | false |
| Country Inn & Suites by Choice | `country-inn-suites` | `recaayt9u7YYg8h7Y` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 6, 2026 | Company Materials | CALA-specific | pass | pass | n_a_protected_prior | 9 | 3 | 3 | 219 | false |
| Courtyard by Marriott | `courtyard-by-marriott` | `rec6hye5H8zJmAGv3` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 24, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 106 | false |
| Curio Collection by Hilton | `curio-collection` | `receQkxgjlezsc1xg` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 6, 2026 | Company Materials | International Reference | pass | pass | n_a_protected_prior | 6 | 3 | 5 | 215 | false |
| Dazzler by Wyndham | `dazzler-by-wyndham` | `rec5CNMM4ZUD7ZHlM` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 23, 2026 | Company Materials + Public Sources | CALA-specific | pass | pass | pass | 6 | 3 | 3 | 108 | false |
| Design Hotels | `design-hotels` | `rec02zPClpWUTCyXM` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 23, 2026 | Company Materials | International Reference | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 130 | false |
| DoubleTree by Hilton | `doubletree-by-hilton` | `rechVYWQ5ikRnr99B` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Aug 4, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | pass | 6 | 3 | 3 | 100 | false |
| Even Hotels | `even-hotels` | `recvvmiyReHhiKdoK` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 24, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 107 | false |
| Everhome Suites | `everhome-suites` | `recqkkrsevi4r9ibj` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 6, 2026 | Company Materials | CALA-specific | pass | pass | n_a_protected_prior | 6 | 3 | 4 | 212 | false |
| Fairmont | `fairmont-hotels-and-resorts` | `recJhPaDVU3YUDQUt` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 27, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 107 | false |
| Hampton by Hilton | `hampton-by-hilton` | `rectRvOWQPaL6FkzZ` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Aug 4, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | pass | 6 | 3 | 3 | 100 | false |
| Handwritten Collection | `handwritten-collection` | `rec7hTXwMRC81EPqz` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 23, 2026 | Company Materials | International Reference | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 109 | false |
| Hilton Garden Inn | `hilton-garden-inn` | `recrvdAjRlXxPvPPF` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Aug 4, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | pass | 6 | 3 | 3 | 100 | false |
| Hilton Hotels & Resorts | `hilton-hotels-and-resorts` | `recWubG3rhiS1BaWi` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Aug 4, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | pass | 6 | 3 | 3 | 101 | false |
| Holiday Inn Express | `holiday-inn-express` | `recmGmiIqDtAsm01f` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 24, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 107 | false |
| Home2 Suites by Hilton | `home2-suites-by-hilton` | `reccZ4zV6wMav7a2i` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Aug 4, 2026 | Company Materials + Public Sources | International Reference | pass | pass | pass | 6 | 3 | 3 | 100 | false |
| Homewood Suites by Hilton | `homewood-suites-by-hilton` | `recZjYI4nYflGHFNR` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Aug 4, 2026 | Company Materials + Public Sources | International Reference | pass | pass | pass | 6 | 3 | 3 | 100 | false |
| Hotel Indigo | `hotel-indigo` | `recegXrqaPiSLGCIe` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 23, 2026 | Company Materials | International Reference | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 100 | false |
| ibis | `ibis` | `reclFXbpZ5XzLWbGP` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 27, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 107 | false |
| Kimpton Hotels | `kimpton` | `recCKuXCmGvxHPfb3` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jun 12, 2026 | Company Materials | International Reference | pass | pass | n_a_protected_prior | 6 | 3 | 5 | 203 | false |
| Mama Shelter | `mama-shelter` | `recXCZCK05XXYX7Q8` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 27, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 106 | false |
| Marriott Hotels | `marriott-hotels` | `recn59UtkyyoYwzSz` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 28, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 102 | false |
| Mercure | `mercure` | `recevrLJ3m6rIug3S` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 27, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 107 | false |
| MGallery Collection | `mgallery-collection` | `recrWCD1LMqu864oU` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 23, 2026 | Company Materials | International Reference | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 97 | false |
| Motto by Hilton | `motto-by-hilton` | `reclt44apoi8co0e6` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 24, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 107 | false |
| Moxy Hotels | `moxy-hotels` | `recahVIW4aCx0Ao84` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 24, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 106 | false |
| Novotel | `novotel` | `recQE2lSSSSyuUrMQ` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 27, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 107 | false |
| Preferred Hotels & Resorts | `preferred-hotels-and-resorts` | `recwl5JOYxlChuCAr` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 23, 2026 | Company Materials | International Reference | pass | pass | n_a_protected_prior | 6 | 3 | 4 | 101 | false |
| Pullman | `pullman` | `recFW9kfqKfOjv7Z1` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 27, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 107 | false |
| Quality Inn | `quality-inn` | `recd8o4k1JddhkRWW` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 6, 2026 | Company Materials | CALA-specific | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 208 | false |
| Radisson Blu by Choice | `radisson-blu` | `recWPEvxBQxVVzSq3` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 6, 2026 | Company Materials | CALA-specific | pass | pass | n_a_protected_prior | 6 | 3 | 6 | 222 | false |
| Radisson by Choice | `radisson` | `recywbx1YQSTCPqW1` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 6, 2026 | Company Materials | CALA-specific | pass | pass | n_a_protected_prior | 6 | 3 | 4 | 207 | false |
| Radisson Individuals by Choice | `radisson-individuals-by-choice` | `recRyvM8OmLlDj9G7` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 6, 2026 | Company Materials | CALA-specific | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 213 | false |
| Radisson RED by Choice | `radisson-red` | `recmKqo7M7mLZgRqQ` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 6, 2026 | Company Materials | CALA-specific | pass | pass | n_a_protected_prior | 6 | 3 | 4 | 212 | false |
| Residence Inn by Marriott | `residence-inn-by-marriott` | `rec9Ufbpa0GxJGzt8` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 28, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 102 | false |
| Sheraton | `sheraton` | `recg8HjT5Bky7NXeV` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 28, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 102 | false |
| Small Luxury Hotels of the World | `small-luxury-hotels-of-the-world` | `recjjSnY2opb8P4DG` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 23, 2026 | Company Materials | International Reference | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 99 | false |
| SO/ | `so-hotels-and-resorts` | `recTJdPlr4mDs9app` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 28, 2026 | Company Materials + Public Sources | International Reference | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 107 | false |
| Spark by Hilton | `spark-by-hilton` | `recfv66er4Ch2vJDO` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Aug 4, 2026 | Company Materials + Public Sources | International Reference | pass | pass | pass | 6 | 3 | 3 | 100 | false |
| SpringHill Suites by Marriott | `springhill-suites-by-marriott` | `recBzdGfkMUN9fYsv` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 28, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 101 | false |
| StudioRes | `studiores` | `recDM0LAD8jVRA2x3` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 28, 2026 | Company Materials + Public Sources | International Reference | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 101 | false |
| Suburban Studios | `suburban-studios` | `reclcjg5Foa9Vs5TC` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 23, 2026 | Company Materials | International Reference | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 209 | false |
| Tapestry Collection by Hilton | `tapestry-collection-by-hilton` | `reccXxMHEh7NNRhIE` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 23, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | pass | 6 | 3 | 3 | 129 | false |
| Tempo by Hilton | `tempo-by-hilton` | `recqiHq3GHKMj8Meo` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 24, 2026 | Company Materials + Public Sources | International Reference | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 107 | false |
| TownePlace Suites by Marriott | `towneplace-suites-by-marriott` | `recUPiPDivkhNUogr` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 28, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 101 | false |
| Trademark Collection by Wyndham | `trademark-collection-by-wyndham` | `recob7tgHRryRSbeO` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 23, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | pass | 6 | 3 | 3 | 108 | false |
| Tribute Portfolio | `tribute-portfolio` | `recCvV0PuZOi8c3hC` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 9, 2026 | Company Materials | CALA-specific | pass | pass | n_a_protected_prior | 6 | 3 | 4 | 165 | false |
| Tru by Hilton | `tru-by-hilton` | `recJLiMTv4W8VgO9L` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Aug 4, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | pass | 6 | 3 | 3 | 100 | false |
| Vignette Collection | `vignette-collection` | `recDwzv86TWnz2gGB` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 23, 2026 | Company Materials | International Reference | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 109 | false |
| Voco Hotels | `voco-hotels` | `recwONQTqGU1jHCsM` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 24, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 107 | false |
| Westin | `westin` | `recIPuBC50fv13zRR` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 28, 2026 | Company Materials + Public Sources | CALA-informed | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 102 | false |
| WoodSpring Suites | `woodspring-suites` | `recsOd51NzRPYsMko` | Active | true | active_profile_ready | pass | approve_for_baseline_freeze | true | Jul 23, 2026 | Company Materials | International Reference | pass | pass | n_a_protected_prior | 6 | 3 | 3 | 211 | false |

## 4. Wave 14 Marriott eight release summary

Eight of the nine Wave 14 Marriott International brands were promoted via founder acceptance → Brand Status Active → public release → semantic cleanup. **Four Points Flex by Sheraton** (`four-points-flex-by-sheraton`) remains **held** (Under Review) and is NOT part of the **54** Active/Live public-full universe.
- Public eight slugs: `hilton-hotels-and-resorts`, `homewood-suites-by-hilton`, `home2-suites-by-hilton`, `tru-by-hilton`, `doubletree-by-hilton`, `hampton-by-hilton`, `hilton-garden-inn`, `spark-by-hilton`
- Held slug: `four-points-flex-by-sheraton` (excluded=true)
- Expected partial active count: 62
- Value scenario visual remediation: `brand-explorer-wave15-medium-cleanup.json`
- Dated momentum cleanup: `brand-explorer-wave15-public-release.json`
- Founder visual/semantic remediation: `brand-explorer-wave15-status-promotion.json`
- Public/Active semantic blocker cleanup: `brand-explorer-wave15-medium-cleanup.json`

## 5. AI-Assisted Profile footnote standardization summary

- Artifact: `brand-explorer-ai-assisted-footnote-standardization.json`
- Ready state: **ai_assisted_profile_footnote_standardized_globally**
- Approach: code/rendering enricher (`applyBrandExplorerAiAssistedFootnote`) — **0 Airtable writes**
- Gate: `ai_assisted_profile_footnote_visible` (PVQL + factory)
- Footnote visible count: **62** / complete: **62**

## 6. Validation results

- Quality audit: brand-explorer-24-tab-section-quality-audit.json (ready_to_freeze_45_active_public_full_baseline)
- PVQL: brand-explorer-public-visibility-quality-lock.json (publicFull=62; overallPass=true)
- Image audit: brand-explorer-24-image-repetition-audit.json (crossBrand=0)
- OS: brand-explorer-v41-os-consolidation.json
- Recent Momentum / Openings Evidence Quality: pass=true

## 7. PVQL result

Public-full PVQL must be **62/62** lockPass, including `ai_assisted_profile_footnote_visible`.
Snapshot: publicFull=62 · overallPass=true

## 8. 24-tab quality result

All Active/Live brands must be `approve_for_baseline_freeze` with blockerCount=0 (accepted minors: **none** — quality-clean).
Freeze recommendation count: **62** · remediation: **0**

## 9. Evidence quality result

Gate: `npm run test:brand-explorer-recent-momentum-evidence-quality`
Mandatory wave pass: **true**
Wave 14 hard-fail gate (allows known cala_not_prioritized_first Sort Order drift): **true**

## 10. Image uniqueness / role-match result

| Metric | Count |
|--------|------:|
| Image uniqueness pass | 62 |
| Image role-match pass | 62 |
| Scenario repetition flagged | 2 |
| Cross-brand image reuse | 0 |

## 11. Value scenario / semantic cleanup result

- Value scenario visual remediation: `brand-explorer-wave15-medium-cleanup.json` · ready: **wave15_medium_cleanup_applied_ready_for_62_validation**
- Founder visual/semantic remediation: `brand-explorer-wave15-status-promotion.json` · ready: **wave15_status_promotion_applied_ready_for_public_release**
- Public/Active semantic blocker cleanup: `brand-explorer-wave15-medium-cleanup.json` · ready: **wave15_medium_cleanup_applied_ready_for_62_validation**
- No `owner-fit diligence`; no standalone platform placeholders; no Property Fit / Support Across Lifecycle titles.

## 12. Geographic footprint / Recent Momentum pattern result

- Dated momentum cleanup: `brand-explorer-wave15-public-release.json` · ready: **wave15_eight_brand_release_complete_ready_for_62_freeze_or_post_release_cleanup**
- ≥3 geographic region cards (or accepted cleanly_unavailable); structured Recent Momentum; no raw URLs in visible body.

## 13. Held / excluded brands

These brands are **explicitly excluded** from the 62 Active/Live public-full freeze:

| Brand | Slug | Record ID | Brand Status | Category | Included |
|-------|------|-----------|--------------|----------|----------|
| Radisson Collection | `radisson-collection` | `rec2DDyPu38C6zDBC` | Draft | excluded | false |
| The House of Originals | `the-house-of-originals` | `rec7ZPOVYsldGmNfx` | Under Review | excluded | false |
| Four Points Flex by Sheraton | `four-points-flex-by-sheraton` | `recgaMzDn2GKkpUsi` | Under Review | held | false |
| Morgans Originals | `morgans-originals` | `—` | — | excluded | false |

- **The House of Originals** — excluded from Wave 13.
- **Morgans Originals** — not created / not modified.
- **Radisson Collection** — excluded unless separately promoted to Active/Live.
- **Four Points Flex by Sheraton** — held (Under Review) after the Wave 14 partial release.

## 14. Protected fields

- Company Validated
- Company Validation Date
- Source Library status
- Registry approval/status
- Brand Status
- release fields
- public restore registry

Baseline freeze does **not** write any of these fields.

## 15. Regression rules

- Active/Live universe count must remain 54 unless freeze is explicitly revised
- Every Active/Live brand must remain public-full with shouldRenderFullProfile=true
- Every Active/Live brand must pass PVQL
- Every Active/Live brand must remain approve_for_baseline_freeze
- No blocker or remediation_required on Active/Live brands
- No cross-brand image reuse
- Image uniqueness and role-match must pass for Active/Live brands
- Value scenario images must remain distinct (scenarioDistinct ≥ 3)
- Value Creation Scenarios must not regress to owner-fit diligence / generic platform placeholders
- Where This Brand Creates the Most Value must not use Property Fit / Support Across Lifecycle card titles
- Geographic footprint must keep ≥3 filled region cards (or accepted cleanly_unavailable)
- Recent Momentum cards must keep date, geography, structured source; no raw URLs in visible body
- raw_url_scan / forbidden_owner_facing_language / generic_copy_scan mechanical hits must stay at 0
- ADR / RevPAR / fee-stack / FDD / Item 19 / LOI must not appear in visible owner-facing copy
- Recent Momentum / Openings Evidence Quality must pass for mandatory wave brands (npm gate)
- Wave 14 public eight evidence must have no hard failures (raw URL / missing date-source / thin body / wrong brand); known cala_not_prioritized_first Sort Order drift is snapshotted as a note until a separate Sort Order remediation
- Company Validated, Company Validation Date, Source Library, Registry, Brand Status must not change unexpectedly
- Four Points Flex by Sheraton must remain held (Under Review) — not Active/Live public-full
- AI-Assisted Profile footnote must render for every Active/Live brand (enriched path) with Last Reviewed, Source Basis, and Region
- Company Validated / Brand Verified wording must not appear unless Company Validated is true
- CALA-specific must not appear without source-supported CALA basis
- House of Originals, Morgans Originals, Radisson Collection, and Four Points Flex by Sheraton must remain excluded unless separately promoted
- Stale 23/24/25/27/39/45/46-brand / PRIMARY_RELEASE lists must never replace the Active/Live universe SoT

Test: `npm run test:brand-explorer-62-active-public-full-baseline`

## 16. Rollback notes

- This freeze is report-only — no Airtable writes occurred.
- Protected 46 public-full freeze preserved at reports/brand-explorer-46-active-public-full-baseline.json
- Protected 45 / 39 / 27 / interim 27 / 24 / 25 freezes remain predecessor artifacts.
- To unfreeze: revise docs/reports and EXPECTED_ACTIVE_COUNT_62 after an explicit founder decision.
- Do not revert Brand Status / CV / Source / Registry to undo this freeze — those fields were never written.
- AI-Assisted footnote is code/rendering — rollback is a code revert of brand-explorer-ai-assisted-footnote.js wiring, not Airtable.

## 17. Future Wave 15 starting conditions

Start next factory work from frozen_62_active_public_full_baseline_quality_clean_flex_held (quality-clean; no accepted minors). Keep House of Originals / Morgans Originals / Radisson Collection / Four Points Flex by Sheraton excluded unless separate promotion. Preserve AI-Assisted footnote always-on gate. Do not use 46/45/39/27/54-brand lists as universe SoT. Child Brand Setup table validation is a separate read-only program.

### Future factory rules

- New Active/Live brands require a new baseline revision (count will leave 54).
- Wave 15 factory work starts from this 54 freeze — do not silently absorb new Active brands.
- Do not patch baseline brands casually — use targeted cleanup + re-audit + re-freeze.
- Required gates: test:brand-explorer-62-active-public-full-baseline · test:brand-explorer-public-visibility-quality-lock --public-full-only · test:brand-explorer-recent-momentum-evidence-quality · brand-explorer-ai-assisted-footnote-standardization --audit
- Prefer quiet sequential PVQL/quality audits when Airtable 429 risk is high (scripts/brand-explorer-quiet-sequential-pvql.mjs, scripts/brand-explorer-quiet-sequential-quality-audit.mjs).
- Operational cohorts (PRIMARY_RELEASE, restore lanes, factory preview) remain overlays, not universe SoT.

## Commands

```bash
npm run brand-explorer-62-active-public-full-baseline -- --dry-run
npm run test:brand-explorer-62-active-public-full-baseline
npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only
npm run test:brand-explorer-recent-momentum-evidence-quality
npm run brand-explorer-ai-assisted-footnote-standardization -- --audit
# Quiet sequential (avoid Airtable 429 thrash):
node scripts/brand-explorer-quiet-sequential-pvql.mjs
node scripts/brand-explorer-quiet-sequential-quality-audit.mjs
```

