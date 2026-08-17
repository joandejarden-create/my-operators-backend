# Brand Explorer Active Universe — Source of Truth

Version: `active-universe-source-of-truth-v1` · Generated: 2026-08-09T21:12:55.241Z
Read-only: **true** · Airtable writes: **none** · Company Validated untouched: **true**

## Active source of truth

| Field | Value |
| --- | --- |
| Name | Brand Basics Brand Status Active/Live |
| Table | Brand Setup - Brand Basics |
| API | GET /api/brand-library/brands (Active only) + GET /api/brand-explorer/brands |
| File | `lib/brand-status-active.js` |
| Filter | `OR({Brand Status}='Active', {Brand Status}='Live')` |
| Total count | **62** |
| Drives user-facing list | true |

This is the filter used by Brand Library card list and Brand Explorer brands list. Code constants (PRIMARY_RELEASE_SLUGS, LEGACY_SEED, etc.) are operational cohorts — not the active universe.

Product expectation: **54**. Reconciles: **false**. Live count is 62, not 54. Investigate Brand Status values or revise the protected baseline.

## Canonical active inventory (54)

| Brand Name | Slug | Record ID | Active Flag | OS State | Public | Presentation | Public Full? | PVQL Included? | Restore Cohort | Classification | Notes |
| --- | --- | --- | --- | --- | --- | ---: | --- | --- | --- | --- | --- |
| AC Hotels by Marriott | `ac-hotels-by-marriott` | `rec9aZp7GHtzUEg0c` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 107 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Aloft Hotels | `aloft-hotels` | `recJ1GZQpttX7qHgw` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 101 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Ascend Hotel Collection | `ascend` | `reclkgOzvAcBheUSo` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 213 | true | true | visibility_restored_code | `public_full_clean` | — |
| Autograph Collection | `autograph-collection` | `recEJCTDj1zrsjPM6` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 112 | true | true | intentional_public_restore | `public_full_clean` | — |
| avid hotels | `avid-hotels` | `recoEarnE8T6sDjZq` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 106 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Bunkhouse Hotels | `bunkhouse-hotels` | `recGv268Wda31PlSZ` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 107 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| BW Premier Collection | `bw-premier-collection` | `recwXZ5gVZ8ZH8ekA` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 101 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| BW Signature Collection | `bw-signature-collection` | `recdeh1NsP4gjrv80` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 101 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Canopy by Hilton | `canopy-by-hilton` | `recsggfbKlJbjeRP9` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 107 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| City Express by Marriott | `city-express-by-marriott` | `recucEzAS6724tOYA` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 107 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Comfort Inn & Suites | `comfort-inn-suites` | `recOzH5iAE1xEjyD0` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 216 | true | true | visibility_restored_code | `public_full_clean` | — |
| Country Inn & Suites by Choice | `country-inn-suites` | `recaayt9u7YYg8h7Y` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 219 | true | true | intentional_public_restore | `public_full_clean` | — |
| Courtyard by Marriott | `courtyard-by-marriott` | `rec6hye5H8zJmAGv3` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 106 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Curio Collection by Hilton | `curio-collection` | `receQkxgjlezsc1xg` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 215 | true | true | visibility_restored_code | `public_full_clean` | — |
| Dazzler by Wyndham | `dazzler-by-wyndham` | `rec5CNMM4ZUD7ZHlM` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 108 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Design Hotels | `design-hotels` | `rec02zPClpWUTCyXM` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 130 | true | true | primary_release | `public_full_clean` | — |
| DoubleTree by Hilton | `doubletree-by-hilton` | `rechVYWQ5ikRnr99B` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 100 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Even Hotels | `even-hotels` | `recvvmiyReHhiKdoK` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 107 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Everhome Suites | `everhome-suites` | `recqkkrsevi4r9ibj` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 212 | true | true | primary_release | `public_full_clean` | — |
| Fairmont | `fairmont` | `recJhPaDVU3YUDQUt` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 107 | true | true | none | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Hampton by Hilton | `hampton-by-hilton` | `rectRvOWQPaL6FkzZ` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 100 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Handwritten Collection | `handwritten-collection` | `rec7hTXwMRC81EPqz` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 109 | true | true | intentional_public_restore | `public_full_clean` | — |
| Hilton Garden Inn | `hilton-garden-inn` | `recrvdAjRlXxPvPPF` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 100 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Hilton Hotels & Resorts | `hilton-hotels-and-resorts` | `recWubG3rhiS1BaWi` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 100 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Holiday Inn Express | `holiday-inn-express` | `recmGmiIqDtAsm01f` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 107 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Home2 Suites by Hilton | `home2-suites-by-hilton` | `reccZ4zV6wMav7a2i` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 100 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Homewood Suites by Hilton | `homewood-suites-by-hilton` | `recZjYI4nYflGHFNR` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 100 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Hotel Indigo | `hotel-indigo` | `recegXrqaPiSLGCIe` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 99 | true | true | primary_release | `public_full_clean` | — |
| ibis | `ibis` | `reclFXbpZ5XzLWbGP` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 107 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Kimpton Hotels | `kimpton` | `recCKuXCmGvxHPfb3` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 203 | true | true | primary_release | `public_full_clean` | — |
| Mama Shelter | `mama-shelter` | `recXCZCK05XXYX7Q8` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 106 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Marriott Hotels | `marriott-hotels` | `recn59UtkyyoYwzSz` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 102 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Mercure | `mercure` | `recevrLJ3m6rIug3S` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 107 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| MGallery Collection | `mgallery-collection` | `recrWCD1LMqu864oU` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 97 | true | true | primary_release | `public_full_clean` | — |
| Motto by Hilton | `motto-by-hilton` | `reclt44apoi8co0e6` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 107 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Moxy Hotels | `moxy-hotels` | `recahVIW4aCx0Ao84` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 106 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Novotel | `novotel` | `recQE2lSSSSyuUrMQ` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 107 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Preferred Hotels & Resorts | `preferred-hotels-and-resorts` | `recwl5JOYxlChuCAr` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 101 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Pullman | `pullman` | `recFW9kfqKfOjv7Z1` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 107 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Quality Inn | `quality-inn` | `recd8o4k1JddhkRWW` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 208 | true | true | intentional_public_restore | `public_full_clean` | — |
| Radisson Blu by Choice | `radisson-blu` | `recWPEvxBQxVVzSq3` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 222 | true | true | intentional_public_restore | `public_full_clean` | — |
| Radisson by Choice | `radisson` | `recywbx1YQSTCPqW1` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 207 | true | true | intentional_public_restore | `public_full_clean` | — |
| Radisson Individuals by Choice | `radisson-individuals-by-choice` | `recRyvM8OmLlDj9G7` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 213 | true | true | primary_release | `public_full_clean` | — |
| Radisson RED by Choice | `radisson-red` | `recmKqo7M7mLZgRqQ` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 212 | true | true | intentional_public_restore | `public_full_clean` | — |
| Residence Inn by Marriott | `residence-inn-by-marriott` | `rec9Ufbpa0GxJGzt8` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 102 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Sheraton | `sheraton` | `recg8HjT5Bky7NXeV` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 102 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Small Luxury Hotels of the World | `small-luxury-hotels-of-the-world` | `recjjSnY2opb8P4DG` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 99 | true | true | primary_release | `public_full_clean` | — |
| SO/ | `so` | `recTJdPlr4mDs9app` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 107 | true | true | none | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Spark by Hilton | `spark-by-hilton` | `recfv66er4Ch2vJDO` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 100 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| SpringHill Suites by Marriott | `springhill-suites-by-marriott` | `recBzdGfkMUN9fYsv` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 101 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| StudioRes | `studiores` | `recDM0LAD8jVRA2x3` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 101 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Suburban Studios | `suburban-studios` | `reclcjg5Foa9Vs5TC` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 209 | true | true | intentional_public_restore | `public_full_clean` | — |
| Tapestry Collection by Hilton | `tapestry-collection-by-hilton` | `reccXxMHEh7NNRhIE` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 129 | true | true | intentional_public_restore | `public_full_clean` | — |
| Tempo by Hilton | `tempo-by-hilton` | `recqiHq3GHKMj8Meo` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 107 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| TownePlace Suites by Marriott | `towneplace-suites-by-marriott` | `recUPiPDivkhNUogr` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 101 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Trademark Collection by Wyndham | `trademark-collection-by-wyndham` | `recob7tgHRryRSbeO` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 108 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Tribute Portfolio | `tribute-portfolio` | `recCvV0PuZOi8c3hC` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 165 | true | true | visibility_restored_code | `public_full_clean` | — |
| Tru by Hilton | `tru-by-hilton` | `recJLiMTv4W8VgO9L` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 100 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Vignette Collection | `vignette-collection` | `recDwzv86TWnz2gGB` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 109 | true | true | intentional_public_restore | `public_full_clean` | — |
| Voco Hotels | `voco-hotels` | `recwONQTqGU1jHCsM` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 107 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| Westin | `westin` | `recIPuBC50fv13zRR` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 102 | true | true | intentional_public_restore | `public_full_clean` | Absent from prior 23-brand reconciliation inventory |
| WoodSpring Suites | `woodspring-suites` | `recsOd51NzRPYsMko` | Brand Status="Active" matches OR({Brand Status}='Active', {Brand Status}='Live') | active_profile_ready | public_full | 211 | true | true | intentional_public_restore | `public_full_clean` | — |

## Classification buckets

### public_full_clean

Slugs: `ac-hotels-by-marriott`, `aloft-hotels`, `ascend`, `autograph-collection`, `avid-hotels`, `bunkhouse-hotels`, `bw-premier-collection`, `bw-signature-collection`, `canopy-by-hilton`, `city-express-by-marriott`, `comfort-inn-suites`, `country-inn-suites`, `courtyard-by-marriott`, `curio-collection`, `dazzler-by-wyndham`, `design-hotels`, `doubletree-by-hilton`, `even-hotels`, `everhome-suites`, `fairmont`, `hampton-by-hilton`, `handwritten-collection`, `hilton-garden-inn`, `hilton-hotels-and-resorts`, `holiday-inn-express`, `home2-suites-by-hilton`, `homewood-suites-by-hilton`, `hotel-indigo`, `ibis`, `kimpton`, `mama-shelter`, `marriott-hotels`, `mercure`, `mgallery-collection`, `motto-by-hilton`, `moxy-hotels`, `novotel`, `preferred-hotels-and-resorts`, `pullman`, `quality-inn`, `radisson-blu`, `radisson`, `radisson-individuals-by-choice`, `radisson-red`, `residence-inn-by-marriott`, `sheraton`, `small-luxury-hotels-of-the-world`, `so`, `spark-by-hilton`, `springhill-suites-by-marriott`, `studiores`, `suburban-studios`, `tapestry-collection-by-hilton`, `tempo-by-hilton`, `towneplace-suites-by-marriott`, `trademark-collection-by-wyndham`, `tribute-portfolio`, `tru-by-hilton`, `vignette-collection`, `voco-hotels`, `westin`, `woodspring-suites`

### public_full_failing_pvql

Slugs: —

### restored_pending_validation

Slugs: —

### fully_ready_held_from_public

Slugs: —

### content_remediation_needed

Slugs: —

### image_remediation_needed

Slugs: —

### true_incomplete

Slugs: —

### duplicate_or_mapping_issue

Slugs: —

### active_but_unconfigured

Slugs: —

## Governance

- Do **not** use prior 23-brand reconciliation lists as the active universe.
- Operational cohorts (PRIMARY_RELEASE, Lane 1/2, intentional restore) are subsets/overlays.
- PVQL / OS / restore scripts must start from this Brand Status Active/Live set (or an explicit subset with rationale).

