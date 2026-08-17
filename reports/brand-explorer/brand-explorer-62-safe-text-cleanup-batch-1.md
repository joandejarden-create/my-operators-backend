# Brand Explorer 62 — Safe Text Cleanup Batch 1 (Founder Review)

**Status:** `brand_explorer_62_safe_text_cleanup_batch_1_ready_for_founder_review`
**Generated:** 2026-08-05T14:28:49.802Z
**Mode:** dry-run / exact patch proposals only — **do not apply**

## 1. Executive summary

- Brands in Batch 1: **34**
- Patches: **68** (1A high-leakage **36** · 1B lighter **32**)
- Property example refreshes included: **1** (Kimpton Mas Olas only — true same-property Census match)
- MGallery render fix: **held** (majors not Low-risk)
- Held for later: **7** · Founder decisions: **7**
- Gates: active=62 · semantic C/H/M={"critical":0,"high":0,"medium":0,"low":0} · footnote=true · momentum=true · mandatory=true · PVQL=PASS 62/62 · quality=`freeze_after_minor_cleanup_pass` (61 approve · 1 minor: mgallery-collection)

## 2. Brands included in Batch 1

`aloft-hotels`, `ascend`, `comfort-inn-suites`, `country-inn-suites`, `design-hotels`, `doubletree-by-hilton`, `everhome-suites`, `fairmont`, `hampton-by-hilton`, `hilton-garden-inn`, `hilton-hotels-and-resorts`, `home2-suites-by-hilton`, `homewood-suites-by-hilton`, `ibis`, `kimpton`, `mama-shelter`, `marriott-hotels`, `mercure`, `novotel`, `pullman`, `quality-inn`, `radisson-individuals-by-choice`, `radisson-red`, `residence-inn-by-marriott`, `sheraton`, `spark-by-hilton`, `springhill-suites-by-marriott`, `studiores`, `suburban-studios`, `tapestry-collection-by-hilton`, `towneplace-suites-by-marriott`, `trademark-collection-by-wyndham`, `tru-by-hilton`, `westin`

## 3. Fields included

- `footprint.editorial.Body`
- `footprint.editorial_bullets.Body`
- `footprint.geo_intro.Body`
- `footprint.momentum.Body`
- `footprint.openings.Body`
- `footprint.openings.Case Summary Interpretation`
- `footprint.openings.Case Summary Overview`
- `footprint.openings.Title`
- `footprint.portfolio_mix.Body`
- `footprint.region.apac.Body`
- `footprint.region.cala.Body`
- `loyalty.proof.Body`
- `materials.caseStudy.Body`
- `materials.caseStudy.Case Summary Overview`

## 4. Forbidden language removed

| Term id | Patch count |
| --- | ---: |
| `census` | 31 |
| `consumer_site` | 6 |
| `listed_on_choice` | 13 |
| `census_property_url` | 3 |
| `active_property_page` | 3 |
| `choicehotels_domain_prose` | 6 |
| `census_url` | 7 |
| `item_19_tables` | 1 |
| `chd_brand_page` | 1 |
| `chd` | 4 |
| `chd_everhome` | 1 |
| `source_pack` | 4 |

## 5. Property examples refreshed

- **kimpton** (`recYOSVJXXqkGjN6m`): `Kimpton Mas Olas Resort & Spa Kimpton Hotels — Todos Santos` → `Kimpton Mas Olas Resort & Spa — Todos Santos, Mexico`

## 6. MGallery render fix proposal

- Recommendation: **`hold_for_later`**
- Risk: **Medium** — missing major slots (audience / valueOwners.overview / watchouts) are not Batch 1 Low-risk text cleanup

## 7. Exact before/after diffs

See `brand-explorer-62-safe-text-cleanup-batch-1-diff.md`.

## 8. Patches held for later

- **courtyard-by-marriott** — fuzzy_census_match_is_different_property — do not rewrite BE example to a different hotel
- **curio-collection** — fuzzy_census_match_is_different_property — do not rewrite BE example to a different hotel
- **hilton-garden-inn** — fuzzy_census_match_is_different_property — do not rewrite BE example to a different hotel
- **hilton-garden-inn** — fuzzy_census_match_is_different_property — do not rewrite BE example to a different hotel
- **holiday-inn-express** — fuzzy_census_match_is_different_property — do not rewrite BE example to a different hotel
- **motto-by-hilton** — fuzzy_census_match_is_different_property — do not rewrite BE example to a different hotel
- **MGallery Collection** — Quality quiet minor is not Low-risk factual-safe for Batch 1; majors require content additions

## 9. Founder decisions needed

- **courtyard-by-marriott** · property_example_false_match — "Courtyard by Marriott Bogota Airport — Bogotá" was loosely matched to Census "Courtyard by Marriott Monterrey Airport" — keep International/CALA example; do not swap
- **curio-collection** · property_example_false_match — "Nacar Hotel Cartagena Curio Collection by Hilton — Cartagena" was loosely matched to Census "MS Milenium Monterrey, Curio Collection by Hilton" — keep International/CALA example; do not swap
- **hilton-garden-inn** · property_example_false_match — "Hilton Garden Inn Bogota Airport — Bogotá" was loosely matched to Census "Hilton Garden Inn Guadalajara Airport" — keep International/CALA example; do not swap
- **hilton-garden-inn** · property_example_false_match — "Hilton Garden Inn Guanacaste Airport — CALA" was loosely matched to Census "Hilton Garden Inn Guadalajara Airport" — keep International/CALA example; do not swap
- **holiday-inn-express** · property_example_false_match — "Holiday Inn Express & Suites Bogota DC — Bogotá" was loosely matched to Census "Holiday Inn Express Tapachula" — keep International/CALA example; do not swap
- **motto-by-hilton** · property_example_false_match — "Motto by Hilton Cusco — Cusco" was loosely matched to Census "Motto by Hilton Tulum" — keep International/CALA example; do not swap
- **mgallery-collection** · webflow_render_minor — MGallery needs dedicated cleanup for missing major slots; not include_in_batch_1

## 10. Validation gate results

```json
{
  "activeCount": 62,
  "semanticCHM": {
    "critical": 0,
    "high": 0,
    "medium": 0,
    "low": 0
  },
  "pvqlPass": true,
  "pvqlCount": 62,
  "footnotePass": true,
  "momentumPass": true,
  "mandatoryPass": true,
  "quality": {
    "freezeDecision": "freeze_after_minor_cleanup_pass",
    "recommendationCounts": {
      "approve_for_baseline_freeze": 61,
      "approve_after_minor_cleanup": 1
    },
    "minor": [
      "mgallery-collection"
    ]
  },
  "semanticFreeze": "ready_to_freeze_62_semantic_qa_clean",
  "footnoteSummary": {
    "activeCount": 62,
    "previewCount": 0,
    "totalRows": 62,
    "pass": 62,
    "fail": 0,
    "footnoteVisibleCount": 62,
    "footnoteMissingCount": 0
  },
  "lighterGatesRevalidatedAt": "2026-08-05T14:30:54.320Z",
  "note": "PVQL + quality quiet re-run in progress after batch prep"
}
```

## 11. Apply command (later — do not run now)

```bash
node scripts/brand-explorer-62-safe-text-cleanup-batch-1-apply.mjs --dry-run  # (not created until approved) then --apply with confirm flags
```

**Final status:** `brand_explorer_62_safe_text_cleanup_batch_1_ready_for_founder_review`

