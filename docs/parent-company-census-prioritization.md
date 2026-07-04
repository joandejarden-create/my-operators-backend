# Parent-company prioritization — independent hotel census

**Phases:** Brand Setup CALA inventory (4F) + Choice brand-directory seeds (4G), read-only.  
**Source of truth:** `Brand Setup - Brand Basics` on `AIRTABLE_BASE_ID` — not legacy Hotel Census `Parent Company` and not manually typed brand lists.

Regenerate inventory:

```bash
npm run independent-census:brand-setup:cala-inventory
```

Reports:

- `reports/brand-setup-cala-parent-company-inventory.json`
- `reports/brand-setup-cala-parent-company-inventory.csv`
- `reports/brand-setup-cala-brand-gaps.csv`

---

## 1. Brand Setup table identified

| Layer | Table | Base env |
|-------|--------|----------|
| Brand profile / Explorer setup | **Brand Setup - Brand Basics** | `AIRTABLE_BASE_ID` |
| Linked tabs (not full inventory here) | Brand Footprint, Project Fit, Fee Structure, Brand Standards, Deal Terms, Portfolio & Performance, Operational Support, Legal Terms, Loyalty & Commercial, Brand Explorer Presentation | Same |
| Affiliation matching (read-only compare) | **Brand Alias Mapping** | `AIRTABLE_BASE_ID_ALT` |

Field mapping follows `api/brand-library.js` (`F.brandBasics`): Parent Company, Hotel Chain Scale, Brand Architecture (family/collection), Hotel Service Model, Brand Positioning, Region Offered, Brand Status, Brand Website.

**Include in Brand Explorer:** dedicated column used when present; otherwise derived from **Brand Status** (`Active` / `Live` = shown).

---

## 2. Actual parent companies (from Brand Setup)

Default run inventories **all** Brand Setup - Brand Basics rows (every status). Optional filter: `--activeOnly=true` for Active/Live only (Brand Explorer visibility).

Re-run for live counts:

```bash
npm run independent-census:brand-setup:cala-inventory
```

JSON report section `parentCompanies` lists:

- Raw parent-company labels as stored in Airtable
- Normalized parent key (`normalizeParentCompanyKey`)
- Brand count per parent
- CALA-relevant brand count (`Region Offered` contains CALA, Caribbean, Latin America, Americas, etc.)
- Recommended priority rank (mapping below — does not rename Airtable values)

---

## 3. Target priority order (independent census validation)

Use this **processing order** for brand-directory discovery and two-source validation batches. Map each Brand Setup **Parent Company** string to the nearest row via normalized matching; do not overwrite Airtable labels automatically.

| Rank | Target operator | Normalized match hints |
|------|-----------------|-------------------------|
| 1 | Choice Hotels | `choice hotels` |
| 2 | IHG | `ihg`, `intercontinental hotels group` |
| 3 | Marriott International | `marriott` |
| 4 | Hilton | `hilton` |
| 5 | Hyatt | `hyatt` |
| 6 | Accor | `accor` |
| 7 | Wyndham | `wyndham` |
| 8 | Radisson / legacy Radisson | `radisson`, `rhg`, `rezidor` |
| 9 | Meliá | `melia`, `meliá` |
| 10 | Barceló | `barcelo`, `barceló` |
| 11 | Palladium | `palladium` |
| 12 | Karisma | `karisma` |
| 13 | Playa | `playa` |
| 14 | RCD | `rcd` |
| 15 | Other regional / independent operators | everything else |

---

## 4. Brands to process first (by parent)

The **full Brand Setup inventory** (all statuses, 251 brands / 30 parents) is the parent-company source of truth for independent census validation order.

**Validation priority:** Choice → IHG → Marriott → Hilton → Hyatt (+ Hyatt Vacation Ownership) → Accor → Wyndham → Radisson → regional groups (Sonesta, BWH, Red Roof, Minor, Iberostar, Rosewood, Shangri-La, etc.).

**Choice is first** because it is priority #1 and has **22** Brand Setup brands under **Choice Hotels International**.

Within each parent, process **CALA-relevant** brands (`Region Offered` includes Caribbean & Latin America / CALA / Americas). Carry **Brand Status** in seeds; prioritize **Active/Live** (`priorityRank` 1) before Draft/inactive (`priorityRank` 2).

See `reports/brand-setup-cala-parent-company-inventory.csv` and JSON `brandsByRecommendedPriority`.

---

## 4G. Brand directory seeds from Brand Setup (Choice first)

Seeds are **generated from Brand Setup**, not hand-typed brand lists.

```bash
npm run independent-census:brand-directory:generate-seeds-from-brand-setup -- \
  --parent-company "Choice Hotels International" \
  --normalized-parent-company "choice hotels" \
  --batch-id brand-directory-seeds-choice-hotels-brand-setup-2026-05-20
```

Output: `fixtures/independent-census/brand-directory-seeds-choice-hotels-from-brand-setup.json`

Each seed includes: brand, parent, status, family, chain scale, service model, regions, `sourceUrl`, `sourceMethod`, `priorityRank`, `missingSourceUrl`, `requiresManualReview: true`.

**Missing URLs** stay in the seed file with `missingSourceUrl: true` — fix in Brand Setup before expecting property-level discovery.

Dry-run (search-list, report-only):

```bash
npm run independent-census:brand-directory:dry-run -- \
  --mode search-list \
  --input fixtures/independent-census/brand-directory-seeds-choice-hotels-from-brand-setup.json \
  --batch-id brand-directory-choice-hotels-brand-setup-2026-05-20
```

Brand-directory discovery remains **review_required** and **report-only** (no Candidate/Evidence/Verified writes).

**Next after Choice discovery:** match Choice discovery leads against **Independent Hotel Source Candidates** (OSM DR batch) and **Verified Independent Hotel Census** using safe name + geo matching (no STR fields).

---

## 4H. Property-level discovery path analysis (Choice)

Phase 4G produced **brand-level leads only** (22 Choice `sourceUrl` entry points, 0 property rows). Property-level hotels require **locator pages**, **sitemap review**, **permissioned exports**, or **manual research** — not deep scraping.

```bash
npm run independent-census:brand-directory:analyze-property-paths -- \
  --input fixtures/independent-census/brand-directory-seeds-choice-hotels-from-brand-setup.json \
  --batch-id choice-property-paths-2026-05-20
```

Reports:

- `reports/independent-census-brand-directory-property-paths-choice-hotels-2026-05-20.json`
- `reports/independent-census-brand-directory-property-paths-choice-hotels-2026-05-20.csv`

**Allowed fetches:** each seed `sourceUrl` once; `robots.txt` on same host; at most **one** sitemap URL if listed in robots or `/sitemap.xml` (no recursive crawl; no booking/rate endpoints).

**Policy:** Do **not** apply brand-directory output to **Independent Hotel Source Candidates** until property-level records exist and `brand_directory` source policy is human-reviewed.

**Next step:** follow per-brand `recommendedDiscoveryMethod` in the path report (sitemap review vs locator vs permissioned export), then re-run dry-run with explicit `hotelUrls` or approved sitemap mode — still report-only until ops sign-off.

---

## 4I. Choice sitemap child-index review

Inspects `https://www.choicehotels.com/sitemapindex.xml` with capped child sitemap fetches (URLs only).

```bash
npm run independent-census:brand-directory:review-sitemap -- \
  --sitemap-url "https://www.choicehotels.com/sitemapindex.xml" \
  --parent-company "Choice Hotels International" \
  --brand-seeds fixtures/independent-census/brand-directory-seeds-choice-hotels-from-brand-setup.json \
  --max-child-sitemaps 5 \
  --max-urls 500 \
  --batch-id choice-sitemap-review-2026-05-20
```

Reports: `reports/independent-census-brand-directory-sitemap-review-choice-2026-05-20.json` / `.csv`

No property HTML fetched. No Candidate ingest. See `recommendedNextAction` in report.

---

## 4J. Choice property URL extract (propertysitemap.xml.gz)

Full URL-only parse of `propertysitemap.xml.gz`, mapped to Brand Setup brands, CALA-tagged by path segment.

```bash
npm run independent-census:brand-directory:extract-property-urls -- \
  --property-sitemap-url "https://www.choicehotels.com/propertysitemap.xml.gz" \
  --parent-company "Choice Hotels International" \
  --brand-seeds fixtures/independent-census/brand-directory-seeds-choice-hotels-from-brand-setup.json \
  --region-filter CALA \
  --batch-id choice-property-urls-cala-2026-05-20
```

Reports: `reports/independent-census-choice-property-url-extract-cala-2026-05-20.json` / `.csv`

- Parses `/{country-or-region}/{city}/{brand-slug}-hotels/{propertyId}` only.
- **Does not** fetch property pages or create Candidates.
- `recommendedAction` per row: `ready_for_candidate_review` is blocked until `hold_for_source_policy_review` / policy sign-off.
- **Next:** match CALA property URLs to OSM **Candidates** and **Verified** (safe name + geo, no STR).

---

## 4K. Choice property URL match (OSM candidates + Verified)

Compares Phase 4J CALA property URL leads to **Independent Hotel Source Candidates** and **Verified Independent Hotel Census** (read-only). No candidate ingest until source policy sign-off.

```bash
npm run independent-census:brand-directory:match-properties -- \
  --property-url-report reports/independent-census-choice-property-url-extract-cala-2026-05-20.json \
  --parent-company "Choice Hotels International" \
  --candidate-batch-id osm-dominican-republic-hotel-focused-2026-05-20 \
  --batch-id choice-property-match-cala-2026-05-20
```

Reports: `reports/independent-census-choice-property-match-cala-2026-05-20.json` / `.csv`

- Match signals: country, city slug, brand/name similarity, `choicehotels.com` website host — **no STR IDs**.
- If matching is useful, next controlled step is Choice `brand_directory` **candidate** or **evidence** rows after policy review.

---

## 4L. Choice property URL candidate apply (gated)

Writes CALA property URL leads to **Independent Hotel Source Candidates** only when `--apply`, `--source-policy-approved`, and `INDEPENDENT_CENSUS_PIPELINE_ENABLED=true`. Default: dry-run.

```bash
npm run independent-census:brand-directory:apply-property-candidates -- \
  --property-url-report reports/independent-census-choice-property-url-extract-cala-2026-05-20.json \
  --match-report reports/independent-census-choice-property-match-cala-2026-05-20.json \
  --parent-company "Choice Hotels International" \
  --batch-id choice-cala-property-url-candidates-2026-05-20

# Apply (after policy sign-off):
# ...same args... --source-policy-approved --apply
```

Reports: `reports/independent-census-brand-directory-property-apply-{batchId}.json` / `.csv`

- **No** Verified promotion, **no** Evidence rows, **no** property HTML.
- Dedupe: `brand_directory` + Source Record ID (property ID) + Source URL + batch.

---

## 4N. Choice property re-match (expanded OSM pool)

After Phase 4M, re-run property URL matching against all CALA OSM batches (`--all-osm-candidates`), not only the original DR batch.

```bash
npm run independent-census:brand-directory:match-properties -- \
  --property-url-report reports/independent-census-choice-property-url-extract-cala-2026-05-20.json \
  --all-osm-candidates \
  --batch-id choice-property-match-cala-expanded-osm-2026-05-20
```

Reports: `reports/independent-census-choice-property-match-cala-expanded-osm-2026-05-20.json` / `.csv`

---

## 4M. OSM hotel-focused expansion (Choice CALA countries)

Runs hotel-focused Overpass dry-run per country, then optional gated apply to **Candidates** only.

```bash
npm run independent-census:osm:country-list -- \
  --countries "Colombia,Mexico,Chile,Dominican Republic,Costa Rica,Panama,Trinidad and Tobago,Ecuador,Argentina,Bahamas,Honduras,Puerto Rico,Peru,Brazil" \
  --batch-id choice-cala-osm-expansion-2026-05-20

# Apply: add --apply and INDEPENDENT_CENSUS_PIPELINE_ENABLED=true
```

Per-country batch: `osm-{country-slug}-hotel-focused-choice-cala-2026-05-20`  
Summary: `reports/independent-census-osm-country-list-choice-cala-osm-expansion-2026-05-20.json`

---

## 5. Parent-company name normalization

Report flag: `inconsistentParentCompanies` when multiple raw **Parent Company** strings normalize to the same key (e.g. suffix variants).

**Action:** pick one canonical label per group in Brand Setup (human edit later — not done by this script).

Normalized keys use `lib/hotel-census/brand-alias-resolve.js` (`normalizeParentCompanyKey`) for comparison to **Brand Alias Mapping** only.

---

## 6. Brands needing official directory / website URLs

Gap report: `reports/brand-setup-cala-brand-gaps.csv`

Flags:

- `website_or_directory_url` — no **Brand Website** / directory URL on Brand Basics
- `region_offered` — CALA relevance unknown
- `no_alias_mapping` — no row in Brand Alias Mapping for canonical brand name
- `parent_company` / `brand_family` — missing core setup fields

**Rule:** run `independent-census:brand-directory:dry-run` only after Brand Setup has a chain-level or brand-level official URL for that parent.

---

## 7. Alias mapping vs Brand Setup

| Check | Report field |
|-------|----------------|
| Alias present for brand | `aliasMappingPresent`, `aliasCount` |
| Canonical in Alias Mapping but not in Brand Setup | `aliasOnlyNotInSetup` |
| Brand Setup brand with no alias | `setupWithoutAlias` |

Independent census matching should use **Brand Alias Mapping** affiliations tied to **Brand Setup** canonical names — not raw Hotel Census affiliations alone.

---

## 8. Recommended next actions (independent census pipeline)

1. **Refresh inventory** after Brand Setup edits: `npm run independent-census:brand-setup:cala-inventory`
2. **CALA OSM/Wikidata** — continue DR two-source work (Phases 4A–4E); use parent priority when adding brand-directory validation for flagged chains.
3. **City backfill** — 61 DR `review_before_promote` rows still need city on **Candidates** (manual Airtable or future gated update — not Hotel Census copy).
4. **Brand directory** — `independent-census:brand-directory:generate-seeds-from-brand-setup` then `independent-census:brand-directory:dry-run` per parent (Choice complete first).
5. **Alias alignment** — resolve `setupWithoutAlias` and `aliasOnlyNotInSetup` before census-backed promotion at scale.

---

## Safety (this phase)

- No writes to Hotel Census, Brand Setup, Brand Alias Mapping, Independent Hotel Source Candidates / Evidence, or Verified Independent Hotel Census.
- No STR / CoStar fields read or exported.
- No production API / UI / scoring / Brand Explorer code changes.
