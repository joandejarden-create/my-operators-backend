# IHG CALA Hotel Census enrichment — Website / Property ID / Amenities

**Generated:** 2026-07-23  
**Mode:** Official IHG sources only · fill-blank · dry-run then apply · `typecast: true`

## Summary

| Metric | Count |
|--------|------:|
| CALA IHG census rows | 328 |
| Directory hotels (country + city pages + gap enrich) | 304 |
| Website + Property ID applied (cumulative) | **252** |
| — prior batch | 243 |
| — recovery batch (this run) | **9** |
| Amenities applied (cumulative highlight labels) | **241** |
| — prior batch | 236 |
| — recovery batch (this run) | **5** |
| Steward review (low / below gate) | 0 |
| Unmatched census (gaps) | **76** |
| Amenity fetch empty/blocked (recovery) | 4 |
| Steward triage (this pass) | **76 classified · 0 new applies** |

## Steward triage (2026-07-23)

Recovery expansion checked: island destination pages (Belize, St Kitts, Saint Lucia, Curaçao, etc.) all **404** on ihg.com; destinations sitemap has **0** island hits. Near-misses are either PID-claimed duplicates or below apply gate — **no live apply**.

| Bucket | Count |
|--------|------:|
| pipeline_or_unopened | 45 |
| name_variant_needs_human | 11 |
| alliance_not_on_ihg_directory | 10 |
| census_duplicate_pid_claimed | 7 |
| missing_from_public_listing | 2 |
| no_destination_page_market | 1 |

Reports: `reports/ihg-census-unmatched-steward-triage.csv`, `reports/ihg-census-unmatched-steward-triage.json`  
Triage script: `scripts/triage-ihg-census-unmatched-steward.mjs`

## Recovery batch (9 Website + Property ID)
| Census | Property ID | Confidence |
|--------|-------------|------------|
| Holiday Inn Natal | NATAR | high |
| avid hotel Tijuana - Otay | TIJAV | high |
| Holiday Inn Express Veracruz Centro Historico | VCRDT | high |
| Holiday Inn Morelia | MLMFO | high |
| Crowne Plaza Mexico City North Tlalnepantla | MEXLM | high |
| Avid Queretaro Sur | QROAV | high |
| Holiday Inn Resort Santiago - Presa La Boca | MTYSA | medium |
| Holiday Inn Leon Convention Center | BJXAP | medium |
| Holiday Inn Express & Suites Diamond (SVG) | SVDDM | medium |

Amenities recovered for: VCRDT, MEXLM, MTYSA, BJXAP, SVDDM (5). Empty/blocked: NATAR, TIJAV, MLMFO, QROAV.

## Source

- Destination country pages: `https://www.ihg.com/destinations/us/en/{slug}-hotels`
- City destination pages from `destinations.en.sitemap.xml` (CALA only; US border bleed filtered by `countryCode`)
- Hoteldetail sitemaps + h1 name enrich for gap city slugs (e.g. `diamond`, `natal`)
- Property ID = IHG mnemonic from URL (`/…/{mnemonic}/hoteldetail`)
- Amenities = server-rendered `span.amenity-title` highlight list (not invented)

## Field mapping

| Logical | Airtable field |
|---------|----------------|
| website | `Website` |
| propertyId | `Property ID` (`CENSUS_PROPERTY_ID_FIELD`) |
| amenities | `Amenities` |

## Scripts / libs

- `lib/ihg-brand-directory-extract.js` — city harvest, gap enrich, US bleed filter
- `lib/hotel-census/plan-ihg-census-directory-match.js` — name normalize, brand-from-card-name, claimed-ID guard
- `scripts/extract-ihg-cala-directory.mjs` — `--harvest-city-pages --enrich-gap-slugs`
- `scripts/sync-ihg-census-from-directory.mjs`
- `scripts/backfill-ihg-census-amenities.mjs`

## Reports

- `reports/ihg-cala-directory-extract.json` / `.csv`
- `reports/ihg-census-directory-match-plan.json`
- `reports/ihg-census-steward-review.csv`
- `reports/ihg-census-unmatched-steward.csv`
- `reports/ihg-census-unmatched-steward-triage.csv` / `.json`
- `reports/ihg-census-apply-dry-run.json`
- `reports/ihg-census-enrichment-apply-log.json` / `.csv` (latest recovery batch)
- `reports/ihg-census-enrichment-apply-log-prior.json` (prior 243)
- `reports/ihg-census-amenities-apply-log.json` (latest recovery amenities)
- `reports/ihg-census-amenities-apply-log-prior.json` (prior 236)

## Steward gaps / limitations

1. **Six Senses / alliance** — not on ihg.com directories; leave stewarded (no third-party scrape).
2. **Missing destination pages** — Curaçao, Turks & Caicos, Saint Lucia, Antigua, St Kitts, BVI, USVI, Cuba, Haiti, Martinique, Guadeloupe, Bonaire, Belize.
3. **Census duplicates** — same hotel already matched on another record (e.g. Grand Cayman, Santo Domingo DO, Navojoa typo, Avid Fresnillo vs Avid/ZCLAV); one-to-one Property ID, no reassignment.
4. **Pipeline / unopened** — many Mexico/Colombia names absent from public IHG listings.
5. **Brand naming variants** — e.g. Buenavista vs Zona Centro held back (generic city slug).
6. **Amenities** — highlight chips only; some hoteldetail pages return empty.
7. Apply gate: **medium+ confidence**, minScore 68, minNameSim 0.6.

## Rollback

Re-clear fill-blank fields for record IDs in:
- `reports/ihg-census-enrichment-apply-log.json` (recovery 9)
- `reports/ihg-census-enrichment-apply-log-prior.json` (prior 243)
- amenities logs (prior + recovery)
