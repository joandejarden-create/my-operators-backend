# Hilton hotel descriptions → Hotel Census

## What you get

Hilton’s public **GraphQL** API returns the same marketing copy shown on each hotel’s website under **Description**:

- **Primary field:** `facilityOverview.shortDesc`
- **Also available:** `headline`, `locationShortDesc`, `hotelTeaserText`, `directionsTo`

Example — **Gran Hotel Costa Rica** (`SJOCUQQ`):

> Our 1930’s building, a historical-architectural monument of Costa Rica, features contemporary urban interiors. We’re located on the Avenida Central pedestrian boulevard, steps from the National Theatre, Gold Museum, cafés, and shops. We have a fitness center and piano bar. Our staff is on-hand with recommendations to help you live like a local.

This does **not** come from the locations directory JSON (that tier has no description). Detail HTML pages are often blocked (403); GraphQL is the reliable path.

---

## Airtable prerequisite

Add to **Hotel Census** (Platform base):

| Column | Type | Maps from Hilton |
|--------|------|------------------|
| **Hotel Description** | Long text / multilineText | `shortDesc` |
| **Hotel Headline** (optional) | Single line text | `headline` |

Override names via env:

- `AIRTABLE_CENSUS_DESCRIPTION_FIELD` (default `Hotel Description`)
- `AIRTABLE_CENSUS_HOTEL_HEADLINE_FIELD` (default `Hotel Headline`)

Until `Hotel Description` exists, use plan CSV/JSON output only.

---

## Workflow

### 1. Test one hotel

```bash
node scripts/plan-hilton-census-descriptions.mjs --ctyhocn SJOCUQQ
```

### 2. Plan a brand (e.g. Curio)

```bash
npm run plan-hilton-census-descriptions -- --brand "Curio Collection by Hilton"
```

### 3. Plan all Hilton brands with census rows

```bash
npm run plan-hilton-census-descriptions -- --brand-codes QQ,GI,HP,HI,UA,UP,PY,CH,WA,OL,DT,ES,HT,HW,HP,RU,PE,PO,SA,AQ,ID,EY,GU,GV
```

Outputs:

- `reports/hilton-census-descriptions-plan-<brand>.json`
- `reports/hilton-census-descriptions-plan-all-brands.csv`

### 4. Apply (after Hotel Description column exists)

```bash
node scripts/apply-hilton-census-descriptions.mjs --input reports/hilton-census-descriptions-plan-curio-collection-by-hilton.json
```

Fill-blank only for `Hotel Description` / `Hotel Headline`.

---

## Matching

1. Crawl Hilton locations directory per brand (`ctyhocn` / brand property code).
2. Match to census via existing directory matcher (ctyhocn, website, name+geo).
3. Fetch description by `ctyhocn` from GraphQL.

---

## Modules

| Module | Role |
|--------|------|
| `lib/hilton-hotel-description-fetch.js` | GraphQL fetch + `pickPrimaryHiltonDescription()` |
| `lib/hotel-census/hilton-description-enrichment-contract.js` | Census field map + probe |
| `lib/hotel-census/plan-hilton-census-descriptions.js` | Plan logic |

---

## Risks

| Risk | Mitigation |
|------|------------|
| GraphQL rate limits | Default 300ms delay between hotels (`--fetch-delay-ms`) |
| Missing Airtable column | Plan still writes CSV; apply probes field first |
| Match gaps | Only matched census rows get descriptions; review unmatched in directory plan |
| Licensed copy | Hilton marketing text — use per brand terms / internal product policy |

---

## Regression checklist

- Curio Gran Hotel (`SJOCUQQ`) description matches website
- Apply does not overwrite existing Description when fill-blank
- Brand Explorer / census read paths unaffected until Description is consumed in UI
