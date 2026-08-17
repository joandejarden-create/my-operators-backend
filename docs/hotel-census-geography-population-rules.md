# Hotel Census geography — population rules

**Scope:** `Hotel Census` on Deal Capture Platform (`AIRTABLE_BASE_ID_ALT`).

**Fields covered:**

| Airtable field | Type | Notes |
|----------------|------|--------|
| `Region` | multilineText | Dealality UI region (5 labels) |
| `Sub-Continent` | singleLineText | CALA subdivision — **not** a separate `Sub Continent` column |
| `Market` | singleLineText | STR market taxonomy — **official field** for Excel `STR Market` import |
| `Submarket` | singleLineText | STR submarket taxonomy — **official field** for Excel `STR Submarket` import |

Related but **out of scope for this doc:** `Dealality Market` (steward-only commercial geography), `Continent` (not in live base).

---

## Geography hierarchy

```
Region (Dealality — product / Brand Explorer)
  └── Sub-Continent (CALA commercial geography)
        └── country / city (property location)
              └── Market (STR)
                    └── Submarket (STR)
```

**Dealality Market** sits parallel to STR `Market`/`Submarket` — never copy between them.

---

## 1. Region

**Purpose:** Roll up hotels into the five Dealality UI regions used in Brand Explorer, Radar, and operator footprint views.

**Allowed values** (exact labels):

- `Caribbean & Latin America`
- `North America`
- `Europe`
- `Middle East & Africa`
- `Asia Pacific`

**Source priority**

| Tier | Source | When to use |
|------|--------|-------------|
| 1 | Steward-verified `Region` on census row | Keep if value matches an allowed label |
| 2 | `country` → Dealality map | `lib/hotel-census/region.js` (`countryToDealalityRegion`) |
| 3 | Manual review | Country unmapped → `Other`; flag for steward |

**Write rules**

- Automated enrichment: **fill blank only** — never overwrite a valid steward value.
- Do **not** derive from STR `Market`, Hilton directory, or city name.
- New CALA portfolio rows (e.g. HE CALA apply): set from country at create time.

**Code:** `resolveDealalityRegion`, `resolveCensusRegion` in `lib/hotel-census/region.js` and `geography-enrichment-contract.js`.

**Current census (2026-06):** ~100% `Caribbean & Latin America` — expected for CALA STR import scope.

---

## 2. Sub-Continent

**Purpose:** Finer geography within a Dealality Region for filters, reporting, and CALA market views. This is **not** UN continent and **not** the same as `Region`.

**Allowed values (CALA census today)**

- `Caribbean`
- `Central America`
- `South America`
- `North America` — used for **Mexico** (geographic North America while `Region` stays `Caribbean & Latin America`)

**Source priority**

| Tier | Source | When to use |
|------|--------|-------------|
| 1 | Steward-verified `Sub-Continent` | Keep if in allowed list |
| 2 | `country` → map | `countryToSubContinent` in `geography-enrichment-contract.js` |
| 3 | Manual review | Unknown country or disputed territory |

**Write rules**

- Automated enrichment: **fill blank only**.
- Do **not** infer from `Market` or `city`.
- STR Excel files in `data/str-imports/` do **not** include Sub-Continent — country map is the automated path.

**Gaps today:** ~787 rows blank (often recent Hilton directory adds, pipeline hotels, or non-STR matches). Running the geography plan script can propose country-derived fills.

---

## 3. Market

**Purpose:** STR commercial market label for benchmarking alignment (e.g. `Mexico Central South`, `Jamaica`, `Brazil Southeast`). Hotel Census column name is `Market` — there is no separate `STR Market` field.

**Source priority**

| Tier | Source | When to use |
|------|--------|-------------|
| 1 | STR Excel `STR Market` | Match census row by `STR Number` (preferred) or Name+City+Country |
| 2 | Steward assignment | Non-STR hotels, pipeline without STR ID |
| — | **Never** | Hilton city, brand directory, or copying `city` |

**Write rules**

- STR import path: `lib/str-census-import/match-excel-to-census.mjs` → `Market` column.
- Default: **fill blank** on STR apply; use `--force` only for deliberate full STR resync.
- ~721 rows missing Market — mostly no STR match or hotels added after STR import.

**Not the same as:** `Dealality Market` (0% populated; steward playbook required per `docs/verified-independent-hotel-census-schema.md`).

---

## 4. Submarket

**Purpose:** STR submarket within a market (e.g. `Dominican Republic Regional`). Hotel Census column name is `Submarket` — there is no separate `STR Submarket` field.

**Source priority**

| Tier | Source | When to use |
|------|--------|-------------|
| 1 | STR Excel `STR Submarket` | Same matching as Market |
| 2 | Steward assignment | When STR row lacks submarket or hotel has no STR ID |
| — | **Never** | Copy `Market`, `city`, or Hilton neighborhood |

**Write rules**

- STR import → `Submarket` column; fill-blank by default.
- ~78% of census rows blank Submarket — many STR rows have submarket in Excel but census was only partially synced; re-run STR plan/apply for matched rows.

---

## Operational workflows

### A. New hotel from brand directory (Hilton, etc.)

1. Write identity, geo, contact, amenities (existing directory contract).
2. Run geography enrichment plan → fill `Region` + `Sub-Continent` from `country`.
3. Leave `Market` / `Submarket` blank until STR match or steward assigns.

### B. STR batch import

1. `node scripts/inventory-hotel-census-for-str-import.mjs`
2. Plan → review CSV → apply for `Market` + `Submarket` (and city/country/name when blank).

### C. Manual / portfolio adds (HE CALA pattern)

Set `Region` from country at row creation (`he-cala-census-apply.js`). Add `Sub-Continent` in the same step going forward.

### D. Geography backfill (all census rows)

```bash
node scripts/plan-hotel-census-geography-enrichment.mjs
node scripts/plan-hotel-census-geography-enrichment.mjs --apply
```

Fill-blank only for `Region` and `Sub-Continent`. Does not touch Market/Submarket.

### E. Market / Submarket backfill (when STR strict match is insufficient)

```bash
node scripts/apply-str-census-market-submarket-fill-blank.mjs
node scripts/apply-hotel-census-market-submarket-fallback.mjs
```

Fallback priority (fill-blank): STR excel by `STR Number` → city+country mode `Market` → single-country `Market`; `Submarket` from STR excel → `city` when `Market` is set.

See also: `docs/hotel-census-location-population-rules.md` for **`Location`** (STR location type — Urban / Resort / …), which is separate from geographic fields above.

---

## 5. Submarket corridor backfill (CALA)

**Purpose:** For CALA census rows, `Submarket` holds **Dealality commercial corridors** (from `lib/radar-buildout/country-configs.js`), replacing STR `*Regional` buckets (e.g. `Dominican Republic Regional` → `Miches / Costa Esmeralda`).

**Not a separate field** — updates go directly to existing `Submarket`.

**Scripts**

```bash
node scripts/audit-cala-regional-submarkets.mjs
node scripts/plan-hotel-census-dealality-submarket.mjs --full-census
node scripts/plan-hotel-census-dealality-submarket.mjs --apply --full-census
```

| Flag | Effect |
|------|--------|
| `--full-census` | All countries in scope; overwrite `*Regional`; normalize STR city labels; `min-confidence=Low`; assign `Other` when no corridor match |
| `--overwrite-regional` | Replace values ending in `Regional` |
| `--normalize-labels` | Normalize city-level STR labels (e.g. `Punta Cana` → full corridor) |
| `--assign-unmapped-other` | Set `Other` when inference finds no corridor |
| `--all-countries` | Include non-CALA rows (without full-census defaults) |
| `--apply` | Write to Airtable |

**Change impact:** **High** — overwrites STR `Submarket` for regional rows; review plan CSV before apply.

---

## Data contract snapshot

| Item | Value |
|------|--------|
| Table | `Hotel Census` |
| Mapping module | `MAP_GEOGRAPHY_ENRICHMENT` in `lib/hotel-census/geography-enrichment-contract.js` |
| Required for automation | `country` |
| Optional | existing `Region`, `Sub-Continent`, `STR Number` (for STR path only) |
| Select / allowed lists | Region: 5 Dealality labels; Sub-Continent: 4 CALA labels |
| STR fields | `Market`, `Submarket` — from licensed Excel only |

---

## Change impact

| Change type | Impact |
|-------------|--------|
| Region / Sub-Continent backfill | **Medium** — affects Brand Explorer region breakdowns when blank rows are filled |
| STR Market/Submarket apply | **High** — overwrites if `--force`; review plan CSV first |
| Dealality Market | **High** — steward-only; not automated here |

---

## Regression checklist

- Brand Explorer footprint region mix (`aggregate-presence-summary.js`)
- Operator footprint by region (`build-operator-census-footprint.js`)
- STR import match counts and conflict CSV
- Curio / Hilton rows: Region = CALA, Sub-Continent matches country
- Mexico rows: Region = CALA, Sub-Continent = `North America`

---

## Known limitations

- `country` map may return `Other` for territories not yet in `region.js` — extend map, do not guess.
- Global census expansion will need additional Sub-Continent labels (e.g. Western Europe) — CALA-only list today.
- `Sub Continent` (space) is not an Airtable field; use `Sub-Continent` only.
