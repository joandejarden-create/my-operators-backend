# Brand Explorer ↔ Hotel Census — Phase 1 plan

**Goal:** Power Brand Explorer footprint metrics from **Deal Capture Platform** `Hotel Census`, while Brand Explorer UI and Brand Setup remain on **Deal Capture MVP** (`AIRTABLE_BASE_ID`).

**Constraints for Phase 1**

- **Read-only** Airtable from application code (no creates/updates/deletes).
- **No AI enrichment.**
- **Do not change** Radar pages or `/api/operators-by-brand-region` behavior.
- **Exact** affiliation matching only — no `contains` / fuzzy search on `{Affiliation}`.

---

## Prerequisites (order of operations)

### Step 0 — Airtable (manual, Platform base)

Create table **`Brand Alias Mapping`** on **Deal Capture Platform** (`AIRTABLE_BASE_ID_ALT`) with columns:

| Column | Suggested type | Purpose |
|--------|----------------|---------|
| **Canonical Brand Name** | Single line | Standard name for rollups / Brand Explorer request |
| **Alias / Source Brand Name** | Single line | Exact value(s) stored in `Hotel Census`.`Affiliation` |
| **Parent Company** | Single line | Optional scope; blank = any parent |
| **Active** | Checkbox | Only `Active = true` rows are loaded |
| **Match Confidence** | Single select or text | Metadata returned in API (not used to filter in Phase 1) |
| **Notes** | Long text | Ops documentation |

**Canonical naming:** **Canonical Brand Name** = Dealality / Brand Explorer display name (e.g. `Courtyard by Marriott`). **Alias / Source Brand Name** = exact `Hotel Census`.`Affiliation` strings (e.g. `Courtyard`). Do not use census short names as canonical when the display name is longer.

Seed via `node scripts/ensure-brand-alias-mapping-table.mjs` then `node scripts/seed-brand-alias-mapping.mjs` (fixture: `fixtures/brand-alias-mapping-phase1-seed.json`).

| Canonical Brand Name | Alias / Source Brand Name | Parent Company | Active |
|----------------------|---------------------------|----------------|--------|
| Courtyard by Marriott | Courtyard | Marriott International | ✓ |
| Courtyard by Marriott | Courtyard by Marriott | Marriott International | ✓ |
| AC Hotels by Marriott | AC Hotels by Marriott | Marriott International | ✓ |
| AC Hotels by Marriott | AC Hotel by Marriott | Marriott International | ✓ |
| AC Hotels by Marriott | AC Hotels | Marriott International | ✓ |

`Hotel Census` already has **15,635** rows; open-brand metrics use `status = 'Open'`. Branded rollups **exclude** `Affiliation = 'Independent'` (exact).

### Step 1 — Brand alias resolution (code, read-only)

Module: `lib/hotel-census/brand-alias-resolve.js`

**Resolve canonical name from request**

1. `requestedBrand` = query `brand` or name resolved from MVP `brandId` (future).
2. Load all **Active** rows from `Brand Alias Mapping`.
3. If any row has **Canonical Brand Name** equal to `requestedBrand` (trim-exact) → canonical = that name.
4. Else if any row has **Alias / Source Brand Name** equal to `requestedBrand` (trim-exact) → canonical = that row’s **Canonical Brand Name** (Explorer often uses a long MVP name that is stored as an alias).
5. Collect every **Alias / Source Brand Name** for the resolved canonical where **Active** and **Parent Company** is blank or equals optional `parentCompany` query param.
6. If **no alias rows** exist for the resolved canonical (or step 3–4 failed entirely), **fallback:** affiliation matchers = `[requestedBrand.trim()]`, `warnings[]` includes `NO_ALIAS_TABLE_MATCH`.

**Match census**

- Include census row iff `Affiliation` is **exactly** (after trim) one of the affiliation matchers.
- Exclude row if `Affiliation` (trim) === `Independent`.
- Open metrics: `status` (trim) === `Open`.
- Pipeline metrics: `status` (trim) === `Pipeline` (and optional `project_phase` breakdown later).

**No** `SEARCH()`, **no** substring, **no** `includes()` on affiliation in Phase 1.

### Step 2 — Summary endpoint

`GET /api/brand-presence-summary`

Query: `brand` (required), `parentCompany` (optional).

Response includes: metrics, country/region/chain-scale/location mixes, `source` metadata, `warnings`, `alias` block (canonical, matchers used, rows matched).

Implementation: `api/brand-presence-summary.js` + `lib/hotel-census/aggregate-presence-summary.js`.

### Step 3 — Merge into Brand Library (implemented)

Behind env `BRAND_EXPLORER_CENSUS_METRICS=1`, `GET /api/brand-library/brand` attaches `brand.censusSummary` (read-only; does not overwrite `brand.footprint`). Helper: `lib/hotel-census/build-brand-census-summary.js`. Test: `node scripts/test-brand-library-census-summary.mjs` with flag set in env.

### Step 4 — Tests

- `node scripts/test-brand-presence-summary.mjs` (read-only; requires ALT base + alias table seeded).
- Pilot brands: **Courtyard by Marriott** (canonical stays display name; matchers include `Courtyard` + `Courtyard by Marriott`); **AC Hotels by Marriott** → multiple exact affiliations.

---

## Architecture choice

**D-lite:** Read Platform census at request time; **Brand Alias Mapping** on Platform base; MVP Footprint remains fallback. No sync job in Phase 1.

---

## Census fields used (existing)

| Code field | Airtable column |
|------------|-----------------|
| `affiliation` | `Affiliation` |
| `parentCompany` | `Parent Company` |
| `status` | `status` |
| `rooms` | `rooms` |
| `country` | `country` |
| `city` | `city` |
| `market` | `Market` |
| `chainScale` | `Chain Scale` |
| `locationType` | `Location` |
| `projectPhase` | `project_phase` |

**Dealality region:** derived from `country` via `lib/hotel-census/region.js` (same country lists as Radar), not raw `Region`.

---

## Phase 1B — Governance & QA (implemented)

### Optional Hotel Census fields (Platform base)

| Column | Type | Behavior in code |
|--------|------|------------------|
| **Include in Brand Explorer** | Checkbox | **Phase 1B:** include row when checked **or** blank/null; **exclude only** when explicitly `false`. If column missing, no filter (same as Phase 1). |
| **Data Confidence** | Single select: High, Medium, Low, Needs Review | Exposed as `dataConfidenceBreakdown` on `censusSummary`; **not** used as a filter yet. |

Env overrides: `AIRTABLE_CENSUS_INCLUDE_IN_BRAND_EXPLORER_FIELD`, `AIRTABLE_CENSUS_DATA_CONFIDENCE_FIELD`.

**Ops — add fields (idempotent):**

```bash
node scripts/ensure-hotel-census-governance-fields.mjs
```

**QA — MVP footprint vs census (read-only CSV):**

```bash
node scripts/audit-brand-explorer-census-coverage.mjs
```

Output: `reports/brand-explorer-census-coverage.csv`

**Future (after QA):** require `Include in Brand Explorer = true` (stop counting blank as include). Do not enable until ops have backfilled checkboxes.

---

## Phase 1C — Alias coverage & reconciliation (implemented)

**Goal:** Expand **Brand Alias Mapping** safely for active brands that still show `fallbackRecommended: yes` in QA.

| Step | Command | Output |
|------|---------|--------|
| QA coverage | `node scripts/audit-brand-explorer-census-coverage.mjs` | `reports/brand-explorer-census-coverage.csv` |
| Propose aliases (no writes) | `node scripts/propose-brand-aliases-from-coverage.mjs` | `reports/proposed-brand-aliases.json` |
| Human review | Edit copy → `reports/proposed-brand-aliases-reviewed.json` with `"Approved": true` | — |
| Seed approved only | `node scripts/seed-reviewed-brand-aliases.mjs` | Brand Alias Mapping only |
| Seed known Choice/Radisson fixture | `node scripts/seed-brand-alias-mapping.mjs --choice-radisson` | `fixtures/brand-alias-mapping-choice-radisson-seed.json` |

**Rules:** exact affiliation matching only; proposals use MVP display name + **explicit** safe short rules (Park Inn → Park Inn, Ascend Hotel Collection → Ascend, strip `(Choice)` suffix, etc.). No contains matching, no parent-company-as-alias, no automatic census guessing.

**Parent company:** MVP `Choice Hotels International` normalizes to alias table `Choice Hotels` for matching (code change in `normalizeParentCompanyKey`).

---

## Out of scope (Phase 1 / 1B)

- Writing alias or census rows from the app (except ops Metadata scripts run manually)
- `Include in Radar` column / Radar behavior changes
- Filtering census by Data Confidence
- Service model mix (no reliable census column)
- Broad affiliation matching
- Radar UI changes
