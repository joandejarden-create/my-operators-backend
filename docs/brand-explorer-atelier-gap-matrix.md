# Brand Explorer (Atelier) ↔ API ↔ Airtable ↔ Brand Setup — gap matrix (Step 2)

**Goal:** Map every atelier detail surface to the JSON contract from `GET /api/brand-library/brand`, then to Airtable columns and Brand Setup coverage so you can spot missing fields, naming drift, or display-only gaps.

**Primary sources**

| Artifact | Path |
|----------|------|
| Atelier UI contract | `docs/brand-explorer-atelier-detail-contract.md` |
| Combined 8-tab strip (API) | `public/js/brand-explorer-atelier-from-api.js` |
| Brand Setup gold embed (2 tabs) | `public/js/brand-explorer-gold-detail.js` (`TAB_DEFS`: Requirements, Support/Legal) |
| API read/write maps | `api/brand-library.js` (`FORM_TO_AIRTABLE_*`, `F`, `PROJECT_FIT_*`, etc.) |
| Brand Setup UI | `public/brand-setup.html` (save paths keyed off section ids) |
| Legacy vs atelier (G4) | `docs/brand-library-brand-vs-atelier.md` |

**Brand Setup ↔ Airtable (high level)** — from header comment in `api/brand-library.js`:

| Brand Setup area | Airtable table |
|------------------|----------------|
| Brand Basics (incl. website, positioning) | Brand Setup - Brand Basics |
| Sustainability & ESG | Brand Setup - Sustainability & ESG |
| Brand Footprint | Brand Setup - Brand Footprint |
| Project Fit | Brand Setup - Project Fit |
| Portfolio & Performance | Brand Setup - Portfolio & Performance |
| Brand Standards | Brand Setup - Brand Standards |
| Fee Structure / Deal Terms (sections) | Brand Setup - Fee Structure / Deal Terms |
| Operational Support | Brand Setup - Operational Support |
| Legal Terms | Brand Setup - Legal Terms |
| Loyalty & Commercial | Brand Setup - Loyalty & Commercial |

**PATCH endpoints (brand record `rec…`)** — `server.js` / `server.upload-ready.js`:

- `PATCH /api/brand-library/brand/:recordId` — Brand Basics
- `PATCH .../sustainability-esg` — Sustainability & ESG
- `PATCH .../brand-footprint` — Footprint
- `PATCH .../project-fit` — Project Fit
- `PATCH .../portfolio-performance` — Portfolio
- `PATCH .../brand-standards` — Brand Standards
- `PATCH .../fee-structure` — Fee Structure
- `PATCH .../deal-terms` — Deal Terms
- `PATCH .../operational-support` — Operational Support
- `PATCH .../legal-terms` — Legal Terms
- `PATCH .../loyalty-commercial` — Loyalty & Commercial

---

## Legend

| Status | Meaning |
|--------|---------|
| **Aligned** | Atelier reads a key the API fills from Airtable; Brand Setup has a matching control and the same mapping drives PATCH (or read-only derived data documented). |
| **API only** | Present in API payload; atelier does not surface it (optional follow-up: show in atelier or ignore by policy). |
| **Atelier only** | UI expects a path the API does not set (bug or legacy name). |
| **Dynamic** | Keys come from `formValues` or `rowsFromObject` over many Airtable columns — see §Dynamic inventories. |

---

## 1. Hero (global, above tabs)

| Atelier / UI | API path | Airtable | Brand Setup | Status |
|--------------|----------|----------|-------------|--------|
| Logo | `brand.logo` | Brand Basics — `Logo` (and fallbacks in `extractLogoUrl`) | Brand Basics | Aligned |
| Title | `brand.name` | Brand Basics — `Brand Name` | Brand Basics | Aligned |
| Tagline / tag | `brand.brandTaglineMotto`, `hotelChainScale`, `parentCompany` | Basics — `Brand Tagline`, `Hotel Chain Scale`, `Parent Company` | Brand Basics | Aligned |
| Positioning snippet | `brand.brandPositioning` | Basics — `Brand Positioning` | Brand Basics | Aligned |
| Meta: parent, chain scale, service, model, year, website | same keys on `brand` | See `FORM_TO_AIRTABLE_BASICS` | Brand Basics | Aligned |
| Hero stripe color | `brand.hotelChainScale` | `Hotel Chain Scale` | Brand Basics | Aligned (display) |

---

## 2. Tab: Profile & positioning

| Atelier subsection | API path | Airtable (Basics unless noted) | Brand Setup | Status |
|-------------------|----------|-------------------------------|-------------|--------|
| Identity & classification | `brand.brandName`, `parentCompany`, `hotelChainScale`, `brandArchitecture`, `brandModelFormat`, `hotelServiceModel`, `yearBrandLaunched`, `brandDevelopmentStage`, `brandStatus`, `brandWebsite` | `FORM_TO_AIRTABLE_BASICS` values | Brand Basics | Aligned |
| Brand story | `brandPositioning`, `brandTaglineMotto`, `brandCustomerPromise`, `brandValueProposition`, `brandPillars`, `companyHistory` | same map | Brand Basics | Aligned |
| Audience & differentiation | `targetGuestSegments`, `guestPsychographics`, `keyBrandDifferentiators`, `sustainabilityPositioning` | `Target Guest Segments` (array in API), etc. | Brand Basics | Aligned |
| Analysis | `brandProfileAnalysis` | `Brand Profile Analysis` | Brand Basics | Aligned |
| Brand assets | `brand.logo` | Logo | Brand Basics | Aligned |
| Data load notes | `brand.loadWarnings` | n/a (join errors) | n/a | Aligned |

**Note:** `renderProfile` skips duplicate `name` when it equals `brandName`; API sets `name` from Airtable brand name.

---

## 3. Tab: Footprint & distribution

| Atelier section | API path | Airtable | Brand Setup | Status |
|-----------------|----------|----------|-------------|--------|
| Markets summary (region count, coverage) | `footprint.regionalDistribution` | Derived in API from regional columns on **Brand Footprint** (AM/CALA/EU/MEA/APAC patterns) | Brand Footprint | Aligned (derived) |
| City / market count | `footprint.priorityCities` or `footprint.formValues.priorityCities` | Same source as `specificMarkets` — mirrored on **GET** in `api/brand-library.js` | Brand Setup field remains `specificMarkets` | **Aligned (dual keys)** — Airtable column is still `Specific Markets/Cities`; API copies into `priorityCities` when `specificMarkets` is set. **PATCH:** if only `priorityCities` is sent, it is mapped onto `specificMarkets` before write. |
| Totals table | `totalExistingHotels`, `totalExistingRooms`, pipeline sums | Same footprint regional columns | Brand Footprint | Aligned |
| Portfolio distribution | `regionalDistribution` + root `hotelChainScale`, `name`, `brandName` | Footprint + Basics | Footprint + Basics | Aligned |
| Location mix bars | `footprint.locationDistribution` | `Urban - Existing Systemwide Location`, etc. | Brand Footprint (`locationType*`) | Aligned |
| Footprint detail cards | `footprint.formValues.*` | `FOOTPRINT_FORM_TO_AIRTABLE` (geo_*, percents, `figuresAsOf`, `specificMarkets`, …) | Brand Footprint | **Dynamic** — see §3.1 |

### 3.1 `footprint.formValues` — form key ↔ Airtable (from `FOOTPRINT_FORM_TO_AIRTABLE`)

All listed rows are **Aligned** for save/load if the Footprint PATCH includes them (same array in `api/brand-library.js`).

Keys: `geo_na_*`, `geo_cala_*`, `geo_eu_*`, `geo_mea_*`, `geo_apac_*` (existing hotels/rooms, pipeline hotels/rooms per region), `newBuildExperience`, `conversionExperience`, `turnaroundExperience`, `renovationExperience`, `typicalManagedPercent`, `typicalFranchisedPercent`, `locationTypeUrban` … `locationTypeInterstate`, `exitsDeflaggings`, `figuresAsOf`, `numberOfMarkets`, `specificMarkets`.

---

## 4. Tab: Deal economics

Atelier uses `rowsFromObject` on each object: **only top-level scalar/array** keys appear.

### 4.1 `brand.feeStructure`

| API keys | Airtable table | Brand Setup | Status |
|----------|----------------|-------------|--------|
| All keys in `FEE_FORM_TO_AIRTABLE` (e.g. `typicalApplicationFeeMin`, `typicalRoyaltyPercentMin`, `typicalIncentivesOffered`, termination/capital/audit fields, …) | Brand Setup - Fee Structure | Fee Structure / related sections | **Dynamic** — aligned via map; termination trio may **backfill** from Deal Terms (`FEE_STRUCTURE_FIELDS_ALSO_IN_DEAL_TERMS`) |

### 4.2 `brand.dealTerms`

| API keys | Airtable table | Brand Setup | Status |
|----------|----------------|-------------|--------|
| All keys in `DEAL_TERMS_FORM_TO_AIRTABLE` | Brand Setup - Deal Terms | Deal Terms section | **Dynamic** — aligned |

### 4.3 `brand.portfolioPerformance`

| API keys | Airtable table | Brand Setup | Status |
|----------|----------------|-------------|--------|
| All keys in `PORTFOLIO_PERFORMANCE_FORM_TO_AIRTABLE` (incl. `reportTypes` array from column or checkbox columns in `REPORT_TYPES_CHECKBOX_COLUMNS`) | Brand Setup - Portfolio & Performance | Portfolio sections | **Dynamic** — aligned |

**API only (economics-related):** Nested objects inside fee/deal/portfolio are not flattened into the atelier economics tab unless the API adds top-level mirror keys (currently not).

---

## 5. Tab: Requirements & standards

| Atelier label (conceptual) | API path (`brand.brandStandards.*`) | Airtable column(s) on **Brand Setup - Brand Standards** | Brand Setup | Status |
|----------------------------|-------------------------------------|-----------------------------------------------------------|-------------|--------|
| Lobby | `lobby`, `lobbyDescription` | `Lobby` / `Lobby Required`, `Lobby Description` | Brand Standards | Aligned |
| Bar, fitness, pool, parking, meeting, coworking, grab & go | `barBeverage`, `fitnessCenter`, `pool`, `onsiteParking`, `meetingEventSpace`, `coworking`, `grabGo` | Multiple column aliases in GET | Brand Standards | Aligned |
| Room size | `minimumRoomSize`, `minimumRoomSizeMeters` | `Minimum Room Size (sq ft)`, `(sq m)` | Brand Standards | Aligned |
| Narrative | `brandStandards` | `Brand Standards` | Brand Standards | Aligned |
| F&B | `brandFbOutletsRequired`, `brandFbOutletsCount`, `brandFbProgramType`, `brandFbOutletConcepts`, `brandFbOutletSize`, `brandFbOutletSizeUnit` | See `getBrandLibraryBrandById` block ~1086–1095 (`F&B Outlet Size Unit`) | Brand Standards | Aligned |
| Meetings / residences / rental | `brandMeetingSpaceRequired`, `brandMeetingRoomsCount`, `brandMeetingSpaceSize`, `brandCondoResidencesAllowed`, `brandHotelRentalProgram` | Matching columns | Brand Standards | Aligned |
| Parking | `brandParkingRequired`, `brandParkingSpacesCount`, `brandParkingProgramType` | `Parking Required`, counts, `Parking Program` (multi) | Brand Standards | Aligned |
| Sustainability / amenities / compliance / QA | `brandSustainability*`, `brandRequiredAmenities*`, `brandCompliance*`, `brandQaExpectations`, `brandStandardsNotes` | `Sustainability Features`, `Additional Amenities`, `Compliance & Safety`, etc. | Brand Standards | Aligned |

---

## 6. Tab: Owner fit & risk

### 6.1 Project fit — `brand.projectFit.formValues`

Built from **Brand Setup - Project Fit**: scalar columns per `PROJECT_FIT_AIRTABLE_TO_FORM` plus multi-select / checkbox column groups (`PROJECT_FIT_ACCEPTABLE_PROJECT_TYPES_COLUMNS`, `PROJECT_FIT_PRIORITY_MARKETS_COLUMNS`, etc.). Brand Setup PATCH uses the same vocabulary.

**Status:** **Dynamic** — **Aligned** for every form key present in `PROJECT_FIT_AIRTABLE_TO_FORM` and the checkbox column lists in `api/brand-library.js`.

### 6.2 Project fit — “source fields” (`brand.projectFit` minus `formValues`)

**Default `GET /api/brand-library/brand`:** `brand.projectFit` is **`{ formValues }` only** — no duplicate flat Airtable column keys. Atelier’s “Project fit (source fields)” block is therefore **empty** unless extras are enabled.

**Optional extras (audit / legacy integrations):** append **`?projectFitExtras=1`** (or **`?debug=projectFit`**) to the brand GET URL. The API then merges **`fieldsToDisplayObject(raw)`** onto `projectFit` (same keys as before G3). Raw Airtable fields remain in **`brand.projectFitDebug.rawAirtableFields`** when `debug=projectFit`.

**Status:** **Aligned** for Brand Setup + atelier on the curated `formValues` path; loose columns are **opt-in** so orphan Airtable columns do not appear in the default explorer UI.

### 6.3 Sustainability & ESG (root `brand`)

| API path | Airtable (**Brand Setup - Sustainability & ESG**) | Brand Setup | Status |
|----------|---------------------------------------------------|-------------|--------|
| `sustainabilityPrograms` | `Sustainability Programs` | Sustainability & ESG tab | Aligned |
| `esgReporting` | `ESG Reporting` | same | Aligned |
| `carbonTracking` | `Carbon Footprint Tracking` | same | Aligned |
| `energyEfficiency` | `Energy Efficiency Initiatives` | same | Aligned |
| `wasteReduction` | `Waste Reduction Programs` | same | Aligned |

Single-select allowed values for three columns are constrained in PATCH (`SUSTAINABILITY_ESG_SELECT_OPTIONS` in `api/brand-library.js`).

### 6.4 Loyalty & commercial — `brand.loyaltyCommercial`

| Surface | API path | Airtable | Brand Setup | Status |
|---------|----------|----------|-------------|--------|
| Main grid | `loyaltyCommercial.formValues.*` | `LOYALTY_COMMERCIAL_FORM_TO_AIRTABLE` | Loyalty & Commercial section | **Dynamic** — aligned |
| Additional | keys on `loyaltyCommercial` except `formValues`, `unlinkedFields` | Other scalar columns on linked record | Depends on form | **Dynamic** |
| Debug | `unlinkedFields` | Columns with no value | n/a | API only (not shown in atelier) |

---

## 7. Tab: Support, legal & commercial

### 7.1 Operational support — `brand.operationalSupport`

| Source | Airtable | Brand Setup | Status |
|--------|----------|-------------|--------|
| `OPERATIONAL_SUPPORT_FORM_TO_AIRTABLE` | Brand Setup - Operational Support | Operational Support section | **Dynamic** — aligned |
| `typesOfIncentives` | `Incentive Types` (multi) | Incentives | Aligned |
| Service multi-selects | `OPERATIONAL_SUPPORT_SERVICE_MULTI_SELECT` + `OPERATIONAL_SUPPORT_SERVICE_COLUMNS` | Checkbox-style columns per option | Aligned |

### 7.2 Legal terms — `brand.legalTerms`

| Source | Airtable | Brand Setup | Status |
|--------|----------|-------------|--------|
| `LEGAL_TERMS_FORM_TO_AIRTABLE` | Brand Setup - Legal Terms | Legal Terms section | **Dynamic** — aligned |

---

## 8. Consolidated “gaps” list (actionable)

| ID | Issue | Suggested fix |
|----|-------|----------------|
| G1 | ~~Atelier city count vs `specificMarkets` name mismatch~~ | **Mitigated:** GET mirrors `specificMarkets` → `priorityCities` + `formValues.priorityCities`; PATCH accepts `priorityCities` when `specificMarkets` is empty. Remove dual keys later when all clients use one name. |
| G2 | ~~`brandStandards.brandFbOutletSizeUnit` not shown in atelier~~ | **Done:** Food & beverage section shows “F&B outlet size unit” in `brand-explorer-gold-detail.js`. |
| G3 | ~~Extra Project Fit Airtable columns on default API payload~~ | **Mitigated:** default `brand.projectFit` is `{ formValues }` only. Use **`?projectFitExtras=1`** or **`?debug=projectFit`** to attach loose `fieldsToDisplayObject` keys again. |
| G4 | Legacy **Brand Library** detail vs atelier | **Documented:** [brand-library-brand-vs-atelier.md](./brand-library-brand-vs-atelier.md) — tabs, legacy-only UX, fee-field mismatch, retirement checklist. |

---

## 9. Dynamic inventories (copy from code when auditing)

When filling Airtable option lists, use the **exact** constants in `api/brand-library.js`:

- Fee: `FEE_FORM_TO_AIRTABLE`, `FEE_PERCENT_FORM_NAMES`, fee basis normalization.
- Deal terms: `DEAL_TERMS_FORM_TO_AIRTABLE`.
- Portfolio: `PORTFOLIO_PERFORMANCE_FORM_TO_AIRTABLE`, `REPORT_TYPES_CHECKBOX_COLUMNS`.
- Project fit scalars: `PROJECT_FIT_AIRTABLE_TO_FORM`; checkbox groups: `PROJECT_FIT_*_COLUMNS` arrays.
- Footprint: `FOOTPRINT_FORM_TO_AIRTABLE`.
- Loyalty: `LOYALTY_COMMERCIAL_FORM_TO_AIRTABLE`.
- Operational support: `OPERATIONAL_SUPPORT_FORM_TO_AIRTABLE`, `OPERATIONAL_SUPPORT_SERVICE_*`.
- Legal: `LEGAL_TERMS_FORM_TO_AIRTABLE`.
- ESG: `SUSTAINABILITY_ESG_FORM_TO_AIRTABLE`, `SUSTAINABILITY_ESG_SELECT_OPTIONS`.

---

## 10. Brand Explorer Combined — backward pass (Airtable / Brand Setup vs UI)

**Goal:** Everything the combined detail experience *shows as brand-specific* should trace to **Brand Setup → PATCH → Airtable → GET →** `brand` JSON, except deliberate **curated** copy (decide policy per row below). Optional **presentation** rows (`brand.brandExplorer`) override specific slots without changing operational tables — see [brand-explorer-presentation-slots.md](./brand-explorer-presentation-slots.md).

### 10.1 What you already have (tables ↔ API ↔ most of combined UI)

The GET payload already aggregates the tables listed in the header of this doc (Basics, Footprint, Standards, Loyalty & Commercial, Operational Support, Legal Terms, plus Fee / Deal / Portfolio / Project Fit / ESG for other surfaces). **§§1–7** above remain the source for column-level maps.

**Combined page uses that same `brand` object for:**

| Combined area | Primary `brand` paths | Brand Setup / Airtable (summary) |
|---------------|----------------------|----------------------------------|
| Hero (detail + popup fallback) | Basics: `name`, `logo`, `brandPositioning`, `brandTaglineMotto`, `hotelChainScale`, `parentCompany`, `hotelServiceModel`, `brandModelFormat`, `yearBrandLaunched`, `brandWebsite`, meta strip | Brand Basics (+ stripe from chain scale) |
| Atelier **Overview** | Basics + `footprint` (`formValues`, totals, `regionalDistribution`, `priorityCities`), bullets from `keyBrandDifferentiators`, `brandPillars`, `brandValueProposition`, `companyHistory`, `brandProfileAnalysis`, `loyaltyCommercial.formValues` (summary line) | Basics + Footprint + Loyalty & Commercial |
| **Value to Owners** | Long text / bullets: `brandValueProposition`, `companyHistory`, `brandCustomerPromise`, `keyBrandDifferentiators`, `brandProfileAnalysis` | Basics (value promise, differentiators, analysis, history) |
| **Operations & Standards** | `brandStandards.*`, `brandModelFormat`, `hotelServiceModel`, `parentCompany`, `brandWebsite`, `hotelChainScale`, `brandProfileAnalysis` | Brand Standards + Basics |
| **Commercial Engine** | Mostly **static** narrative; API: `explorerDetailCard(..., brand.brandPositioning)` only | *Optional:* long-text “Commercial narrative” on Basics if you want it editable |
| **Loyalty Program** | Mostly **static** IHG-style education; API: `loyaltyCommercial.formValues` via `loyaltyStrengthLine` / KPI row uses mix of static + `lcFv` | Loyalty & Commercial — **Brand Setup already has** `LOYALTY_COMMERCIAL_FORM_TO_AIRTABLE` fields; combined tab does **not** yet render the full `LOYALTY_FORM_ROWS` grid (constant exists in JS but is unused) |
| **Footprint & Growth** | `footprint.*`, `footprint.formValues.*`, `brand.brandProfileAnalysis`, `hotelServiceModel`, `brandDevelopmentStage`, `yearBrandLaunched` | Footprint + Basics |
| **Brand Materials** | **Static** placeholders (file grid, case studies, gallery, locked notices) | **Gap** — needs product decision: linked media table, attachments on Basics, or CMS |
| **Dealality Insight** | `brandProfileAnalysis`, `keyBrandDifferentiators`, `name`; peer cards static | Basics + optional new “competitive set” structured field |
| **Gold: Requirements** | `brandStandards` | Brand Standards — **Aligned** (`rowsFromObject`) |
| **Gold: Support, Legal & Commercial** | `operationalSupport`, `legalTerms` | Operational Support + Legal Terms — **Aligned** |

### 10.2 Gaps to close (so “all fields populate” from Airtable)

| ID | Gap | Where it shows | Suggested path |
|----|-----|------------------|----------------|
| C1 | **Commercial tab** is ~95% fixed copy | `renderCommercialEngine` | Either keep as curated **non-data**, or add Basics (or new table) long-text / JSON **mapped in `api/brand-library.js`** and replace static HTML with `fmtCell` blocks. |
| C2 | **Loyalty tab** IHG narrative, earn/redeem tables, proof cards | `renderLoyaltyProgram` | Same as C1, or bind key numbers only (already partially possible via `loyaltyCommercial.formValues`) and render **`LOYALTY_FORM_ROWS`** in a card grid like Brand Setup. |
| C3 | **Materials tab** — no `brand` fields | `renderBrandMaterials` | New Airtable linked table (e.g. “Brand Explorer Assets”) + GET expansion + Brand Setup upload UI + renderer. |
| C4 | **Momentum / openings / region schematic / property cards** | Footprint tab | Placeholder UX; needs **dated events** + **property examples** (new table(s) or reuse Portfolio/Footprint fields) + API + forms. |
| C5 | **Value to Owners** scenario titles (`VALUE_SCENARIOS`) | Static array | Map to **multi-line Basics** or Project Fit **if** you want owner-facing scenario labels editable. |
| C6 | **Dealality** strength/caution cards | `STRENGTH_PAIRS` / `CAUTION_PAIRS` static | Optional: `brandProfileAnalysis` + structured fields, or separate “diligence narrative” Airtable fields. |
| C7 | **Fee / Deal / Portfolio / Project Fit** on combined strip | Not in the **two** gold tabs anymore | Data still on GET for other pages; if you want them on combined, **re-append gold tabs** or **surface in Atelier tabs** using existing `rowsFromObject` patterns. |

### 10.3 Brand Setup page — implementation order (practical)

0. **Code ↔ Airtable inventory (no tab order):** Run `npm run export-brand-setup-airtable-inventory` — reads `api/brand-library.js` and writes `docs/generated/brand-setup-airtable-inventory.{json,csv}` (`buildBrandSetupAirtableMappingInventory()`). Diff that list against each **Brand Setup** table in Airtable, then add `brandJsonPath` / `uiConsumer` columns in a spreadsheet copy for the full matrix (see `docs/generated/README.txt`).

1. **UI vs §10.1:** For each combined row in §10.1, confirm inputs exist in `public/brand-setup.html` and PATCH in `server.js` / `api/brand-library.js` (most Basics/Footprint/Standards/Loyalty/Support/Legal already do).
2. **Fill gaps:** Add inputs for any **new** Airtable columns (C1–C7), extend `FORM_TO_AIRTABLE_*` + GET normalizers, then read in `brand-explorer-atelier-from-api.js`.
3. **Replace static blocks** incrementally (Commercial → Loyalty numbers grid → Materials when table exists).
4. **QA:** Compare `GET /api/brand-library/brand?brandId=` JSON to each combined tab with a fully populated brand record.

### 10.4 API-only payload (still useful for future tabs)

Objects returned on `brand` but **not** currently driving the combined atelier strip: `feeStructure`, `dealTerms`, `portfolioPerformance`, `projectFit` (default slim), nested fee/deal/portfolio objects, `projectFitDebug`, `loyaltyCommercial.unlinkedFields`, etc. Reuse when you add tabs or inline cards.

---

## 11. Changelog

| Date | Change |
|------|--------|
| 2026-05-13 | `brand.brandExplorer` presentation table + slot doc; §10 goal sentence. |
| 2026-05-13 | §10: Combined backward pass (atelier-from-api + 2 gold tabs), gaps C1–C7, Brand Setup sequencing. |
| 2026-05-13 | Legacy brand-library Support & Legal display (humanize keys, arrays). |
| 2026-05-13 | Legacy `brand-library-brand.html` deal terms aligned with `DEAL_TERMS_FORM_TO_AIRTABLE`. |
| 2026-05-13 | Step 4: legacy fee UI + BDD `brandFit` from `data.brand.projectFit`; G4 doc updated. |
| 2026-05-13 | G3 default `projectFit` slimmed; G4 legacy comparison doc linked. |
| 2026-05-13 | Step 3: atelier shows `brandFbOutletSizeUnit`; contract + matrix updated. |
| 2026-05-13 | Step 2 gap matrix from `api/brand-library.js` + atelier contract + `brand-setup.html` spot-check (`specificMarkets`). |
