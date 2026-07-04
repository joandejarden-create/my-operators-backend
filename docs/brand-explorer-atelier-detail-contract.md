# Brand Explorer (Atelier) — frozen detail contract

**Purpose:** Step 1 of consolidation — single specification of what the atelier brand detail UI reads from the API. Use this for gap analysis against Brand Setup forms and `api/brand-library.js` mappers.

**Source of truth (code):** `public/js/brand-explorer-gold-detail.js` for the **two** embedded **Brand Setup** tabs (Requirements, Support/Legal). On **`/brand-explorer-combined`**, the eight-tab **API presentation strip** is `public/js/brand-explorer-atelier-from-api.js` (same `brand` payload). The full static **Atelier education** experience opens in a **modal popup** (`#beCombinedBrandDetailPopup`) with the same markup and behavior as `brand-library-atelier-north.html` (`brandDetailPopup` — overlay, close, `brand-detail-popup-loading`, `brand-detail-popup-frame`, `embed=1`, `brand-education-ready` postMessage) when a `brand-education-*.html` URL can be resolved.

**API:** `GET /api/brand-library/brand?brandId=<id|name>` → JSON `{ success, brand }`. The UI uses `data.brand` only.

**Note on dynamic fields:** Several tabs render **every key** on `formValues` or whole objects via `rowsFromObject()`, which only includes **primitive or array** values (nested plain objects are skipped). Those keys are **not fixed in this file** — they follow whatever the API attaches (typically aligned to Airtable column titles after normalization). Inventory those keys by sampling real API responses or tracing `getBrandLibraryBrandById` in `api/brand-library.js`.

---

## Global (above tabs): hero

Rendered by `renderHero(brand)` and `applyHeroStripe(brand)`.

| JSON path | Role |
|-----------|------|
| `brand.logo` | Hero logo (HTTP URL only for image) |
| `brand.name` | Display title (fallback chain with `brandName`) |
| `brand.brandName` | Fallback for title |
| `brand.brandTaglineMotto` | Subtitle tag (competes with `hotelChainScale`, `parentCompany`) |
| `brand.hotelChainScale` | Tag / meta; drives hero stripe color |
| `brand.parentCompany` | Tag / meta |
| `brand.brandPositioning` | Hero statement (truncated in hero) |
| `brand.hotelServiceModel` | Meta card |
| `brand.brandModelFormat` | Meta card |
| `brand.yearBrandLaunched` | Meta card |
| `brand.brandWebsite` | Meta card |

---

## Tab: `profile` — Profile & positioning

Subsections: Identity & classification; Brand story & positioning; Audience & differentiation; Analysis & notes; Brand assets. Optional **Data load notes** if `loadWarnings` present.

### Root `brand` — identity & classification

`name`, `brandName`, `parentCompany`, `hotelChainScale`, `brandArchitecture`, `brandModelFormat`, `hotelServiceModel`, `yearBrandLaunched`, `brandDevelopmentStage`, `brandStatus`, `brandWebsite`

### Root `brand` — brand story & positioning

`brandPositioning`, `brandTaglineMotto`, `brandCustomerPromise`, `brandValueProposition`, `brandPillars`, `companyHistory`

### Root `brand` — audience & differentiation

`targetGuestSegments`, `guestPsychographics`, `keyBrandDifferentiators`, `sustainabilityPositioning`

### Root `brand` — analysis & notes

`brandProfileAnalysis`

### Root `brand` — assets

`logo` (again in subsection as image)

### Root `brand` — load diagnostics

`loadWarnings` — array of strings; if non-empty, shown as “Data load notes”

**Display labels** for many keys come from `PROFILE_LABELS` in the same file (see `public/js/brand-explorer-gold-detail.js`).

---

## Tab: `footprint` — Footprint & distribution

All under `brand.footprint` unless noted. Denote `fp = brand.footprint`.

### Markets & footprint (derived summary)

| Path | Role |
|------|------|
| `fp.regionalDistribution` | Object: region name → `{ hotels, rooms, pipelineHotels, pipelineRooms }` (counts for summary) |
| `fp.priorityCities` | Array or string; city/market count (UI); **mirrored from** `fp.formValues.specificMarkets` on **GET** when `priorityCities` / `formValues.priorityCities` would otherwise be empty (`api/brand-library.js`) |
| `fp.formValues.priorityCities` | Fallback for city count; filled from `specificMarkets` when empty (same GET mirror) |
| `fp.formValues.specificMarkets` | Canonical text from Airtable `Specific Markets/Cities` (Brand Footprint) |

Derived only in UI: region count, city count, coverage label (Broad / Balanced / Focused / Limited).

### Footprint metrics

| Path | Role |
|------|------|
| `fp.totalExistingHotels`, `fp.totalExistingRooms` | Totals table |
| `fp.totalNewBuildHotels`, `fp.totalConversionHotels` | Summed → “Pipeline Hotels” |
| `fp.totalNewBuildRooms`, `fp.totalConversionRooms` | Summed → “Pipeline Rooms” |

### Portfolio distribution tables

Uses `fp.regionalDistribution` plus **root** `brand.hotelChainScale`, `brand.name`, `brand.brandName` for chain/brand rows.

### Location type mix

| Path | Role |
|------|------|
| `fp.locationDistribution` | Object of location type → numeric share |

### Footprint detail (dynamic)

| Path | Role |
|------|------|
| `fp.formValues` | Every key with a value, humanized for labels; several patterns filtered out to avoid duplicating summaries (regional metrics rows, existing/pipeline totals, managed/franchised %, location mix keys, etc.) |

---

## Tab: `economics` — Deal economics

Three blocks; each uses **only top-level primitive (or array) fields** on the object — **nested objects inside these are not walked**.

| Parent | Section title |
|--------|----------------|
| `brand.feeStructure` | Fee structure |
| `brand.dealTerms` | Deal terms |
| `brand.portfolioPerformance` | Portfolio & performance |

Field names = whatever keys the API provides; labels = `humanizeKey(key)` unless the value is yes/no or long text (handled generically).

---

## Tab: `requirements` — Requirements & standards

Parent: `brand.brandStandards` (`std`).

| JSON path | UI group (conceptual) |
|-----------|------------------------|
| `std.lobby`, `std.lobbyDescription`, `std.barBeverage`, `std.fitnessCenter`, `std.pool`, `std.onsiteParking`, `std.meetingEventSpace`, `std.coworking`, `std.grabGo`, `std.minimumRoomSize`, `std.minimumRoomSizeMeters`, `std.brandStandards` | Core spaces & amenities |
| `std.brandFbOutletsRequired`, `std.brandFbOutletsCount`, `std.brandFbProgramType`, `std.brandFbOutletConcepts`, `std.brandFbOutletSize`, `std.brandFbOutletSizeUnit` | Food & beverage |
| `std.brandMeetingSpaceRequired`, `std.brandMeetingRoomsCount`, `std.brandMeetingSpaceSize`, `std.brandCondoResidencesAllowed`, `std.brandHotelRentalProgram` | Meetings & events |
| `std.brandParkingRequired`, `std.brandParkingSpacesCount`, `std.brandParkingProgramType` | Parking & program rules |
| `std.brandSustainability`, `std.brandSustainabilityOther`, `std.brandRequiredAmenities`, `std.brandRequiredAmenitiesOther` | Sustainability & amenities |
| `std.brandCompliance`, `std.brandComplianceOther`, `std.brandQaExpectations`, `std.brandStandardsNotes` | Compliance & QA |

---

## Tab: `owner-fit` — Owner fit & risk

### Project fit (grouped cards)

| Path | Role |
|------|------|
| `brand.projectFit.formValues` | **Dynamic:** mapped keys from `PROJECT_FIT_AIRTABLE_TO_FORM` + checkbox column groups (see `api/brand-library.js`). This is the **default** payload surface for project fit. |
| `brand.projectFit` (keys other than `formValues`) | **Optional:** only present when the GET URL includes **`projectFitExtras=1`** or **`debug=projectFit`** — then loose Airtable scalar columns are merged for the “Project fit (source fields)” atelier section. |

### Sustainability & ESG (root brand)

These are **top-level on `brand`**, not only on the profile tab list:

| Path |
|------|
| `brand.sustainabilityPrograms` |
| `brand.esgReporting` |
| `brand.carbonTracking` |
| `brand.energyEfficiency` |
| `brand.wasteReduction` |

### Loyalty & commercial

| Path | Role |
|------|------|
| `brand.loyaltyCommercial.formValues` | **Dynamic** key/value cards |
| `brand.loyaltyCommercial` | **Dynamic:** keys except `formValues` and `unlinkedFields` (“Loyalty & commercial (additional)”) |

---

## Tab: `support-legal` — Support, legal & commercial

Both panels use **top-level primitive/array fields only** (`rowsFromObject`).

| Parent | Section |
|--------|---------|
| `brand.operationalSupport` | Operational support (subgrouped by label regex: key money, service/communication, governance/disputes) |
| `brand.legalTerms` | Legal terms |

---

## Objects referenced but not fully enumerated in UI

The file defines `NESTED` and `PROFILE_KEYS` for documentation; **`PROFILE_KEYS` is not used by the tab renderer** — the authoritative profile field set is the `identityKeys` / `narrativeKeys` / `audienceKeys` / `notesKeys` lists inside `renderProfile()`.

If the API adds `projectFitDebug` or other nested payloads, they only appear if flattened to primitives at the top level of the parent object per `rowsFromObject` rules.

**See also:** [brand-explorer-atelier-gap-matrix.md](./brand-explorer-atelier-gap-matrix.md) (Step 2 — API / Airtable / Brand Setup mapping and known gaps). [brand-library-brand-vs-atelier.md](./brand-library-brand-vs-atelier.md) (G4 — legacy Brand Library detail vs atelier).

---

## Atelier education tabs (combined explorer) — modal iframe + API strip + gold tabs

**Combined Brand Explorer** (`/brand-explorer-combined`) uses:

1. **Modal popup** (same DOM/CSS/behavior as Atelier North `brandDetailPopup`): `#beCombinedBrandDetailPopup` with overlay, **×** close, wave loading overlay, and `iframe.brand-detail-popup-frame` + `?embed=1` + `{ type: 'brand-education-ready' }` — only when a static `brand-education-*.html` path resolves (demo mock ids + name matches).
2. **Eight-tab strip** on the detail page from **`public/js/brand-explorer-atelier-from-api.js`** — data from `GET /api/brand-library/brand` (mix of Basics/Footprint/Standards/Loyalty fields + static education copy on some tabs; see [gap matrix §10](./brand-explorer-atelier-gap-matrix.md)).
3. **Two Brand Setup tabs** after the divider — **`public/js/brand-explorer-gold-detail.js`** (`Requirements & Standards`, `Support, Legal & Commercial`); footprint / deal economics gold tabs were removed in favor of the Atelier **Footprint & Growth** tab and other atelier shells.

| Surface | Source |
|---------|--------|
| Full mock education (8 tabs, gold visuals) | **Iframe in modal** when URL resolves |
| In-page “Brand explorer presentation” 8 tabs | **`brand-explorer-atelier-from-api.js`** + `brand` JSON |
| “Structured profile & diligence” 2 gold tabs | **`brand-explorer-gold-detail.js`** + `brand` JSON |

### Atelier overview — Airtable checklist (gaps vs static education mocks)

These appear on static education pages (e.g. voco) but are **not** first-class on `brand` today. Until they exist in Airtable **and** are mapped in `api/brand-library.js`, the **iframe** presentation (or Brand Setup forms) will not show them unless they are hand-authored in `brand-education-*.html` or added to the API-driven gold tabs.

| Atelier UI idea | Suggested location | Status on GET `brand` |
|-----------------|-------------------|------------------------|
| Highlight tag chips (e.g. “Premium”, “Conversion-led”) | Brand Basics: multi-select or long text | **Proxy:** `brandPillars` / `keyBrandDifferentiators` chips only |
| “Brand family” (collection within parent) | Brand Basics: new text or single-select | **Gap** — use `brandArchitecture` only as proxy |
| “Brand type” (full-service / select-service wording) | Brand Basics: new field or derive from service model | **Proxy:** `brandModelFormat` + `hotelServiceModel` |
| Typical keys (room count) range | Brand Footprint or Basics: e.g. “Typical keys min / max” | **Gap** |
| “Typical use case” one-liner | Brand Basics: new short text | **Proxy:** trim of `brandPositioning` / `targetGuestSegments` only if you populate those |
| “Guest orientation” / “Relative positioning” snapshot lines | Brand Basics | **Proxy:** `guestPsychographics`, `brandCustomerPromise`; no separate “relative positioning” field |
| Scenario cards (image + title + body) for “where value is strongest” | New linked table or JSON blob on Basics | **Gap** — UI shows placeholder until modeled |
| “Owner outcomes / owner experience” bullet columns | Portfolio & performance + operational support excerpts, or new long text | **Gap** — partial **proxy:** `brandValueProposition`, `companyHistory` |

**Already wired on Overview (no new columns):** `parentCompany`, `brandArchitecture`, `yearBrandLaunched`, `brandWebsite`, `hotelChainScale`, `brandModelFormat`, `hotelServiceModel`, `brandDevelopmentStage`, `brandPositioning`, `brandStatus`, `brandTaglineMotto` (hero elsewhere), `targetGuestSegments`, `guestPsychographics`, `keyBrandDifferentiators`, `brandPillars`, `brandValueProposition`, `companyHistory`, footprint totals and `formValues` (`specificMarkets`, `numberOfMarkets`, `figuresAsOf`, …), loyalty `typicalLoyaltyProgramName`, `totalGlobalMembersMillions`, `typicalLoyaltyRoomsPercent`, etc.

---

## Changelog

| Date | Change |
|------|--------|
| 2026-05-13 | Combined explorer: Atelier `brandDetailPopup` modal + restored API presentation strip (`brand-explorer-atelier-from-api.js`); inline iframe removed. |
| 2026-05-13 | Owner-fit: default `projectFit` is `formValues` only; optional extras query documented. |
| 2026-05-13 | Footprint doc: `priorityCities` GET mirror from `specificMarkets`; requirements: `brandFbOutletSizeUnit`. |
| 2026-05-13 | Initial contract frozen from `brand-explorer-gold-detail.js` (Step 1). |
