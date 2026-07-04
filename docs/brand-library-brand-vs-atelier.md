# Brand Library detail (`brand-library-brand.html`) vs Brand Explorer atelier (`brand-library-atelier-north.html`)

**Purpose (G4):** Record how the legacy brand detail page compares to the atelier (gold) explorer so you can retire `brand-library-brand.html` without losing UX or discover integration bugs.

**Shared data:** Both load **`GET /api/brand-library/brand?brandId=…`** and use the same `brand` payload (nested `footprint`, `feeStructure`, `projectFit.formValues`, etc.).

---

## Tab / information architecture

| Legacy `brand-library-brand.html` tab | Atelier `brand-explorer-gold-detail.js` tab | Notes |
|--------------------------------------|-----------------------------------------------|--------|
| Overview | Split: **hero** (above tabs) + **Profile** tab | Legacy “investment brief” + similar brands live only on Overview. |
| Brand Profile | **Profile** (overlap) + parts of **Owner fit** (ESG block) | Atelier separates ESG to Owner fit; legacy “extended profile” copy is in Brand Profile tab. |
| Portfolio & Distribution | **Footprint** | Both use `brand.footprint` totals, `regionalDistribution`, `locationDistribution`. Legacy adds **SVG/bar charts** in the Portfolio tab. |
| Financial Information | **Deal economics** | **`renderFeeStructure()`** reads **`typical*`** keys from the API (with fallbacks to legacy `applicationFeeMin`-style names). Reservation and training fee rows added where the API provides them. |
| Reqs. & Standards | **Requirements** | Same `brand.brandStandards` object; legacy splits amenities vs room vs narrative; atelier groups by F&B / meetings / etc. |
| Deal Terms Overview | **Deal economics** (`dealTerms`) + **Support, Legal** (legal slice) | Legacy has a dedicated deal-terms tab; atelier merges fee + deal + portfolio under economics and legal under support-legal. |
| Support & Legal | **Support, Legal** | Same `operationalSupport` + `legalTerms` primitives. |
| Differentiators | **Profile** (pillars, differentiators, sustainability positioning) | Legacy isolates pillars / differentiators / sustainability positioning; atelier keeps them in Profile + Standards where relevant. |

---

## Legacy-only UX / features

| Feature | Location | Parity if retiring legacy |
|---------|----------|---------------------------|
| **Print brief** | Overview — `overviewPrintBtn` forces Overview then `window.print()` | Reintroduce print stylesheet + button on atelier or accept loss of one-click print. |
| **Investment brief header** | `renderInvestBriefHeader`, `renderOverviewBrief` — executive layout | Atelier has a simpler hero; decide if marketing layout must be ported. |
| **“Also consider” similar brands** | Overview — fetches `/api/brand-library/brands`, chips linking to other detail pages | Atelier has no similar-brand strip; add if needed for discovery. |
| **Regional / location charts** | Portfolio tab — bar visuals from `footprint.regionalDistribution` / `locationDistribution` | Atelier has tables + `locationMixBars` + distribution toggles; charts are optional enhancement. |
| **`postMessage` to parent** | `brand-library-detail-ready` with `popupToken` | Used when detail opens in iframe/popup; atelier does not emit this — update embedders to listen on atelier route or add equivalent. |

---

## API / field-name caveats

1. **Fee structure (legacy page):** ~~Mismatch~~ **Addressed (2026-05-13):** `brand-library-brand.html` reads `typical*` keys. **Deal terms tab:** **Addressed (2026-05-13):** `renderDealTerms()` reads **`minInitialTermQty`**, **`renewalNoticeQty`**, **`performanceTestRequirement`**, **`pipAtRenewal`**, etc. (same ids as `DEAL_TERMS_FORM_TO_AIRTABLE` in `api/brand-library.js`) with fallbacks to older property names. **Support & Legal tab:** **Addressed (2026-05-13):** `renderSupportAndLegal()` humanizes camelCase **`form`** keys from `OPERATIONAL_SUPPORT_FORM_TO_AIRTABLE` / `LEGAL_TERMS_FORM_TO_AIRTABLE` and joins multi-select arrays for display.
2. **`projectFit` shape:** Default API response is now **`{ formValues }`** only for `brand.projectFit`. Extra Airtable columns (humanized flat keys) appear only with **`?projectFitExtras=1`** or **`?debug=projectFit`** (see G3 in gap matrix). Atelier only needs `formValues` for Project fit cards.
3. **Brand Development Dashboard:** `_fetchBrandData` sets **`brandFit` from `data.brand.projectFit`**. Scoring code that still expects **flat Airtable column names** on `brandFit` (e.g. `Min - Room Count`) should be updated to read **`brandFit.formValues`** (e.g. `idealRoomCountMin`) or call the API with **`projectFitExtras=1`** during a transition period.

---

## Suggested retirement checklist

1. Replace links to `brand-library-brand.html?id=…` with **`/app#/brand-library-atelier`** (or hosted equivalent) + same `id` / `brandId` query pattern as atelier.
2. Confirm embeds that relied on **`brand-library-detail-ready`** are migrated or shimmed.
3. Decide on **print**, **similar brands**, and **charts** — port, defer, or drop explicitly.
4. Re-test **financial** numbers on atelier vs Airtable (not vs legacy page, if legacy was blank).

---

## Changelog

| Date | Change |
|------|--------|
| 2026-05-13 | Support & Legal grids: humanize API form keys, format arrays. |
| 2026-05-13 | Deal terms tab aligned with API (`DEAL_TERMS_FORM_TO_AIRTABLE` field ids + legacy fallbacks). |
| 2026-05-13 | Financial tab aligned with API `typical*` fee keys; BDD `brandFit` reads `data.brand.projectFit`. |
| 2026-05-13 | Initial G4 comparison (read-only audit of `brand-library-brand.html` + atelier). |
