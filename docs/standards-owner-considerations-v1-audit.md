# Standards & Owner Considerations — Pre-Build Audit

**Date:** 2026-05-21  
**Scope:** Audit only (no implementation).  
**Goal:** Inform a Brand Explorer tab that helps owners understand typical brand requirement areas and owner planning considerations—aligned with Deal Readiness Snapshot v1 discipline (neutral, non-advisory, document-style), and explicitly **not** a brand recommendation output.

---

## Executive summary

| Area | Status |
|------|--------|
| **Presentation data (`standards.*` slots)** | Substantial fixture + Airtable seeding exists; **not documented** in `docs/brand-explorer-presentation-slots.md`; **not rendered** in UI |
| **Brand Setup structured standards** | `Brand Setup - Brand Standards` table mapped in `api/brand-library.js`; shown in gold tab **Requirements & Standards** as field grids |
| **Atelier “Operations & Standards” tab** | Live; covers operating model, flexibility bars, operator compatibility, compliance—**different IA** from owner requirement table |
| **Deal Readiness Snapshot patterns** | Reusable renderer (`deal-readiness-snapshot.js`) + print CSS; neutral disclaimer pattern established |
| **Brand fit / scoring / recommendations** | Separate systems (`match-score-server.js`, `api/brand-explorer.js` fit-to-deal, deal-setup radar copy)—must stay **out** of this tab |

**Critical gap:** `standards.intro`, `standards.requirement`, `standards.questions`, `standards.deal_inputs`, `standards.conversion`, `standards.last_reviewed`, and `standards.source_confidence` are loaded into `brand.brandExplorer.blocks` via API but **no tab reads them**.

---

## A. Existing files and routes

| File / route | Purpose | Relevance | Reuse / modify / leave alone |
|--------------|---------|-----------|------------------------------|
| `public/brand-explorer-combined.html` | Primary Brand Explorer (list + detail popup, unified tabs) | **Primary mount point** for new tab | **Modify** — add tab panel shell / script hook |
| `public/js/brand-explorer-atelier-from-api.js` | 8 atelier tabs + appends 2 gold tabs; reads `brand.brandExplorer.blocks` | **Where new tab renderer belongs** | **Modify** — add `ATELIER_TAB_DEFS` entry + `renderStandardsOwnerConsiderations()` |
| `public/js/brand-explorer-gold-detail.js` | Gold tabs: Requirements & Standards, Dev. Support & Legal | Raw `brandStandards` field dump | **Leave alone** initially (or later rename/clarify vs new tab) |
| `public/css/brand-explorer-atelier-oe-panels.css` | Atelier section layout (`.oe-section`, cards, grids) | Tab content styling | **Reuse** for in-tab layout |
| `public/css/brand-explorer-gold-surface.css` | Gold embed panels | Secondary tab styling | **Reuse** only if content stays in gold embed |
| `public/css/brand-explorer-atelier-list-parity.css` | List/cards/filters | List page only | **Leave alone** |
| `public/brand-explorer.html` | Standalone explorer (older) | Secondary entry | **Leave alone** unless parity required |
| `public/brand-library-brand.html` | Legacy brand detail (multi-tab) | Reference for `renderBrandStandards` | **Leave alone** (retirement candidate per `docs/brand-library-brand-vs-atelier.md`) |
| `public/brand-library-compare.html` | Side-by-side brand comparison | Uses `brandStandards` rows, not presentation slots | **Leave alone** for v1 |
| `public/brand-setup.html` | Brand Setup form (incl. Brand Standards section) | Data entry for structured standards | **Leave alone** v1; optional later editorial workflow |
| `public/brand-education-atelier-north.html` | Static atelier education reference | IA reference (`Operations & Standards`) | **Leave alone** (reference only) |
| `public/deal-readiness-snapshot.html` | Standalone DRS print page | Document output pattern | **Reuse patterns**, not route for brand tab |
| `public/js/deal-readiness-snapshot.js` | Two-page doc renderer + `OUTPUT_NOTE` disclaimer | **Style/discipline reference** | **Reuse** disclaimer + page structure ideas; **do not** import deal scoring |
| `public/css/deal-readiness-snapshot.css` | Light paper document, print rules | Print/document CSS | **Adapt** subset if tab gets print/export (optional v1.1) |
| `public/new-deal-setup.html` | Deal setup + DRS modal embed | DRS integration reference | **Leave alone** |
| `api/brand-library.js` | `GET /api/brand-library/brand` — merges Brand Setup + `brandExplorer.blocks` | **Data API for tab** | **Reuse**; optional small helper for `standards.*` parsing |
| `api/brand-explorer.js` | List brand, get brand modules, **POST fit-to-deal** | Fit scoring (not presentation) | **Leave alone** for this tab |
| `api/match-score-server.js` | Deal–brand match score, `computeRecommendedBrand` | Recommendation engine | **Keep out** of new tab |
| `api/deal-readiness-review.js` | Deal readiness scoring | Deal output only | **Leave alone** |
| `server.js` | Routes: `/brand-explorer-combined`, `/api/brand-library/*`, redirects `/brand-library` → combined | Routing | **Modify** only if new standalone HTML route (unlikely v1) |
| `public/app.js` | App shell: `/brand-explorer-combined` route | Shell navigation | **Leave alone** |
| `docs/brand-explorer-presentation-slots.md` | Slot key catalog for presentation table | Missing `standards.*` | **Modify** — document slots when implementing |
| `docs/brand-explorer-atelier-detail-contract.md` | Gold tab field contract | Documents `requirements` tab fields | **Modify** — add new tab contract |
| `docs/brand-library-brand-vs-atelier.md` | Legacy vs atelier mapping | Context | **Leave alone** |
| `fixtures/brand-explorer-presentation-standards-owner-table-template.json` | Generic 8-area owner table | **v1 content template** | **Reuse** |
| `fixtures/brand-explorer-presentation-standards-*.json` | Brand-specific standards copy | Test/seed data | **Reuse** for QA brands |
| `fixtures/brand-explorer-presentation-*-full.json` | Full Choice tier-1 bundles incl. `standards.*` rows | Bulk seeded content | **Reuse** |
| `scripts/apply-standards-owner-table-all-brands.mjs` | Seeds template to brands missing `standards.requirement` | Data ops | **Reuse** |
| `scripts/apply-brand-explorer-presentation-fixture.mjs` | Push fixtures to Airtable | Data ops | **Reuse** |
| `scripts/lib/choice-explorer-full-builder.mjs` | Generates `standards.*` rows in full explorer fixtures | Content generation | **Reuse** for Choice brands |
| `scripts/build-radisson-blu-standards-fixture.mjs` | Radisson Blu standards fixture builder | Content | **Reuse** |
| `scripts/create-brand-setup-brand-explorer-presentation-table.mjs` | Table creation | Infra | **Leave alone** |
| `lib/brand-explorer-footprint-trust.js` | Census trust labels | Footprint only | **Leave alone** |
| `public/js/brand-explorer-census-metrics.js` | Footprint metrics | Footprint only | **Leave alone** |
| `public/js/brand-explorer-favorites.js` | Favorites API client | List feature | **Leave alone** |
| `api/brand-explorer-favorites.js` | Favorites CRUD | List feature | **Leave alone** |

### Routes (HTTP)

| Route | Serves |
|-------|--------|
| `/brand-explorer-combined` (app shell: `/app#/brand-explorer-combined`) | Combined explorer |
| `/brand-explorer`, `/brand-explorer.html` | Legacy standalone |
| `/brand-library` → redirect | Combined |
| `/brand-library-brand` | Legacy detail |
| `/brand-library-compare` | Compare tool |
| `GET /api/brand-library/brand?brandId=` | Detail payload (+ `brandExplorer`) |
| `GET /api/brand-library/brands` | List |
| `PATCH /api/brand-library/brand/:id/brand-standards` | Brand Setup write |
| `GET/POST /api/brand-explorer/*` | Alternate API (fit-to-deal, modules) |
| `/deal-readiness-snapshot.html` | Deal output (reference) |

---

## B. Existing data model

### Airtable tables (Brand Setup / Explorer)

| Table | Role for this output |
|-------|----------------------|
| `Brand Setup - Brand Basics` | Parent link for all child tables + presentation rows |
| `Brand Setup - Brand Standards` | Structured amenity/F&B/meeting/parking/QA fields → `brand.brandStandards` |
| `Brand Setup - Brand Explorer Presentation` | Slot-keyed copy → `brand.brandExplorer.blocks[]` |
| `Brand Setup - Project Fit` | Conversion, room range, red flags—not standards table |
| `Brand Setup - Deal Terms` | PIP, term—referenced in economics / diligence, not owner table |
| `Brand Setup - Operational Support` | Fallback for ops tab compliance cards |
| `Brand Explorer - Modules` (optional) | DNA / owner lens overrides (`api/brand-explorer.js`) |
| `Brand Explorer - Updates` (optional) | News strip |

### `brand.brandStandards` fields (API → gold Requirements tab)

From `api/brand-library.js` (`brandStandardsData`):

| Theme | API keys / Airtable columns |
|-------|-----------------------------|
| Core amenities | `lobby`, `lobbyDescription`, `barBeverage`, `fitnessCenter`, `pool`, `onsiteParking`, `meetingEventSpace`, `coworking`, `grabGo` |
| Guestroom | `minimumRoomSize`, `minimumRoomSizeMeters`, `brandStandards` (narrative) |
| F&B | `brandFbOutletsRequired`, `brandFbOutletsCount`, `brandFbProgramType`, `brandFbOutletConcepts`, `brandFbOutletSize`, `brandFbOutletSizeUnit` |
| Meetings | `brandMeetingSpaceRequired`, `brandMeetingRoomsCount`, `brandMeetingSpaceSize`, `brandCondoResidencesAllowed`, `brandHotelRentalProgram` |
| Parking | `brandParkingRequired`, `brandParkingSpacesCount`, `brandParkingProgramType` |
| Sustainability / amenities | `brandSustainability`, `brandSustainabilityOther`, `brandRequiredAmenities`, `brandRequiredAmenitiesOther` |
| Compliance / QA | `brandCompliance`, `brandComplianceOther`, `brandQaExpectations`, `brandStandardsNotes` |

### `brand.brandExplorer.blocks` — `standards.*` slots (in fixtures, **not in slots doc**)

| Slot key | Purpose (from fixtures) | In UI today |
|----------|-------------------------|-------------|
| `standards.source_confidence` | Directional / validation label (Courtyard sample) | **No** |
| `standards.last_reviewed` | Review date hint | **No** |
| `standards.intro` | Tab intro / disclaimer framing | **No** |
| `standards.requirement` | **Multi-row** owner table: Title = area name; Body = structured lines | **No** |
| `standards.conversion` | Conversion / PIP planning paragraph | **No** |
| `standards.questions` | Owner confirm-with-brand checklist | **No** |
| `standards.deal_inputs` | Deal context chips (room count, conversion, etc.) | **No** |

**`standards.requirement` body shape** (convention in fixtures):

```
Typical consideration: …
Owner planning consideration: …
Typical status: Typically Expected | May Apply | Confirm with brand
Notes to confirm: …
Source confidence: … (optional, Courtyard sample)
```

### Coverage vs product field wishlist

| Desired theme | Existing support |
|---------------|------------------|
| Brand standards (general) | `brandStandards` narrative + `standards.intro` |
| F&B requirements | Brand Standards table + `standards.requirement` titles |
| Market / retail | `grabGo` + requirement row “Market / Retail” |
| Lobby / public space | `lobby`, `lobbyDescription` + requirement row |
| Guestroom prototype | Room size fields + requirement row |
| Fitness / amenities | Amenity checkboxes + requirement row |
| Technology / systems | Notes + `operations.model.technology` (ops tab) + requirement row |
| Loyalty / distribution | **Separate tab** (`atelier-loyalty`, Loyalty & Commercial table) |
| Signage / exterior | Requirement row only (not structured column) |
| Training / QA | `brandQaExpectations` + requirement row + ops compliance |
| Procurement / FF&E | Mentioned in requirement **body text** only |
| Conversion flexibility | `operations.flexibility.conversion` + `standards.conversion` |
| Design story / local identity | Brand Basics positioning/pillars—not standards slots |
| PIP / CapEx implications | Deal Terms + economics presentation slots |
| Brand approval process | Phrase in requirement bodies (“brand design review”) |
| Source confidence | `standards.source_confidence` slot (sample only) |
| Notes / caveats | `Notes to confirm` in body; `brandStandardsNotes` |

### Missing for v1 (if structured, not prose-in-Body)

- Dedicated Airtable columns per requirement area (optional; v1 can stay **Body parsing**).
- Documented slot keys in `brand-explorer-presentation-slots.md`.
- UI parser/renderer for multi-line requirement bodies.
- Global neutral **output disclaimer** block on Brand Explorer (DRS has `OUTPUT_NOTE` in JS).
- Loyalty/distribution **inside** this tab (should remain cross-linked, not duplicated).

---

## C. Existing UI structure

### Combined Brand Explorer tab order (`brand-explorer-atelier-from-api.js`)

1. Overview  
2. Value to Owners  
3. **Operations & Standards** (`atelier-ops`) — operating model, standards philosophy, flexibility indicators, operator compatibility, compliance cards  
4. Commercial Engine  
5. Economics & Obligations  
6. Loyalty Program  
7. Footprint & Growth  
8. Brand Materials  
9. Dealality Insight  
10. **Requirements & Standards** (gold `requirements`) — Brand Setup field grids  
11. **Dev. Support & Legal** (gold `support-legal`)

**Rendering:** `mountIntoRoot()` builds `#brandTabs` + `#brandPanels` from `combinedTabRowDefs()`; atelier panels use `.be-atelier-tab-panel` + `data-atelier-tab`; gold panels wrapped in `.be-atelier-gold-embed`.

**Where to add “Standards & Owner Considerations”:**

- **Recommended:** New atelier tab id e.g. `atelier-standards-owner` inserted **after** `atelier-ops` or **replacing** gold `requirements` visibility for end users (product decision).
- **Not recommended:** Only extending `atelier-ops`—already dense and mixes “how brand operates” with “what owner must confirm.”
- **Not recommended:** Only extending gold `requirements`—reads as internal data dictionary, not owner planning table.

### Tab pattern reuse

| Pattern | Source |
|---------|--------|
| Section + hint + cards | `.oe-section`, `.oe-section-hint`, `explorerDetailCard()` |
| Multi-row presentation | `explorerCardRowsForSlot()`, `explorerMergedBody()` |
| Checklist bullets | `explorerParagraphs()` / line-split Body |
| Print/document | DRS `.drs-page`, `.drs-sheet` (separate concern) |

### Deal Readiness Snapshot alignment

| DRS element | Applicable to standards tab |
|-------------|----------------------------|
| Page 1 narrative / Page 2 technical | Map to: **Page 1** = intro + requirement area summaries; **Page 2** = questions + deal inputs + conversion + link to Brand Setup fields |
| `OUTPUT_NOTE` non-advisory disclaimer | **Add equivalent** at top of tab |
| Context-aware scoring | **Do not port** (deal-specific) |
| Print-ready CSS | Optional export button later using `deal-readiness-snapshot.css` tokens |

---

## D. Existing recommendation / advisory language to avoid

**Rule for new tab:** Use “typically expected,” “may apply,” “confirm with brand,” “subject to brand approval,” etc. Do not import match-score or fit-to-deal copy.

| Location | Language | Action for new tab |
|----------|----------|-------------------|
| `public/brand-explorer-combined.html` (~915) | “shortlist with confidence” | **Keep out** of tab; optional marketing page edit later |
| `public/js/brand-explorer-atelier-from-api.js` (~1646) | “shortlist the property faster” (commercial static) | **Unrelated** — commercial tab |
| `public/js/brand-explorer-atelier-from-api.js` (~4216) | “Before Shortlisting, Evaluate” (Dealality Insight) | **Do not mirror** in standards tab |
| `operations.operator_compat.fit` slot + card label **“Fit”** | Implies brand–operator fit scoring | **Avoid** in standards tab; ops tab already uses “Fit” for operator compatibility |
| `fixtures/brand-explorer-presentation-*` (`insight.*`, `overview.scenario.*`) | “Strong fit when… Weak fit when…” | **Do not reuse** in standards renderer |
| `scripts/lib/choice-explorer-full-builder.mjs` | Same strong/weak fit in generated insight bodies | **Content pipeline** — exclude from standards slot generator |
| `api/match-score-server.js` | `computeRecommendedBrand`, “Brand Standards Compatibility” score | **Separate feature** — no UI in Brand Explorer combined today |
| `api/brand-explorer.js` `fit-to-deal` | Dimension scores 0–100 | **Separate API** — not mounted on combined page |
| `public/new-deal-setup.html`, `public/deal-setup.html` | “What is a Good Fit,” “lower priority” (importance radar) | **Deal-owner profile** — not brand explorer |
| `api/deal-readiness-review.js` | `workflowRecommendation`, `scoreImprovementPlan` | **Deal output only** |
| `public/brand-review.html` | “Best balance… Strong loyalty…” | Legacy review UI — **leave alone** |
| `public/my-deals.html` | Match score, “Major gaps…” | Deals workspace — **leave alone** |
| `materials.caseStudy` bodies | “why the brand fit” | Case studies — **not standards tab** |

**Acceptable elsewhere (do not copy tone into standards tab):** “illustrative,” “typical,” “confirm with brand,” “directional example,” “not equivalency claims” (already used in insight/footprint hints).

---

## E. Proposed v1 architecture

### Recommended approach

1. **Content:** Continue using **`Brand Setup - Brand Explorer Presentation`** with existing `standards.*` slot keys (no new child table for v1).
2. **UI:** Add **one new atelier tab** “Standards & Owner Considerations” that renders only presentation slots (+ optional fallback snippet from `brand.brandStandards` narrative when intro empty—**labeled as Brand Setup fields, not requirements**).
3. **Do not** add brand-vs-brand ranking, complexity labels, or deal fit scores to this tab.
4. **Sample data:** Use `fixtures/brand-explorer-presentation-standards-owner-table-template.json` + brand-specific fixtures for QA; run `apply-standards-owner-table-all-brands.mjs` for breadth.
5. **Document** all `standards.*` keys in `docs/brand-explorer-presentation-slots.md`.
6. **Defer:** Child table per requirement area; PATCH editor in Brand Setup UI; standalone print route (unless required day one).

### Not recommended for v1

- New Airtable table for requirement areas (higher ops burden).
- Replacing gold Requirements tab without product sign-off (may still help internal users).
- Merging into Operations & Standards (confuses operating model with owner checklist).
- Wiring `fit-to-deal` or match score into tab.

### Relationship to Deal Readiness Snapshot

- **Same discipline:** neutral framing, disclaimer, two-tier information density (summary table → diligence checklist).
- **Different entity:** brand-level typicals, not deal readiness score.
- **Shared assets:** disclaimer wording pattern from `deal-readiness-snapshot.js`; optional shared typographic tokens from `deal-readiness-snapshot.css` if print export is required.

---

## F. Recommended implementation plan

### Phase 0 — Product / IA (no code)

1. Confirm tab label: **“Standards & Owner Considerations”** vs rename gold **“Requirements & Standards”** to avoid collision.
2. Confirm tab position in nav (after Operations & Standards vs before Economics).
3. Confirm whether gold Requirements tab stays visible for admins or is hidden on combined UX.
4. Approve disclaimer text (DRS-style, adapted for brand-level typicals).

### Phase 1 — Data & docs

1. Add `standards.*` section to `docs/brand-explorer-presentation-slots.md` (all keys + body format + multi-row rules).
2. Update `docs/brand-explorer-atelier-detail-contract.md` with new tab contract.
3. Run `apply-standards-owner-table-all-brands.mjs` (dry-run, then apply) for brands missing rows.
4. Spot-check 2–3 brands (generic template, Radisson Blu, Courtyard sample) in Airtable.

### Phase 2 — Renderer (core)

1. In `brand-explorer-atelier-from-api.js`:
   - Add `ATELIER_TAB_DEFS` entry + icon.
   - Implement `renderStandardsOwnerConsiderations(brand)`:
     - Disclaimer block (static string).
     - `standards.intro`, `standards.last_reviewed`, `standards.source_confidence` (if present).
     - Table/cards from **all** `standards.requirement` rows (parse Body lines; display Title as area).
     - `standards.conversion`, `standards.questions`, `standards.deal_inputs`.
   - Register in `buildAtelierPanelsHtml` map.
2. Add minimal CSS in `brand-explorer-atelier-oe-panels.css` for requirement table (status chip, section labels)—or reuse `explorer-detail-card` grid.
3. Empty state: neutral copy when no `standards.requirement` rows (“No planning checklist published—confirm requirements with brand and disclosure documents”).

### Phase 3 — Content quality

1. Replace generic template rows for priority brands with brand-specific fixtures (`standards-radisson-blu.json`, full JSON bundles).
2. Audit generated Choice copy in `choice-explorer-full-builder.mjs` for banned phrases (“Strong fit,” “Weak fit”).
3. Add `standards.source_confidence` to template where editorial status is directional.

### Phase 4 — QA & guardrails

1. Manual QA on `/brand-explorer-combined` for brands with/without presentation rows.
2. Grep guardrail: no “recommend,” “best fit,” “shortlist,” “priority,” “proceed” in new renderer strings.
3. Verify API still returns blocks when presentation table missing (graceful empty state).
4. Regression: existing tabs unchanged; gold tabs still load.

### Phase 5 — Optional follow-ups (post-v1)

1. Print/export using DRS CSS patterns.
2. Link from requirement rows to Brand Setup PDF/materials slots.
3. Brand Setup subform to edit presentation rows (PATCH API).
4. Structured child table if Body parsing becomes limiting.

---

## Appendix: `standards.requirement` area titles in template

From `fixtures/brand-explorer-presentation-standards-owner-table-template.json`:

1. F&B / Bistro  
2. Market / Retail  
3. Lobby / Public Space  
4. Guestroom Standards  
5. Fitness  
6. Signage / Exterior  
7. Technology / Systems  
8. Training / QA  

Radisson Blu fixture adds areas such as Meetings & Events, Loyalty program participation, etc., via extra `standards.requirement` rows.

---

*End of audit.*
