# Operator Explorer ↔ Operator DNA Profile — Consolidation Audit

**Date:** 2026-05-30  
**Status:** Prototype only — live Operator Explorer unchanged.

## Production path today

| Step | Route / file | Role |
|------|----------------|------|
| 1 | `/operator-explorer` → `public/operator-explorer.html` | **Live list / directory** (sidebar nav) |
| 2 | Click operator tile | Opens iframe popup → `operator-explorer-gold-mock.html?id=rec…&embed=1` |
| 3 | Alternate (legacy) | `operator-explorer-detail.html` — **not** the popup path from the list |

**Canonical live detail experience:** `operator-explorer-gold-mock.html` + `operator-explorer-gold-mock-data.js`.

## Parallel detail pages (do not merge yet)

| Page | Data source | Notes |
|------|-------------|--------|
| `operator-explorer-gold-mock.html` | `GET /api/intake/third-party-operators/:id` (+ list row) | Richest bindings: prefill, `explorerProfileJson`, Master-linked tables, tab panels |
| `operator-explorer-detail.html` | Same detail API, with legacy fallback `GET /api/operator-explorer/operator` | Older shell; more inline JS |

**Recommendation:** Treat **gold-mock** as the source of truth for field bindings when evolving Operator DNA Profile. Retire `operator-explorer-detail.html` only after DNA profile is validated and traffic is redirected.

## Prototype (future canonical detail)

| Route | Files |
|-------|--------|
| `/operator-dna-profile.html` | `operator-dna-profile.html`, `operator-dna-profile.js`, `operator-dna-profile.css`, **`operator-dna-view-model.js`** |

Same detail API as gold-mock: `GET /api/intake/third-party-operators/:id`.

## Product architecture

- **Operator Explorer** — discovery / list (unchanged).
- **Operator DNA Profile** — operator intelligence profile (10 DNA sections, hero + tabs).
- **Operator Alignment Snapshot** — deal-specific fit; DNA profile shows **Alignment Context** tab only when `dealId` is present.

## Consolidated DNA page (2026-05-31, v2 — full Explorer shell)

**`operator-dna-profile.html`** now uses the **same Dealality dark shell, tabs, and `buildPanels()` output** as `operator-explorer-gold-mock.html`, so every Setup data point (leadership photos, footprint by region/chain scale/brand, case study images, KPI grids, etc.) is included.

| Layer | What it is |
|-------|------------|
| **All 10 Explorer tabs** | Unchanged names and full panel HTML from `OperatorExplorerGoldMock.buildPanels()` |
| **DNA additions** | Prepended blocks on **Profile & Positioning** (owner DNA intro) and **Markets & Footprint** (3-layer market experience) |
| **Alignment** | `dealId` in URL → alignment context (same as Explorer) + optional Alignment tab |
| **Live Explorer list** | Still `operator-explorer.html` → gold-mock popup — **not modified** |

**Requires** `?operatorId=rec…` (copy from Explorer popup URL). No separate light-theme duplicate UI.

Shared styles: `public/css/operator-explorer-profile-shell.css` (extracted from gold-mock).

## Future replacement path

1. Validate DNA profile UI and view model with owners/operators on prototype URL.
2. Add optional link from Explorer list popup: “View DNA Profile (beta)” → `operator-dna-profile.html?operatorId=…` (not done yet).
3. When ready: point `viewOperator()` / popup to DNA profile OR replace gold-mock content with shared renderers.
4. Deprecate `operator-explorer-detail.html` and reduce gold-mock to shared modules only.
5. No Airtable schema changes until field gaps are signed off.

## Mapped fields (live — from Operator Setup prefill / basics)

| DNA area | Prefill / fields used |
|----------|------------------------|
| Identity | `companyName`, `companyDescription`, `companyHistory`, `parentCompany`, `primaryServiceModel`, `brandedVsIndependentMix`, `chainScalesSupported` |
| Hero scale | `totalProperties`, `totalRooms`, `activeCountries`, `activeMarkets` |
| Market experience (current) | `activeCountries`, `activeMarkets` |
| Market experience (team) | `teamExperienceMarkets` (often empty) |
| Market experience (target) | `targetGrowthMarkets`, `priorityMarkets`, `specificMarkets` |
| DNA tags | `chainScalesSupported`, `serviceModelsSupported`, capability level fields |
| Brands | `brandFamiliesOperated`, `brands`, `brandProfiles`, `additionalBrands` |
| Capabilities | `revenueManagementCapability`, `ownerReportingLevel`, `preOpeningSupportCapability`, `conversionReflagExperience`, `fbCapabilityLevel`, `offeredServices`, `differentiators` |
| Asset value | `revparImprovement`, `noiImprovement`, `achievements`, `procurementServices` |
| Owner fit | `bestFitOwnerTypes`, `bf_*`, `propertyTypes`, `minPropertySize`, `maxPropertySize` |
| Case studies | `caseStudiesDetail[]` |
| Contact | `website`, `headquarters`, `contactEmail` |
| Alignment (deal) | `GET /api/operator-alignment-snapshot/:dealId/companies` |

## Likely schema / mapping gaps (later)

- `teamExperienceMarkets` — critical for CALA team-vs-portfolio story; often not populated.
- `targetGrowthMarkets` — dedicated field vs inferred from `priorityMarkets`.
- Structured `brandRows` (family / depth / relationship type).
- `languages`, `localOffices`, regulatory/cultural fluency as first-class fields.
- DNA snapshot tiles with strength levels per dimension.
- Asset value metrics (RevPAR %, GOP %) as structured numbers, not free text.
- Representative properties table separate from case studies.
- Contact card (name, title, phone) for “Request Introduction”.
