# Brand Explorer — Airtable integration & source-of-truth (Phase 1)

**Contract:** `BE_AIRTABLE_INTEGRATION_SOT_V1`  
**Module:** `lib/brand-explorer/be-airtable-integration-sot-v1.js`  
**Gate:** `npm run test:be-airtable-integration-sot-v1`  
**Status:** Phase 1 audit + ownership contract — **no bulk population**, **no production writes**  
**Date:** 2026-11-19

---

## 1. Purpose

Brand Explorer is a **connected intelligence layer**. Profiles must incorporate verified Dealality inventory and Brand Setup facts without duplicating or corrupting them.

This document freezes:

1. Actual Airtable bases / tables / joins used today  
2. Canonical brand identity mapping  
3. Field-level ownership for the ten-tab profile  
4. HPC footprint calculation rules (designated authority)  
5. Extensible read-only ADP / GDI attachment points  
6. Factory write boundaries and publication separation  

---

## 2. Airtable environment (as implemented)

### Product base (`AIRTABLE_BASE_ID`)

| Table | Role | Link to Brand Basics |
|-------|------|----------------------|
| `Brand Setup - Brand Basics` | Durable brand identity + universe (`Brand Status` Active/Live) | Primary |
| `Brand Setup - Brand Explorer Presentation` | 1:n owner-facing slots (`Slot Key`, Title, Body, Image, Active) | `Brand` (link) + optional `Brand Name` text |
| `Brand Setup - Brand Footprint` | Structured footprint intake / company-reported figures | Link / name |
| `Brand Setup - Project Fit` | Development / conversion / PIP structured fields | Link |
| `Brand Setup - Portfolio & Performance` | Portfolio / performance structured fields | Link |
| `Brand Setup - Brand Standards` | Standards structured fields | Link |
| `Brand Setup - Fee Structure` | Fee structured fields | Link |
| `Brand Setup - Deal Terms` | Deal terms structured fields | Link |
| `Brand Setup - Operational Support` | Ops support structured fields | Link |
| `Brand Setup - Legal Terms` | Legal structured fields | Link |
| `Brand Setup - Loyalty & Commercial` | Loyalty / commercial structured fields | Link |
| `Brand Setup - Sustainability & ESG` | ESG structured fields | Link |

API map: `api/brand-library.js` (`F.*`). Presentation slots: `docs/brand-explorer-presentation-slots.md`.

### Platform base (`AIRTABLE_BASE_ID_ALT`)

| Table | Role today for Brand Explorer |
|-------|-------------------------------|
| `Hotel Census` | **Transitional** inventory rollup for `brand.censusSummary` via Affiliation matchers |
| `Brand Alias Mapping` | Canonical brand name ↔ Affiliation / alias strings |
| `Hotel Property Census` | **Designated** footprint authority for BE (Radar Status Open/Pipeline). Used heavily by HE/ADP/GDI; **not yet** the primary BE rollup path |

HPC identity contract already shared: `lib/hotel-census/hpc-full-onboarding-contract-v1.js` (`HPC_TABLE_NAME = "Hotel Property Census"`).

---

## 3. Canonical brand identity

| Layer | Key | Notes |
|-------|-----|-------|
| **Primary** | Brand Basics Airtable `rec…` | Never reinvent; preserve all existing relationships |
| Display | `Brand Name` | May diverge slightly from Presentation `Brand Name` text — API already reconciles |
| Universe | `Brand Status ∈ {Active, Live}` | SoT for visible Explorer list |
| Alias | `Brand Alias Mapping` | Exact Affiliation / alias matchers for census joins |
| HPC join (designated) | `Current Brand` (+ optional Brand Family / Affiliation Status) | Must certify coverage before replacing Hotel Census rollups |
| Transitional join | Hotel Census `Affiliation` (+ Parent Company filter) | Current `buildBrandCensusSummary` path |

**Dedup rules:** one Basics record per brand; aliases never create a second primary; regional licensing differences must not collapse distinct brands.

---

## 4. Ten-tab source-of-truth (summary)

Canonical order matches atelier UI:

1. Overview — Presentation (+ Basics snapshot support)  
2. Value to Owners — Presentation  
3. Operating Model — Presentation (+ Brand Standards support)  
4. Owner Considerations — Presentation (+ Project Fit / Standards support)  
5. Commercial Engine — Presentation (+ Loyalty & Commercial support)  
6. Economics & Obligations — Presentation for attributed narrative; Fee/Deal/Legal **system-owned** (no fabricated fees)  
7. Loyalty Program — Presentation (+ Loyalty & Commercial)  
8. Footprint & Growth — **HPC designated** inventory; Hotel Census transitional; Brand Setup Footprint = company-reported attribution; Presentation = qualitative openings/momentum only  
9. Brand Materials — Presentation / asset governance  
10. Dealality Insight — Presentation; future ADP/GDI read contracts only  

Machine rows: `BE_TAB_SOURCE_OF_TRUTH` in the contract module.

---

## 5. Footprint calculation rules (binding)

### Designated authority: HPC

When an HPC brand rollup is certified for a brand:

1. Select HPC rows whose `Current Brand` (normalized) matches the brand’s certified matchers.  
2. Keep only `Radar Status ∈ {Open, Pipeline}`.  
3. Deduplicate by HPC record id.  
4. Report separately:
   - Open hotels / keys  
   - Pipeline hotels / keys  
5. Attach metadata: source table, aggregatedAt, matchers, recordsMatched, exclusions, coverageLimitations.  
6. Non-qualifying statuses (Closed, blank, etc.) are **excluded from totals** but may be reported in limitations.

### Transitional authority: Hotel Census

Until HPC brand rollup is certified, `GET /api/brand-library/brand` may attach `censusSummary` from Hotel Census (`BRAND_EXPLORER_CENSUS_METRICS`, default on). Trust model: `lib/brand-explorer-footprint-trust.js`.

`fallbackRecommended=true` must **not** be treated as authoritative inventory.

### Company-reported figures

Brand Setup - Brand Footprint (and similar company materials) may be shown as **separately attributed** company-reported metrics when they differ from HPC/census inventory.

**Hard rule:** never use generated or manually maintained footprint numbers as authoritative replacements for HPC-derived (or certified transitional census) metrics.

### Factory

- May stage qualitative Presentation footprint slots (region narrative, openings cards, momentum).  
- Must **not** overwrite system-owned numeric footprint inventory.  
- Must **not** invent open/pipeline totals.

---

## 6. ADP / GDI extensibility (read-only)

| Contract | Status | Join key | Speculative metrics |
|----------|--------|----------|---------------------|
| `adpBrandLevel` | `RESERVED_READ_ONLY` | Brand Basics `rec…` | **Forbidden in V1** |
| `gdiBrandLevel` | `RESERVED_READ_ONLY` | Brand Basics `rec…` | **Forbidden in V1** |

Do not modify ADP/GDI workstreams in this phase. Attachments land only through Integration-coordinated shared read contracts later.

---

## 7. Research vs publish separation

Lifecycle (aligned with factory recovery):

`DISCOVERED → RESEARCHING → RESEARCH_COMPLETE → DRAFT_GENERATED → VALIDATION_REQUIRED → READY_FOR_FOUNDER_REVIEW → FOUNDER_APPROVED → PUBLISHED`

Additional: `BLOCKED`, `STALE`, `REFRESH_REQUIRED`, `RESEARCH_INCOMPLETE`.

- Dry-run default  
- No auto-publish  
- Preserve approved published Presentation while researching replacements  
- No competing Brand Explorer database  

**Bulk population is blocked** until: identity verified, joins verified, HPC footprint rules verified, and this ownership contract is merged.

---

## 8. Current gaps (honest)

| Gap | Detail | Phase to close |
|-----|--------|----------------|
| HPC not yet primary BE rollup | Live `censusSummary` still uses Hotel Census | Phase 2–3 (certified HPC aggregator + feature flag) |
| Tab contracts on main still 7 tabs | UI has 10; PR #66 canonical shell unmerged | Phase 2 factory restore |
| Alias coverage uneven | Some brands `fallbackRecommended` | Phase 3 reconciliation |
| Company-reported vs inventory UX | Need dual attribution in UI when both exist | Phase 2 pilot |
| ADP/GDI brand panels | Reserved only | Phase 5 |

---

## 9. Regression coverage

```bash
npm run test:be-airtable-integration-sot-v1
```

Covers: tab order, ownership matrix, identity primary key, HPC Open/Pipeline filters, dedupe, stale/missing exclusions, company-reported must not replace HPC, ADP/GDI reserved contracts, atelier/API backward-compatibility presence checks.

Optional live audit (read-only GET):

```bash
npm run be-airtable-integration-audit-v1 -- --dry-run
```

---

## 10. Related code

- `api/brand-library.js` — Brand Setup reads + Presentation merge + optional `censusSummary`  
- `lib/hotel-census/build-brand-census-summary.js` — transitional Hotel Census rollup  
- `lib/hotel-census/aggregate-presence-summary.js` — Open/Pipeline aggregation  
- `lib/brand-explorer-footprint-trust.js` — display trust (census vs MVP vs unverified)  
- `lib/hotel-census/hpc-full-onboarding-contract-v1.js` — shared HPC identity  
- `docs/brand-explorer-presentation-slots.md` — Presentation slot keys  

---

## 11. Founder decisions (Phase 1 exit)

1. Confirm HPC as designated footprint authority (contract default: **yes**).  
2. Confirm Hotel Census remains transitional until HPC brand rollup certification.  
3. Confirm company-reported Brand Setup footprint stays separately attributed.  
4. Authorize Phase 2 factory restore only after this PR merges and BE home syncs.
