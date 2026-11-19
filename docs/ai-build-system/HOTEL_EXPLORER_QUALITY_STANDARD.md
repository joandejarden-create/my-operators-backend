# Hotel Explorer Quality Standard

**Version:** `hotel-explorer-quality-standard-v1.3`  
**Status:** Binding for Hotel Explorer product surfaces (Radar-hosted HE + Founder Preview + Golden Demo)  
**Companion registry:** `docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json`  
**Gate:** `npm run test:he-product-quality-gates-v1`

## Purpose

KGPV (golden UX) and Bethesda Marriott (Airtable-first pilot) define the permanent bar. Every future Hotel Explorer must meet these rules through **shared components and generation workflows**, not hotel-specific patches.

## Surfaces in scope

| Surface | Canonical components |
| --- | --- |
| Hotel Overview | `public/js/hotel-detail-panel.js` |
| Amenities | `hotel-detail-panel.js` amenity grid + taxonomy |
| Guest Rooms & Meeting Space | `hotel-explorer-rooms-meeting-space.js` + `dealality-sortable-table.js` |
| Ownership / Relationships / Sources | `hotel-intelligence-workspace.js` |
| Contact Intelligence | `hotel-contact-intelligence.js` + CI API |
| Navigation / shell | `hotel-explorer.js` + `hotel-explorer.css` |
| Commercial description rubric | `lib/hotel-intelligence/hotel-commercial-description-v1.js` |

## Standards

### 1. Hotel descriptions

- Executive asset summary (~200–300 words when facts allow), multi-paragraph, owner / operator / asset-manager / investor voice (`hotel-commercial-description-v2`).
- Rubric covers: asset positioning, market/location, physical product, demand potential (evidence-only), ownership/operation, asset considerations, commercial interpretation without invented performance.
- Forbidden mechanical phrases: “The physical product comprises…”, “It sits in…”, “Day-to-day operations are managed by…”.
- Forbidden: keyword stuffing, census fact-lists, internal IDs (`rec…`, `dle_…`), invented ADR/RevPAR/occupancy, raw status enums as prose, developer diagnostics in UI.
- Prefer Airtable / overview enrichment when quality-gated; else rubric; else “not yet available.”

### 2. Property facts

- Year Built / Last Renovated always present; use **Not yet verified** when unknown — never invent.
- Meeting-space aggregates must cite provenance and never double-count divisible salons.

### 3. Amenities

- **Visual (KGPV baseline):** single horizontal icon+label strip — no bordered amenity cards, no vertical grid. Prev/next arrows only when overflow; smooth horizontal scroll; keyboard arrows; minimal/hidden scrollbar.
- Source: ADP verified attributes / HPC amenities via shared IDs — never invent amenities to hit a count.
- Display policy: target **≥12** verified when research allows; hard max **20** displayed (priority selection); overflow retained in underlying intelligence.
- Human-readable labels + consistent icons across hotels; no raw DB tokens (`has_pool`); no geo/brand marketing tags as amenity icons.
- Shared policy: `lib/hotel-intelligence/he-amenity-display-v1.js` + `hotel-detail-panel.js` carousel.

### 4. Guest Rooms & Meeting Space

- Tab immediately after Hotel Overview for all HE shells.
- Guestroom inventory + suite classification (suites not additive to total).
- Meeting summary + sortable inventory table (Dealality sortable table: high-contrast headers, sort chevrons, numeric sort, horizontal scroll).
- Combined vs salon rows marked **not additive**.

### 5. Ownership & relationships

- KGPV-quality hierarchy: hotel → legal PropCo → economic sponsor → operator/brand; UBO unresolved when unknown.
- Human org names only — never graph/org IDs in tables.
- Proposed overlays must show **Proposed · not persisted** and never imply Airtable truth.

### 6. Contact Intelligence / Owner & Decision-Maker Intelligence

- Workflow: **Existing Airtable → public research → verification → staged/canonical persistence → Surfe only on explicit click** for missing email/phone.
- Property-to-person path: Hotel → ownership/management entity → portfolio responsibility → relevant individual → contact channel.
- Rank contacts with documented factors (hotel responsibility, AM/ownership authority, evidence, channels). Do **not** auto-select CEO for seniority alone.
- Classifications: `PRIMARY_PROPERTY_DECISION_MAKER`, `OWNER_DECISION_AUTHORITY`, `RELEVANT_COMMERCIAL_ENTRY_POINT`, `CORPORATE_FALLBACK`, `ROLE_NOT_VERIFIED` — UI uses professional labels.
- When property assignment is unconfirmed, show **Suggested Contact — Property Responsibility Not Confirmed** (never a false “verified primary decision-maker”).
- Cards show name, title, org, hotel relevance, property responsibility, verified LinkedIn, email/phone when verified, corporate fallbacks separately, sources, and **Find Contact Details** when eligible.
- Never invent emails/phones; never label org channels as personal; never treat scraped wrong-company data as verified.
- Surfe is on-demand fallback (pass LinkedIn URL when known); check existing channels first; no auto-enrich; no Airtable write from reveal.

### 7. Sources & evidence

- Every newly displayed verified fact links to Sources / evidence.
- Fixture / staged / proposed content must be labeled as such.

### 8. Navigation & tables

- Tabs evenly distributed; consistent icon size; readable labels; overflow scroll without crushing layout when a tab is inserted.
- Inventory tables use shared `DealalitySortableTable` — not hotel-specific CSS patches.

## Machine-testable acceptance

See `scripts/test-he-product-quality-gates-v1.mjs` and the lessons registry `regressionTest` fields.

## Research / population workflow

New hotels must reuse HPC + HI intelligence, run the commercial description rubric, normalize amenities, render ownership/contact/rooms with shared components, and attach evidence. Human corrections update **shared rules** (this standard + registry + code), not one-off hotel data.

## Safety

- No production Airtable apply / schema mutation / deploy from quality work alone.
- GET routes remain write-forbidden.
- Proposed ≠ Live.
