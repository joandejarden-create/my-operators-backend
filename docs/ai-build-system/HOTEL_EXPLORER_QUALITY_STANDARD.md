# Hotel Explorer Quality Standard

**Version:** `hotel-explorer-quality-standard-v1.14`  
**Status:** Binding for Hotel Explorer product surfaces (Radar-hosted HE + Founder Preview + Golden Demo)  
**Companion registry:** `docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json`  
**Gate:** `npm run test:he-product-quality-gates-v1` · `npm run test:he-owner-portfolio-operator-view-v1` · `npm run test:he-ownership-control-network-v1` · `npm run test:he-hotel-news-from-market-alerts-v1` · `npm run test:he-guest-sentiment-brand-operator-v1` · `npm run test:he-market-alerts-closeout-v1` · `npm run test:he-async-section-loading-v1`

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
- Canonical map: `attribute_id → display_label → icon_id → category → display_priority` (`he-amenity-icon-map-v1.js` + `hilton-amenity-icons.js`). Unrelated attributes must not share a generic icon (e.g. Bar ≠ Restaurant, Accessibility ≠ checkmark-only default).
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

### 5a. Owner portfolio & operator reconciliation (Organization & Portfolio)

- **Owned / Controlled Hotels** and **Who Manages These Hotels?** must use the **same** selected hotel set (deduped by canonical HPC identity) for the active evidence scope.
- Evidence classes: **Verified**, **Proposed**, **Historical**, **Unresolved match**. A company portfolio listing is association evidence — not verified current title/control.
- Scope toggle: **Verified only** vs **Verified + proposed**. Changing scope must update both sections with identical denominators.
- Do not equate brand affiliation or management contracts with real-estate ownership. Operator-only hotels must not enter ownership totals. Historical dispositions stay out of current totals.
- HPC-matched hotels deep-link to Hotel Explorer; unmatched candidates show **Pending HPC Match** (no speculative links).
- Section B groups the selected Section A hotels by current primary operator; include **Operator Not Verified** and **Owner-Operated** when applicable.
- Mandatory reconciliation for the selected scope: hotel count = sum of operator-group hotel counts; known rooms = sum of operator-group known rooms.
- Room coverage: total properties, hotels with known rooms, known-room sum, hotels with unresolved rooms. Never present incomplete rooms as a complete portfolio-room total.
- Label **Verified Portfolio Coverage** only for verified-only scope; use **Selected Portfolio Coverage (verified + proposed)** when proposed candidates are included.
- Research the organization once; persist HI graph relationships; reuse across every linked Hotel Explorer.
- Shared modules: `lib/hotel-intelligence/ownership/owner-portfolio-operator-view-v1.js` + `public/js/he-owner-portfolio-operator-view-v1.js`.

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

- Tabs use equal grid allocation (`hex-tabs`); consistent icon size/gap; stable 2px icon borders (no active-state layout shift); overflow scroll on narrow screens.
- Inventory + Submarket tables reuse My Deals / `ada-table` surfaces (`--hex-surface-table` / `#0d152c` headers, dark rows, sort chevrons) — not approximate new themes.
- Tab panels share canvas `--hex-surface-canvas` / `--hdp-bg-deep`; no arbitrary patch backgrounds across sections.

### 8a. Canonical surface hierarchy (all tabs)

Scoped to `.hex-root` (Founder Preview and Radar HE share the same CSS):

| Level | Token | Value | Allowed uses |
| --- | --- | --- | --- |
| Canvas | `--hex-surface-canvas` | `#080f25` | Shell, scroll, tab rail |
| Section / card | `--hex-surface-section` | `#101935` | `hiw-section`, cards, hero/panel, HCI/OI cards |
| Table | `--hex-surface-table` | `#0d152c` | ada/dl tables, operator groups, meas prefs |

Forbidden: intermediate navy shades (`#0c132c`, `#0b1224`, `#0b1228`), translucent navy fills (`rgba(8,15,37,…)`, `rgba(13,21,44,…)`) on HE content surfaces, and per-tab background inventions. Separation uses spacing, borders, and typography — not extra navy fills. Expanded table/operator content inherits the table surface (transparent body).

### 9. Measurement units (presentation only)

- Shared session preference module: `he-measurement-prefs-v1` (do not duplicate conversion logic).
- **Contextual controls only — no global measurement toolbar** (forbidden: `hex-meas-prefs-slot-host` in the HE shell).
  - Area **Sq. Ft. / Sq. M.** selector appears **only** in Guest Rooms & Meeting Space (`renderAreaControlHtml`).
  - Distance **mi / km** selector appears **only** in Area Hotels and Demand Drivers (`renderDistanceControlHtml`); both tabs share one session distance preference.
  - All other tabs (Overview, Ownership, Contacts, Organization & Portfolio, Relationships, Submarket Snapshot, Access & Connectivity, Brand/Operator, Sources) must show **neither** selector.
- Visible labels use exact capitalization: `Sq. Ft.`, `Sq. M.`, `mi`, `km`.
- Conversion constants: `1 sq ft = 0.09290304 sq m`; `1 mi = 1.609344 km`.
- Sort and geographic inclusion always use source numeric units (sq ft / km). Unit toggle must not change which hotels qualify or mutate HPC/Airtable values.
- Tab changes and hotel navigation must not reset session preferences.
- Default Bethesda pilot: Sq. Ft. / mi when no prior preference.

### 9c. Hotel News & Articles (Market Alerts)

- Canonical store = Airtable **MarketAlerts** (`AIRTABLE_BASE_ID`). No parallel HE news database; no article rows in HPC/HI Organizations.
- Brand / Operator tab may show **Hotel News & Articles** only when ≥1 property-relevant article exists; otherwise **hide the subsection** (no empty state).
- Matching classes: `HOTEL_SPECIFIC` / `MULTI_HOTEL_SPECIFIC` visible; `ORGANIZATION_CONTEXT_ONLY` / `AMBIGUOUS_MATCH` / `UNRELATED` never shown as hotel news.
- Brand- or sponsor-only coverage must not populate the subsection.
- **Canonical hotel relationships:** Tags `hpc:<HPC record id>` (idempotent, multi-hotel via multiple tags) is the production association path today. Designed complementary multiline field `HPC Hotel Record IDs` supports one article → many HPC ids — **do not create the field or write associations** without Market Alerts owner + founder authorization (`MARKET_ALERTS_HOTEL_LINK_APPLY=1`). Never duplicate article rows for multi-hotel links.
- **Automatic refresh:** After associations are approved and persisted, HE discovers them through the normal GET news read path (Tags / optional assoc field / property-name matching). Opening Brand/Operator or Sources must **not** write Airtable or trigger paid research (`writeOnRead: false`).
- **Sources & Evidence:** Property-relevant Market Alerts articles surface as citations via the same GET payload (`evidenceCitations`) — preserve Market Alerts record id, source URL, publisher, publication date, supported hotel fact, and verification status. Do **not** copy articles into a second HI Evidence database.
- GET `/api/hotel-explorer/hotels/:hotelId/news-articles` is read-only (`X-Dealality-Write: forbidden`); Market Alerts outage → invisible news section, not a hard HE failure.
- Persist metadata/summaries/URLs only — not full copyrighted article text.
- Shared modules: `lib/hotel-intelligence/hotel-news-from-market-alerts-v1.js`, `market-alerts-hotel-association-sync-v1.js`, `market-alerts-evidence-citations-v1.js`, `api/hotel-explorer-news-articles.js`.

### 9e. Independent loading & canonical wave animation

- Reuse **only** `DealalityWaveLoader` / `dealality-wave-loader.css` (My Deals lineage). Compact/inline variants extend the same component — do not invent new spinners or skeleton systems.
- Hotel Explorer subsections load **independently** (Brand overview, Hotel News, Guest Sentiment, Sources Market Alerts evidence, OCN expand, Contact Surfe enrich). A slow or failed provider must not block sibling sections or the whole tab.
- Section states: `IDLE` · `LOADING` · `READY` · `EMPTY` · `ERROR` · `STALE` via `he-async-section-v1.js`.
- Loading: show compact wave **inside** the section. Empty News remains **hidden**. Errors use restrained copy + Retry — never stack traces or raw JSON.
- Lifecycle: request generation + AbortController; cache TTL; ignore stale responses after hotel switch; no unnecessary refetch of warm cache; no GET-triggered writes; no aggressive polling or unpaid provider calls.
- Accessibility: `role="status"` / `aria-live="polite"`; honor `prefers-reduced-motion`; avoid focus traps and large layout jumps.
- Shared modules: `public/js/dealality-wave-loader.js`, `public/js/he-async-section-v1.js`, Brand/Operator mounts in `hotel-intelligence-workspace.js`.

### 9d. Guest Sentiment & Reputation (Brand / Operator)

- **Not a new Hotel Explorer tab.** Guest Reputation & Experience lives inside **Brand / Operator** after Brand & Operator Overview and Hotel News & Articles (same Guest Sentiment data model).
- Present a **dashboard**, not a flat text report when structured platform data exists: platform scorecards, Category Performance bars, executive insight cards, restrained trends status, collapsed Sources & Methodology.
- Show platform-native rating, scale, review count, observation date, and listing link per source. **Never** silently average incompatible scales (e.g. Tripadvisor /5 vs Booking /10).
- **Category Performance** uses platform-published category scores only. Do **not** label category scores as “Frequently praised” / guest-theme frequency unless licensed review-level evidence exists.
- Review Theme Analysis and historical trend charts remain **hidden** when unsupported — never invent themes, percentages, or 30/90/180/365 series from one snapshot.
- Executive insights must include finding + evidence + owner implication + confidence; do not imply CapEx / service failure without supporting evidence.
- Live customer UI stays **hidden** unless `live_ui_authorized === true` from a rights-cleared store. Founder Proposed Preview may render staged public-listing metadata with an explicit rights chip.
- No unauthorized paid provider calls from HE routes (`X-Dealality-Paid-Providers: forbidden`). No consumer OTA scrape. No full copyrighted review bodies unless licensed.
- Independent loading (HE-QL-024): reputation provider failure must not block Brand overview or Hotel News.
- Themes use `guest-theme-taxonomy-v1`; category scores are evidence of platform aggregates — not operational KPIs or CapEx proof. Do not emit precise % claims below `MIN_SAMPLE_FOR_PERCENT` (30).
- Incorrect hotel↔platform property matches, duplicate reviews, unsupported conclusions, and stale-as-current displays are regression failures (HE-QL-022 / HE-QL-025).
- Shared modules: `lib/hotel-intelligence/guest-sentiment/*`, `api/hotel-explorer-guest-sentiment.js`, Brand/Operator host in `hotel-intelligence-workspace.js`.

### 9a. Ownership & Control Network

- Ownership Intelligence presents an **interactive Ownership & Control Network**, not a linear hierarchy dump.
- Typed edges only: Legally owned by / Economically controlled by / Managed by / Branded as / Associated person / Portfolio hotel.
- **Never** draw operator or brand as an ownership chain step; never invent PropCo→sponsor control from unrelated hotel associations.
- Hotel-centered hierarchical SVG layout (pan/zoom/fit/reset); expand organization hotels on demand with bounded depth/node limits.
- Visual + label differentiation for verified / proposed / historical / unresolved (not color alone).
- Tab structure: Executive Ownership Summary → Interactive Network → Relevant Decision-Makers (compact; Contact Intelligence for full cards) → Relationship Detail ↔ graph highlight → Sources & Verification.
- Forbidden: duplicate hierarchy text blocks, concatenated label/value strings, raw graph node IDs / internal enums, HPC property-contact fallback panels in the ownership narrative, proposed relationships styled as verified, deep links without resolved HPC IDs.
- Shared modules: `lib/hotel-intelligence/ownership/ownership-control-network-v1.js` + `public/js/he-ownership-control-network-v1.js`.

### 9b. Canonical HPC population (portfolio batches)

- Reuse `HPC_LIVE_IDENTITY_COLLISION_V1`, `hpc-shared-upsert-v1`, and HI graph contracts — do not create a parallel hotel database.
- Canonical base `appCCUsuGsE1ifoLk` / HPC `tbl9aY5ijiuIzzWam`; Hotel Census is legacy **read-only** for matching.
- Identity outcomes: `MATCHED_HPC` | `MATCHED_HOTEL_CENSUS` | `MATCHED_EXISTING_ALIAS` | `NEW_HPC_CANDIDATE` | `AMBIGUOUS_MATCH` | `IDENTITY_UNRESOLVED`.
- Default `applyAuthorized: false`. **STOP** before production Airtable apply until the dry-run CREATE/UPDATE packet is founder-approved.
- Ownership intelligence lives in HI Organizations / Relationships / Evidence — not HPC free-text portfolio dumps.
- Contract: `docs/ai-build-system/HE_HPC_POPULATION_WORKFLOW_V1.md`.

### 10. Submarket Snapshot & Area Hotels (HPC)

- Prefer Radar map census; when empty, read-only Scout Hotel Census coverage (never invent market totals).
- Deduplicate hotel identities before aggregation; separate open vs pipeline.
- Show explicit coverage limitations when geography is provisional or sample-capped.
- Area Hotels radii by location setting (initial): Urban 5 mi, Suburban/Airport 10 mi, Resort 30 mi, Rural 25 mi — adjustable; distance unit display independent of inclusion.
- No GET-triggered writes; no unapproved Airtable submarket bulk apply.

## Machine-testable acceptance

See `scripts/test-he-product-quality-gates-v1.mjs` and the lessons registry `regressionTest` fields.

## Research / population workflow

New hotels must reuse HPC + HI intelligence, run the commercial description rubric, normalize amenities, render ownership/contact/rooms with shared components, and attach evidence. Human corrections update **shared rules** (this standard + registry + code), not one-off hotel data.

## Safety

- No production Airtable apply / schema mutation / deploy from quality work alone.
- GET routes remain write-forbidden.
- Proposed ≠ Live.
