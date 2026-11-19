# Hotel Explorer Quality Standard

**Version:** `hotel-explorer-quality-standard-v1.20`  
**Status:** Binding for Hotel Explorer product surfaces (Radar-hosted HE + Founder Preview + Golden Demo)  
**Companion registry:** `docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json`  
**Gate:** `npm run test:he-product-quality-gates-v1` · `npm run test:he-owner-portfolio-operator-view-v1` · `npm run test:he-ownership-control-network-v1` · `npm run test:he-hotel-news-from-market-alerts-v1` · `npm run test:he-guest-sentiment-brand-operator-v1` · `npm run test:he-market-alerts-closeout-v1` · `npm run test:he-async-section-loading-v1` · `npm run test:he-contact-intelligence-directory-ux-v1` · `npm run test:he-loading-ux-toast-v1`

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
- Source: ADP verified attributes / HPC amenities via shared canonical IDs — never invent amenities to hit a count.
- Display policy: research target **≥12** verified when credible evidence permits; hard max **20** displayed (priority selection); overflow retained in underlying intelligence for search/filter.
- **Coverage research trigger (HE-QL-030):** when verified guest-amenity count is below 12, the system must flag an internal `RESEARCH_GAP` (investigate ADP catalog, HPC amenities, first-party brand materials). Do **not** pad the strip, invent amenities, or show a customer-facing completeness badge.
- Prefer **union** of verified + HPC amenity sources (dedupe by human label; verified rows win collisions). Exclusive verified-only selection that hides recoverable HPC guest amenities is a regression.
- Canonical map: `attribute_id → display_label → icon_id → category → display_priority` (`he-amenity-icon-map-v1.js` + `hilton-amenity-icons.js`). Unrelated attributes must not share a generic icon (e.g. Bar ≠ Restaurant, Accessibility ≠ checkmark-only default).
- Human-readable labels + consistent icons across hotels; no raw DB tokens (`has_pool`); no geo/brand marketing tags as amenity icons.
- Shared policy: `lib/hotel-intelligence/he-amenity-display-v1.js` (`selectAmenitiesForHotelExplorerDisplay`, `mergeAmenityDisplaySources`, `assessAmenityCoverageResearchNeed`) + `hotel-detail-panel.js` carousel.

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
- Classifications: `PRIMARY_PROPERTY_DECISION_MAKER`, `OWNER_DECISION_AUTHORITY`, `RELEVANT_COMMERCIAL_ENTRY_POINT`, `CORPORATE_FALLBACK`, `ROLE_NOT_VERIFIED` — UI uses professional labels; never show raw enums to users.
- When property assignment is unconfirmed, show **Suggested Contact — Property Responsibility Not Confirmed** (never a false “verified primary decision-maker”).
- **Compact directory layout (HE-QL-027):** Recommended First Contact (name, title, company, one-line relevance, property responsibility chip, LinkedIn / Company Profile / email / phone / **Find Email & Phone**) → Alternative Decision-Makers as compact rows → Organization Contact Channels once → Research gaps collapsed by default. Progressive disclosure for evidence / notes. No oversized empty email/phone boxes, dash placeholders, duplicated corporate fallbacks under every person, or long repeated authority essays.
- Never invent emails/phones or LinkedIn URLs; never label org channels as personal; never treat scraped wrong-company data as verified; never auto-associate ambiguous provider identities.
- Surfe is on-demand fallback (pass LinkedIn URL, name, company, domain when known); check HI Contact Channels + public research first; no auto-enrich on load; no bulk paid enrichment; no duplicate paid lookup while in-flight or when channels already verified; no Airtable write from reveal. Mock/dry-run provider (`CONTACT_ENRICHMENT_MOCK` / `CONTACT_ENRICHMENT_DRY_RUN_PROVIDER`) enables no-cost UX testing. Provider results stay `PROVIDER_RETURNED_UNREVIEWED` until approved persistence. Per-person Dealality wave loading; other cards stay interactive.

### 7. Sources & evidence

- Every newly displayed verified fact links to Sources / evidence.
- Fixture / staged / proposed content must be labeled as such.
- **One canonical Sources & Evidence tab (HE-QL-031):** all hotel-specific research evidence lives in a single deduplicated register (HI Graph Evidence, HPC/ADP references, Market Alerts, guest reputation listings, contact provenance). Do **not** duplicate source tables on Ownership or other tabs. Do **not** copy canonical Market Alerts / HI Evidence into a second Airtable merely for presentation.
- Register fields: source title · publisher · category · publication date · verified/accessed date · supported claim · verification status · original link. Publication date must never be replaced by research/access/verification time; unknown publication stays unknown.
- Category filters (show only categories present for the hotel): All · Property & Facilities · Ownership & Organizations · Contacts & People · Financing & Development · Brand & Operator · News & Market Alerts · Guest Sentiment.
- Cross-tab navigation uses compact header-right text links (`View Contacts` / `View Sources`) — not oversized white `hiw-btn` cards. Links preserve hotel + preview mode and may deep-link (`data-hiw-source-filter` / `data-hiw-source-focus`) to highlight rows. Keep contextual hyperlinks (LinkedIn on contact cards, article headlines) where useful — centralize the register, not every action link.
- Forbidden in customer UI: duplicate “Sources and Verification” subsections, raw evidence IDs / developer enums, repeated verification essays across tabs, inconsistent hyperlink styling.

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

### 9e. Independent loading & canonical Dealality toast / wave (HE-QL-028)

- Reuse **only** `DealalityWaveLoader` + `HeLoadingUx` (`he-loading-ux-v1.js`) + `he-async-section-v1.js`. Do **not** invent new spinners, skeleton systems, or full-width black loading banners.
- **Modes (shared adapter):**
  - **Toast** (`toastHtml`) — short subsection fetches / tab transitions: small, horizontally centered in the section, Dealality toast chrome (wave orb + border/shadow). Message e.g. `Loading Contacts…`. Auto-dismisses when READY/EMPTY/ERROR.
  - **Wave** (`waveHtml` / compact inline) — sustained section loads, still centered — never `width: 100%` black strips.
  - **Inline** (`inlineHtml`) — action-local (Surfe Find Email & Phone, OCN Explore Key People / Related Hotels, Retry).
- Forbidden: full-width black notification bars; page-blocking overlays for subsection loads; multiple overlapping toasts; stuck loaders; generic browser busy indicators where Dealality UI exists.
- Hotel Explorer subsections load **independently** (Brand overview, Hotel News, Guest Sentiment, Sources Market Alerts evidence, OCN expand, Contact Surfe enrich, Area Hotels / Submarket). A slow or failed provider must not block sibling sections or the whole tab.
- Section states: `IDLE` · `LOADING` · `READY` · `EMPTY` · `ERROR` · `STALE` via `he-async-section-v1.js`.
- Lifecycle: request generation + AbortController; cache TTL; skip toast flash for warm cache; ignore stale responses after hotel switch; preserve visible content on background refresh; no GET-triggered writes; no aggressive polling or unpaid provider calls.
- Accessibility: `role="status"` / `aria-live="polite"`; honor `prefers-reduced-motion`; avoid focus traps and large layout jumps.
- Shared modules: `dealality-wave-loader.js/css`, `he-loading-ux-v1.js`, `he-async-section-v1.js`, Contact Intelligence + Brand/Operator mounts, OCN expand.

### 9d. Guest Sentiment & Reputation (Brand / Operator)

- **Not a new Hotel Explorer tab.** Guest Reputation & Experience lives inside **Brand / Operator** after Brand & Operator Overview and Hotel News & Articles (same Guest Sentiment data model).
- Present a **dashboard**, not a flat text report: seven-platform overview, Category Performance bars, compact executive insights, collapsed Sources, Methodology & Limitations.
- **Seven-platform overview (HE-QL-029):** always show the canonical catalog (`REPUTATION_OVERVIEW_PLATFORMS_V1`) — Tripadvisor, Booking.com, Expedia, Google Reviews, Hotels.com, Agoda, Trivago. Available platforms show native rating, scale, review count, observation date, and listing link. Unavailable platforms stay visible as dimmed **Not connected** / **Data unavailable** cards — never invent 0 ratings, fabricated review counts, or summaries. Catalog candidates are not represented as integrated feeds until source access and rights are confirmed.
- Accent colors reuse Dealality Chain Scale / My Deals tokens (`--accent--primary-1` `#6c72ff`, gold `#fdb52a` for hover) — no arbitrary cyan/sky (`#38bdf8`) on reputation bars, links, or selectors.
- Show platform-native rating, scale, review count, observation date, and listing link per available source. **Never** silently average incompatible scales (e.g. Tripadvisor /5 vs Booking /10).
- **Category Performance** uses platform-published category scores only. Do **not** label category scores as “Frequently praised” / guest-theme frequency unless licensed review-level evidence exists. Relatively softer category scores (e.g. Value 7.7/10) are **areas to investigate**, not proof of recurring guest complaints.
- **Guest Experience Insights** are a compact executive assessment: Relative Strengths · Areas to Investigate · Recommended Management Follow-Up · Trend & Data Confidence — short finding + metric/source + optional expandable evidence. Do not repeat methodology caveats in every card.
- Detailed coverage, licensing, sample scope, observation dates, and platform-score differences live in collapsed **Sources, Methodology & Limitations** — no raw API names or developer diagnostics in the main UI.
- Review Theme Analysis remains **hidden** when unsupported — never invent themes, percentages, or 30/90/180/365 series from one snapshot. Trend status belongs in the compact confidence card, not a fabricated chart.
- Live customer UI stays **hidden** unless `live_ui_authorized === true` from a rights-cleared store. Founder Proposed Preview may render staged public-listing metadata (or seven unavailable cards when no observations) with an explicit rights chip.
- No unauthorized paid provider calls from HE routes (`X-Dealality-Paid-Providers: forbidden`). No consumer OTA scrape. No full copyrighted review bodies unless licensed.
- Independent loading (HE-QL-024 / HE-QL-028): reputation provider failure must not block Brand overview or Hotel News; use canonical compact toast/wave.
- Themes use `guest-theme-taxonomy-v1`; category scores are evidence of platform aggregates — not operational KPIs or CapEx proof. Do not emit precise % claims below `MIN_SAMPLE_FOR_PERCENT` (30).
- Incorrect hotel↔platform property matches, duplicate reviews, unsupported conclusions, invented missing-platform values, and stale-as-current displays are regression failures (HE-QL-022 / HE-QL-025 / HE-QL-029).
- Shared modules: `lib/hotel-intelligence/guest-sentiment/*`, `api/hotel-explorer-guest-sentiment.js`, Brand/Operator host in `hotel-intelligence-workspace.js`.

### 9a. Ownership & Control Network / Stakeholder Network

- Ownership Intelligence presents a **Hotel Stakeholder & Deal Relationship Network** (interactive), not a linear hierarchy dump and not an unbounded “Show All” web.
- **Default view (hotel-centered):** selected hotel · legal property owner · economic owner/sponsor · current operator · current brand · UBO only when a named party is known. Do **not** auto-load all people or portfolio hotels.
- Ownership/control relationships render **above** the hotel; operations/brand render **below** — never imply a false linear hierarchy between sponsor, operator, and brand.
- Typed edges only (extended taxonomy allowed when evidence-backed): Legal / Economic / UBO / Managed / Brand / Key person / Related hotel / Asset managed / Financed / Developed / Advised / Equity partner.
- **Never** draw operator or brand as an ownership chain step; never invent PropCo→sponsor control, financing terms, franchise expiration, or brand-conversion willingness.
- Readable nodes: min ~13px body label, content-measured width/height, wrapped text, no overflow clipping; long titles/evidence belong in the **detail panel**.
- Progressive disclosure: Explore Key People / Explore Related Hotels / Brand Development view; bounded visible caps; collapse supported; short edge tags with full meaning on edge/node selection.
- Visual + label differentiation for verified / proposed / historical / unresolved (not color alone).
- Tab structure: Executive Ownership Summary → Stakeholder Network → Relevant Decision-Makers (compact; header **View Contacts** link) → Relationship Detail ↔ graph highlight (header **View Sources** deep-links to Sources & Evidence). No inline Sources and Verification table on Ownership.
- Forbidden: node-text overflow, overlapping nodes, unreadable typography, unbounded portfolio dumps, duplicate hierarchy text blocks, raw graph node IDs / internal enums, HPC property-contact fallback panels, proposed styled as verified, deep links without resolved HPC IDs, unsupported capital/advisor nodes.
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
