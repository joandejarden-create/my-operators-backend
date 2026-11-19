# Hotel Explorer Quality Standard

**Version:** `hotel-explorer-quality-standard-v1.25`  
**Status:** Binding for Hotel Explorer product surfaces (Radar-hosted HE + Founder Preview + Golden Demo)  
**Companion registry:** `docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json`  
**Gate:** `npm run test:he-product-quality-gates-v1` · `npm run test:he-owner-portfolio-operator-view-v1` · `npm run test:he-ownership-control-network-v1` · `npm run test:he-hotel-news-from-market-alerts-v1` · `npm run test:he-guest-sentiment-brand-operator-v1` · `npm run test:he-market-alerts-closeout-v1` · `npm run test:he-async-section-loading-v1` · `npm run test:he-contact-intelligence-directory-ux-v1` · `npm run test:he-loading-ux-toast-v1` · `npm run test:he-5-0-full-e2e-pilot-v1` · `npm run test:he-5-0-autonomous-reconciliation-v1`

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
- Rank contacts with documented factors (hotel responsibility, AM/ownership authority, evidence, channels). Do **not** auto-select CEO for seniority alone. Never infer signing authority from seniority.
- Classifications: `PRIMARY_PROPERTY_DECISION_MAKER`, `OWNER_DECISION_AUTHORITY`, `RELEVANT_COMMERCIAL_ENTRY_POINT`, `CORPORATE_FALLBACK`, `ROLE_NOT_VERIFIED` — UI uses professional labels; never show raw enums to users.
- When property assignment is unconfirmed, show **Suggested Contact — Property Responsibility Not Confirmed** (never a false “verified primary decision-maker”).
- **Compact directory layout (HE-QL-027):** Recommended First Contact (name, title, company, one-line relevance, property responsibility chip, LinkedIn / Company Profile / channels / Surfe CTA) → Alternative Decision-Makers as compact rows → Organization Contact Channels once → Research gaps collapsed by default. Progressive disclosure for evidence / notes. No oversized empty email/phone boxes, dash placeholders, duplicated corporate fallbacks under every person, or long repeated authority essays.
- **Shared contact channel hierarchy with GDI (HE-QL-032):** display labels **Direct Business Email**, **Direct Business Phone**, **Organizational Contact Route**. Surfe CTAs: **Find Email** / **Find Phone** / **Find Email & Phone** based only on missing direct channels. Never hide verified public directs behind enrichment. Never mislabel org/switchboard channels as personal direct. Reject guessed email formats and wrong-company scraper hits.
- Never invent emails/phones or LinkedIn URLs; never treat scraped wrong-company data as verified; never auto-associate ambiguous provider identities.
- Surfe is on-demand fallback (pass LinkedIn URL, name, company, domain when known); check HI Contact Channels + public research first; no auto-enrich on load; no bulk paid enrichment; no duplicate paid lookup while in-flight or when channels already verified.
- **Surfe session-only (binding):** Surfe-derived email/phone/LinkedIn/provider IDs/raw payloads must **never** persist to Airtable, logs, local caches, research files, or Sources & Evidence. Reveal uses `persist: false`; reload clears session results. Do not reclassify Surfe output as independently publicly verified. Mock/dry-run (`CONTACT_ENRICHMENT_MOCK` / `CONTACT_ENRICHMENT_DRY_RUN_PROVIDER`) for no-cost QA. Do not show enabled nonfunctional enrichment buttons — show a concise unavailable explanation instead.
- Public contact evidence (identity, LinkedIn, org routes, relevance) must appear in Sources & Evidence under Contacts & People. Canonical public upserts are dry-run only until founder-authorized apply. Shared module: `lib/hotel-intelligence/contact-intelligence/he-public-contact-channel-hierarchy-v1.js` (labels aligned with GDI founder directive).

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
  - Distance **mi / km** selector appears **only** in Area Hotels and Access & Connectivity when distances are shown (`renderDistanceControlHtml`); those tabs share one session distance preference (HE-QL-037: not Demand Drivers; not a global toolbar).
  - All other tabs (Overview, Ownership, Contacts, Organization & Portfolio, Relationships, Submarket Snapshot, Demand Drivers, Brand/Operator, Sources) must show **neither** selector.
- Visible labels use exact capitalization: `Sq. Ft.`, `Sq. M.`, `mi`, `km`.
- Conversion constants: `1 sq ft = 0.09290304 sq m`; `1 mi = 1.609344 km`.
- Sort and geographic inclusion always use source numeric units (sq ft / km). Unit toggle must not change which hotels qualify or mutate HPC/Airtable values.
- Tab changes and hotel navigation must not reset session preferences.
- Default Bethesda pilot: Sq. Ft. / mi when no prior preference.

### 9c. Hotel News & Articles (Market Alerts)

- Canonical store = Airtable **MarketAlerts** (`AIRTABLE_BASE_ID`). No parallel HE news database; no article rows in HPC/HI Organizations.
- Guest Sentiment tab may show **Hotel News & Articles** only when ≥1 property-relevant article exists; otherwise **hide the subsection** (no empty state).
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

### 9d. Guest Sentiment tab (reputation + news)

- **Canonical HE tab label: Guest Sentiment** (HE-QL-038). Replaces the former **Brand / Operator** tab label. Internal deep-link aliases `hexTab=brand|guest-sentiment|guest` resolve to `sentiment`. Brand / operator facts remain on Hotel Overview, Ownership Intelligence, and Relationships — do **not** duplicate Franchise & Brand Status or Current Operator inside Guest Sentiment.
- Present a **dashboard**: Guest Reputation (seven platforms) · Category Performance · Guest Experience Insights · Hotel News & Articles (when ≥1 property article) · collapsed Sources, Methodology & Limitations.
- **Seven-platform overview (HE-QL-029):** always show the canonical catalog (`REPUTATION_OVERVIEW_PLATFORMS_V1`) — Tripadvisor, Booking.com, Expedia, Google Reviews, Hotels.com, Agoda, Trivago. Available platforms show native rating, scale, review count, observation date, and listing link. Unavailable platforms stay visible as dimmed **Not connected** / **Data unavailable** cards — never invent 0 ratings, fabricated review counts, or summaries.
- **Dynamic Chain Scale accents (HE-QL-038):** reputation bars, active category selectors, hyperlinks, and insight titles use `DealalityChainScaleTheme` / `--accent--primary-1` for the selected hotel’s chain scale (e.g. Upper Upscale → `#9b59b6`). Do not hardcode one purple for all hotels. No arbitrary cyan/sky (`#38bdf8`) on reputation chrome. Hover may use Dealality gold `#fdb52a`.
- Show platform-native rating, scale, review count, observation date, and listing link per available source. **Never** silently average incompatible scales (e.g. Tripadvisor /5 vs Booking /10).
- **Category Performance** selectors include every platform that has published category scores. Platforms with overall ratings but no categories stay on the overview only — never invent category scores.
- **Guest Experience Insights** are compact executive rows: Relative Strengths · Areas to Investigate · Recommended Management Follow-Up · Trend & Data Confidence — short title + one-line explanation + supporting metric. No raw provider IDs, API names, or “reconcile platform property IDs” as the primary management action.
- Hotel News & Articles uses Market Alerts via `GET /api/hotel-explorer/hotels/:hotelId/news-articles` (independent of reputation). Hide the subsection when empty. Property-specific only — not generic brand-parent news.
- Detailed coverage/licensing lives in collapsed methodology + central Sources & Evidence — do not duplicate full source tables here.
- Live customer UI stays **hidden** unless `live_ui_authorized === true`. Founder Proposed Preview may render staged public-listing metadata with an explicit rights chip.
- No unauthorized paid provider calls; no GET-triggered writes; independent toast/wave loading (HE-QL-024 / HE-QL-028).
- Shared modules: `lib/hotel-intelligence/guest-sentiment/*`, `api/hotel-explorer-guest-sentiment.js`, Guest Sentiment host in `hotel-intelligence-workspace.js` / `hotel-explorer.js`.

### 9a-0. Relationships & Structure (no redundant radial Organization Network)

- Relationships & Structure presents **Hotel Structure** + **Relationship Detail** (filters, verified/proposed distinctions, source deep-links).
- Do **not** render a radial **Organization Network**, “Show Organization Network” control, empty `#hiwOrgNetwork` container, or on-demand mini-graph fetch for that tab.
- Stakeholder exploration belongs in **Ownership Intelligence** (OCN). Organization & Portfolio remains the portfolio surface. Do not duplicate those experiences on Relationships.
- Canonical HI organization/relationship intelligence and APIs remain; only the redundant radial presentation is removed (HE-QL-034).

### 9a-1. Organization & Portfolio — Key People & Strategic Context (HE-QL-035)

- **One shared contact intelligence model.** Organization & Portfolio Key People and Contact Intelligence must read the same canonical people pack (HI Org Contacts / Contact Channels / staged owner decision-maker research / HPC–HI graph). Do not ship a thinner parallel people array that blanks fields Contact Intelligence already shows.
- Key People is a **concise organization-level summary**, not a second contact database and not a Surfe enrichment UI. Required columns when known: Name · Current title · Organization · Relevance to the hotel · Commercial role · Property responsibility · Verified LinkedIn/profile · Verification status.
- **No empty fields when verified intelligence already exists** (relevance, title, organization, LinkedIn, verification, property-responsibility label). Do not show placeholder dashes for shared-model fields that are populated. Genuine gaps go to the research backlog — do not invent authority or property assignment.
- Property responsibility must distinguish **Confirmed** vs **Inferred portfolio** vs **Unknown / organization entry point**. Never claim property-assigned AM / signing authority without evidence.
- Names navigate to Contact Intelligence. Verified LinkedIn / company-profile URLs open independently. Surfe remains gated, user-triggered, session-only on Contact Intelligence — never auto-called for Key People rendering.
- Ranking / person identity must stay consistent between Organization Key People and Contact Intelligence (no duplicate competing person records for the same verified individual).
- **Strategic Context** is an evidence-based executive assessment for a knowledgeable hotel executive — not pipeline/workflow copy. Required substance: Asset & Market Position · Ownership & Portfolio Context · Commercial Strengths · Brand Development & Investment Considerations · Research Priorities / Priority Follow-Ups.
- Presentation: compact subsections (Asset & Market Position · Commercial Strengths · Strategic Considerations · Priority Follow-Ups). No oversized empty boxes, nested card stacks, inconsistent backgrounds, raw research jargon, unsupported sell/rebrand/refinance/management-change conclusions, or “Use Hotel → Owner → Portfolio…” boilerplate.
- Evidence access stays in **Sources & Evidence**. Generated narrative retains supporting facts, generation version, and research date; never overwrite canonical Airtable facts with commentary. Production Airtable apply remains blocked until authorized.
- Shared modules: `lib/hotel-intelligence/ownership/he-org-portfolio-key-people-v1.js`, founder-preview shared people builder, Organization renderers in `hotel-intelligence-workspace.js`.

### 9a. Ownership & Control Network / Stakeholder Network

- Ownership Intelligence presents a **Hotel Stakeholder & Deal Relationship Network** (interactive), not a linear hierarchy dump and not an unbounded “Show All” web.
- **Default view (hotel-centered):** selected hotel · legal property owner · economic owner/sponsor · current operator · current brand · asset manager when evidence-backed · UBO only when a named party is known. Do **not** auto-load all people, financing history, advisors, or portfolio hotels.
- **Relationship-group selectors:** Ownership & Control · Asset Management & People · Capital & Financing · Operations & Brand · Development & Advisors · Related Hotels. Empty categories show an honest research-status message — never fabricate edges.
- Ownership/control relationships render **above** the hotel; operations/brand render **below** — never imply a false linear hierarchy between sponsor, operator, lender, asset manager, and brand.
- Typed edges only (extended taxonomy allowed when evidence-backed): Legal / Economic / UBO / Managed / Brand / role-labeled person edges / Related hotel / Asset managed / Financed / Developed / Advised / Equity partner.
- **Never** draw operator or brand as an ownership chain step; never invent PropCo→sponsor control, financing terms, franchise expiration, or brand-conversion willingness.
- **Historical ≠ current:** historical lenders/advisors may appear only in their group, marked Historical, and never as current decision-makers.
- **Related Hotels** use a bounded **cluster node** (`Related Hotels (N)`) with list + HPC links in the detail panel — never dump every portfolio hotel onto the canvas.
- Person nodes show title + organization relevance; never a generic “Person” as the only explanation; people connect to their organization, not as direct hotel controllers unless evidence says so.
- Readable nodes: min ~13px body label, content-measured width/height, wrapped text, no overflow clipping; edge labels offset from midpoints; long titles/evidence belong in the **detail panel**.
- **Two independent measures:** Stakeholder Research Coverage (what was investigated/verified) vs Network Visualization Coverage (what is correctly rendered). Graph completeness ≠ research completeness.
- Visual + label differentiation for verified / proposed / historical / unresolved (not color alone).
- Tab structure: Executive Ownership Summary → Stakeholder Network → Relevant Decision-Makers (compact; header **View Contacts** link) → Relationship Detail ↔ graph highlight (header **View Sources** deep-links to Sources & Evidence). No inline Sources and Verification table on Ownership.
- Forbidden: node-text overflow, overlapping nodes/labels, unreadable typography, unbounded portfolio dumps, incorrect asset-manager attribution, historical lenders as current, unsupported ownership/control edges, duplicate hierarchy text blocks, raw graph node IDs / internal enums, HPC property-contact fallback panels, proposed styled as verified, deep links without resolved HPC IDs.
- Shared modules: `lib/hotel-intelligence/ownership/ownership-control-network-v1.js` + `public/js/he-ownership-control-network-v1.js`.

### 9b. Canonical HPC population (portfolio batches)

- Reuse `HPC_LIVE_IDENTITY_COLLISION_V1`, `hpc-shared-upsert-v1`, and HI graph contracts — do not create a parallel hotel database.
- Canonical base `appCCUsuGsE1ifoLk` / HPC `tbl9aY5ijiuIzzWam` — **one canonical hotel database**.
- Identity outcomes (runtime): `MATCHED_HPC` | `MATCHED_EXISTING_ALIAS` | `NEW_HPC_CANDIDATE` | `AMBIGUOUS_MATCH` | `IDENTITY_UNRESOLVED`. Legacy `MATCHED_HOTEL_CENSUS` is migration-tooling only.
- Default `applyAuthorized: false`. **STOP** before production Airtable apply until the dry-run CREATE/UPDATE packet is founder-approved.
- Ownership intelligence lives in HI Organizations / Relationships / Evidence — not HPC free-text portfolio dumps.
- Contract: `docs/ai-build-system/HE_HPC_POPULATION_WORKFLOW_V1.md`.

### 9c. Legacy Hotel Census independence (HE-QL-041)

**Mandatory gate:** `LEGACY_HOTEL_CENSUS_INDEPENDENCE_GATE`

- Legacy table `Hotel Census` / `tblgj2qEwxjTcg6q0` must **not** be read, written, updated, or used as a fallback on any Hotel Explorer production-intended execution path.
- Canonical identity resolution uses HPC names, aliases, former names, brand changes, addresses, coordinates, org relationships, and external property ids via `hpc-live-identity-collision-v1` (and related HPC utilities).
- Access guard: `lib/hotel-intelligence/hpc-population/legacy-hotel-census-access-guard-v1.js`. Default = block. Narrow migration reads require `HE_LEGACY_HOTEL_CENSUS_MIGRATION_READ=1` **and** explicit `allowLegacyHotelCensusRead` — never silent fallback.
- Legacy Census record ids (e.g. Secrets Papagayo `recyX1wLsHBJYRPw7`) may appear only as **historical provenance**, never as runtime canonical hotel ids.
- Regression: `npm run test:he-legacy-hotel-census-independence-v1`. Keep this gate after the Airtable table is eventually deleted.
- Cross-product Scout / Brand Explorer / operators-by-brand-region consumers may still use Hotel Census until migrated — that blocks **global** retirement (`CROSS_PRODUCT_RETIREMENT_BLOCKED`), not HE research-path independence.

### 10. Submarket Snapshot & Area Hotels (HPC)

- **HPC is the source of truth** for Submarket Snapshot hotel identity, rooms, status, brand, and market/submarket classifications (`appCCUsuGsE1ifoLk` / `tbl9aY5ijiuIzzWam`). Scout / Hotel Census / Radar map pools are **supporting enrichment only**.
- A valid canonical HPC hotel must **never** disappear from Submarket Snapshot because Scout returned zero rows or Radar has no loaded census (HE-QL-036).
- Always resolve the selected hotel’s HPC record and include it in the matched cohort when the record exists.
- Group by verified HPC Submarket when present; otherwise Market; otherwise city / selected-only with an explicit research flag. **Do not invent** market or submarket labels merely to fill the table.
- Deduplicate by canonical HPC identity; separate Open vs Pipeline; sum only known positive room counts (unknown rooms ≠ zero inventory).
- Incomplete coverage must be labeled (partial cohort ≠ complete submarket census). Never manufacture full-market totals or percentages from incomplete geography.
- Area Hotels radii by location setting (initial): Urban 5 mi, Suburban/Airport 10 mi, Resort 30 mi, Rural 25 mi — adjustable; distance unit display independent of inclusion.
- Shared module: `lib/hotel-intelligence/he-hpc-submarket-snapshot-v1.js` + `GET /api/hotel-explorer/hotels/:hotelId/submarket-snapshot` (GET-only).
- No GET-triggered writes; no unapproved Airtable submarket bulk apply. Geo-gap research uses dry-run artifacts only.

### 11. Demand Drivers & Access / Connectivity (Airtable reuse)

- **Do not rebuild** Demand Anchors or Travel Infrastructure research before recovering existing modules (`lib/demand-anchors/*`, `lib/travel-infrastructure/*`, Radar APIs).
- Canonical hotel identity is **HPC** (`appCCUsuGsE1ifoLk` / `tbl9aY5ijiuIzzWam`). Bethesda Marriott: `recLuxvwwxID7U2B8`.
- **Demand Drivers** read from reusable **Demand Anchors** (Platform base / `AIRTABLE_BASE_ID_ALT`). Entities are researched once and reused across hotels; hotel relevance = straight-line distance + optional verified rationale — **not** automatic confirmation of hotel demand from proximity alone.
- **Access & Connectivity** read from reusable **Travel Infrastructure Data** (same Platform base). Airports/rail/roads are shared geographic entities.
- Distances are **straight-line** from verified HPC coordinates unless Airtable holds a verified Estimated Drive Time. **Never invent** drive/transit times from distance heuristics.
- **mi / km** toggle appears on the Access & Connectivity tab when distances are shown — not as a global toolbar and not on Demand Drivers (HE-QL-037).
- Empty US / geography tables are honest empty states; tabs auto-populate when canonical records are imported — no hardcoded demo rows.
- Shared read model: `lib/hotel-intelligence/he-demand-access-read-v1.js` + `GET /api/hotel-explorer/hotels/:hotelId/demand-drivers` + `…/access-connectivity` (GET-only; no paid providers on tab open).
- Inventory: `reports/hotel-explorer/HE-DEMAND-ACCESS-AIRTABLE-RECOVERY-V1.md`.

## Machine-testable acceptance

See `scripts/test-he-product-quality-gates-v1.mjs` and the lessons registry `regressionTest` fields.

## Research / population workflow

New hotels must reuse HPC + HI intelligence, run the commercial description rubric, normalize amenities, render ownership/contact/rooms with shared components, and attach evidence. Human corrections update **shared rules** (this standard + registry + code), not one-off hotel data.

### HE 5.0 Full E2E engine (HE-QL-039)

Reusable path: `AUDIT → REUSE → RESEARCH → VERIFY → DEDUPLICATE → MAP → DRY_RUN → CONDITIONAL_APPROVAL → APPLY → RE_READ → UI_VALIDATE`.

- Module: `lib/hotel-intelligence/he-5-0-full-e2e/` · runner `npm run he:5-0-full-e2e-pilot` · gate `npm run test:he-5-0-full-e2e-pilot-v1`.
- Free-first research before any paid provider; shared paid ceiling must be cost-bounded (pilot: **$50** across the authorized cohort).
- Never auto-classify portfolio *candidates* as verified current holdings.
- Surfe remains click-only / session-only / nonpersistent.
- Second run with no new evidence must not duplicate records, overwrite verified facts, or re-spend unnecessarily.

### Autonomous reconciliation (HE-QL-040)

When the engine detects uncertain, incomplete, or conflicting hotel intelligence, it must **investigate autonomously** (reuse → free authoritative sources → optional bounded paid within the authorized ceiling), adjudicate with evidence rules, and continue other gaps.

- Do **not** stop the research batch merely to ask the founder about routine room-count conflicts, near-neighbor hotel names, dual renovation milestones, or missing identity fields.
- Resolve only when evidence supports a defensible result; preserve historical vs current context and contradictory sources.
- Classification vocabulary: `RESOLVED_VERIFIED` · `RESOLVED_WITH_HISTORICAL_CONTEXT` · `RESOLVED_NOT_APPLICABLE` · `UNRESOLVED_CONFLICT` · `UNRESOLVED_INSUFFICIENT_EVIDENCE` · `UNRESOLVED_PROVIDER_RESTRICTED` · `UNRESOLVED_BUDGET_LIMIT` · `REQUIRES_FOUNDER_DECISION`.
- Use `REQUIRES_FOUNDER_DECISION` only for genuine business / authorization / governance decisions — not ordinary factual disagreements.
- Runner: `npm run he:5-0-autonomous-reconciliation` · gate: `npm run test:he-5-0-autonomous-reconciliation-v1`.

### Standing conditional Airtable apply (HE-QL-042)

For the authorized eight-hotel pilot, do **not** stop for routine founder approval of each READY mutation.

- Standing gate: `HE50_STANDING_APPLY_GATE` · classifier `conditional-approval-v1` · executor `controlled-apply-executor-v1` · `npm run he:5-0-controlled-apply` / `:write`.
- Approval classes: `AUTO_APPROVED_*` · `SKIPPED_NO_CHANGE` · `QUARANTINED_INSUFFICIENT_EVIDENCE` · `BLOCKED_OUT_OF_SCOPE` · `FOUNDER_APPROVAL_REQUIRED`.
- Auto-apply only when destination/field allowlist, evidence, identity, null-fill/CREATE safety, idempotency, and pilot scope all pass. Quarantine exceptions and continue.
- Never write legacy Hotel Census; never Surfe persistence; never schema changes; never promote proposed Mullen holdings to verified ownership.
- Persistence stages: `RESEARCH_VERIFIED` → `CANONICAL_PERSISTED` → `AIRTABLE_READBACK_VALIDATED` → `HOTEL_EXPLORER_UI_VALIDATED`.
- Report statuses: `AUTO-APPROVED PILOT MUTATIONS APPLIED AND VALIDATED` · `PARTIAL AUTO-APPLY — EXCEPTIONS QUARANTINED` · `FOUNDER APPROVAL REQUIRED — OUT-OF-SCOPE OPERATION`.
- Escalate only for spend >$50, out-of-cohort writes, schema/destructive ops, ambiguous merges, unsupported ownership assertions, new subscriptions, or production deploy.

### Legacy Hotel Census independence (HE-QL-041)

See §9c. Gate `LEGACY_HOTEL_CENSUS_INDEPENDENCE_GATE` fails whenever an active HE production-intended path depends on `tblgj2qEwxjTcg6q0`.

## Safety

- Outside standing pilot authorization, no production Airtable apply / schema mutation / deploy from quality work alone.
- GET routes remain write-forbidden.
- Proposed ≠ Live.
