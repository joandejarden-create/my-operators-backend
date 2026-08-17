# Marriott CALA property-data pattern catalog

This catalog focuses on repeatable **official** Marriott-controlled source patterns that can support deterministic adapter design for CALA property discovery and Level 2 field extraction. The strongest repeatable path is: **(1) enumerate properties and MARSHA codes from Marriott hotel sitemaps / sitemap JSON, then (2) enrich with DAM PDFs and selected press releases for exact room counts, with limited fallback to HTML parsing on lower-risk brand domains.**

[1][6][7][4][8]
**Status at a glance:** hotel-sitemap pages and sitemap JSON are **WORKING** for enumeration; DAM PDFs are **CONDITIONAL** because URL discovery is the hard part; news.marriott.com press releases are **WORKING** but sparse; direct marriott.com property overview HTML and GraphQL detail/search APIs are **BLOCKED** or operationally unsafe for Autopilot use.

[1][2][3][4][5]
## Pattern type: Marriott sitemap discovery pages and sitemap JSON

Pattern name
URL template
MARSHA / property keying method
Available fields
Bot-block risk
Autopilot extraction safety
Status

Country hotel sitemap HTML + embedded `__NEXT_DATA__`
`https://www.marriott.com/en-us/hotel-sitemap/{country-slug}-hotel-sitemap`
Key by `{country-slug}`; parse hotel card links or `__NEXT_DATA__` entries to extract lowercase MARSHA from canonical URLs like `/en-us/hotels/{marsha}-{slug}/overview/`. The JSON entries also carry `marsha`, `title`, and `url`.
Address: No; Phone: No; Rooms: No; Coords: No
Low
Safe
**WORKING**

Sitemap index hierarchy
`https://www.marriott.com/robots.txt` → `/sitemap-index.xml` → `/content/dam/marriott-seo/en/marriott-tng/sitemap-hotel-sitemaps.xml` → country sitemap URLs; locale XML pages also appear under `/content/dam/marriott-hws/sitemap-xmls/`
Start from robots or sitemap index; enumerate country sitemaps, then extract property URLs and MARSHA from each property path.
Address: No; Phone: No; Rooms: No; Coords: No
Low
Safe
**WORKING**

Locale-path property URL enumeration
`https://www.marriott.com/es/hotels/{marsha}-{slug}/{pageType}/` in locale XML sitemaps such as `es-sitemap-hws-1.xml`
MARSHA remains embedded in the URL path; useful for enumerating alternate locale URLs and page types.
Address: No; Phone: No; Rooms: No; Coords: No
Low for sitemap XML itself; High if followed into overview HTML
Conditional
**CONDITIONAL**

[1][10][9][15]
[11][12][13][14]
**Concrete CALA examples**

- Mexico: [https://www.marriott.com/en-us/hotel-sitemap/mexico-hotel-sitemap](https://www.marriott.com/en-us/hotel-sitemap/mexico-hotel-sitemap) — includes JW Marriott Cancun Resort & Spa (`CUNJW`), Marriott Cancun, An All-Inclusive Resort (`CUNMX`), W Punta de Mita (`PVRWH`), and The St. Regis Mexico City (`MEXXR`).

- Costa Rica: [https://www.marriott.com/en-us/hotel-sitemap/costa-rica-hotel-sitemap](https://www.marriott.com/en-us/hotel-sitemap/costa-rica-hotel-sitemap) — includes Sheraton San Jose Hotel, Costa Rica (`SJOSI`) and JW Marriott Guanacaste Beach Resort (`SJOJW`).

- Colombia: [https://www.marriott.com/en-us/hotel-sitemap/colombia-hotel-sitemap](https://www.marriott.com/en-us/hotel-sitemap/colombia-hotel-sitemap) — includes JW Marriott Hotel Bogota (`BOGJW`), W Bogota (`BOGWH`), Sheraton Bogota Hotel (`BOGSI`), and Bogota Marriott Hotel (`BOGMC`).

[1]
[10]
[9]
## Pattern type: Direct marriott.com property pages and internal APIs

Pattern name
URL template
MARSHA / property keying method
Available fields
Bot-block risk
Autopilot extraction safety
Status

Direct property overview page
`https://www.marriott.com/{locale}/hotels/{marsha}-{slug}/overview/`
MARSHA is explicit in the URL path.
Address: Yes, from rendered HTML; Phone: Yes, from rendered HTML; Rooms: No confirmed; Coords: No confirmed
High
Unsafe
**BLOCKED**

Events / meetings page
`https://www.marriott.com/{locale}/hotels/{marsha}-{slug}/events/`
MARSHA in path, and confirmed again in meeting-RFP links such as `?marshaCode=cunjw`.
Address: Not established as repeatable; Phone: Not established as repeatable; Rooms: No guest-room count, but meeting room counts and event-space metrics are available; Coords: No
Medium
Conditional
**CONDITIONAL**

`phoenixShopHQVPropertyInfoCall` GraphQL detail endpoint
`POST https://www.marriott.com/mi/query/phoenixShopHQVPropertyInfoCall`
Would key by property / MARSHA in request payload plus Marriott safelisted GraphQL signature.
Address: Yes; Phone: Yes; Rooms: Not confirmed; Coords: No confirmed
High to Extreme
Unsafe
**BLOCKED**

`phoenixShopDatedSearchByDestinationQuery` GraphQL search endpoint
Fired behind `findHotels.mi` search flow
Key by destination and date range; response includes Marriott property IDs / URLs.
Address: No; Phone: No; Rooms: No; Coords: Yes
Extreme
Unsafe
**BLOCKED**

`findHotels.mi` search page
`https://www.marriott.com/search/findHotels.mi?...destinationAddress.destination={city}...`
Key by city / search request, not directly by MARSHA.
Address: No confirmed; Phone: No; Rooms: No; Coords: Not directly in static response
Extreme
Unsafe
**BLOCKED**

[4]
[17]
[8][16]
**Concrete CALA examples**

- JW Marriott Cancun overview: [/en-us/hotels/cunjw-jw-marriott-cancun-resort-and-spa/overview/](https://www.marriott.com/en-us/hotels/cunjw-jw-marriott-cancun-resort-and-spa/overview/) — HTML contains address and phone, but no structured JSON payload and no room count.

- JW Marriott Cancun events: [/en-us/hotels/cunjw-jw-marriott-cancun-resort-and-spa/events/](https://www.marriott.com/en-us/hotels/cunjw-jw-marriott-cancun-resort-and-spa/events/) — working supplemental page for event-space metrics and MARSHA confirmation.

- Spanish locale sitemap-backed URL pattern: [es-sitemap-hws-1.xml](https://www.marriott.com/content/dam/marriott-hws/sitemap-xmls/es-sitemap-hws-1.xml) — confirms locale property paths like `/es/hotels/{marsha}-{slug}/overview/`, but does not reduce block risk when you fetch the overview page itself.

## Pattern type: Official Marriott DAM PDFs and newsroom pages

Pattern name
URL template
MARSHA / property keying method
Available fields
Bot-block risk
Autopilot extraction safety
Status

Marriott DAM fact-sheet PDFs
`https://www.marriott.com/content/dam/marriott-digital/{brand-prefix}/cala/hws/{first-letter}/{marsha}/en_us/document/assets/{prefix}-{marsha}-fact-sheet-{assetId}.pdf` or close variants such as `hotel-fact-sheet`
Key by brand prefix + lowercase MARSHA + first letter directory; the hard part is discovering the property-specific trailing asset ID.
Address: Yes; Phone: Yes; Rooms: Yes; Coords: Not confirmed
Low
Conditional
**CONDITIONAL**

Marriott DAM event / meeting brochure PDFs
Same DAM path family, with names such as `meeting-event-brochure` or `event-fact-sheet`
Key by brand prefix + MARSHA + DAM asset discovery.
Address: Sometimes; Phone: Sometimes; Rooms: Sometimes; Coords: No confirmed
Low
Conditional
**CONDITIONAL**

news.marriott.com opening / renovation press releases
`https://news.marriott.com/news/{YYYY}/{MM}/{DD}/{slug}`
Key by property-name search; join back to MARSHA via sitemap mapping or article body context.
Address: Sometimes; Phone: Sometimes; Rooms: Yes; Coords: No
Low
Safe
**WORKING**

[6][2][21][22]
[3][7][19]
**Concrete CALA examples**

- Fairfield by Marriott Luquillo Beach (`SJULU`): [official fact sheet PDF](https://www.marriott.com/content/dam/marriott-digital/fi/cala/hws/s/sjulu/en_us/document/assets/fi-sjulu-fact-sheet-23971.pdf) — 104 rooms, address `110 Seaside Drive, Luquillo, PR 00773`, phone `787.657.0000`.

- Courtyard by Marriott Guayaquil (`GYECY`): [official hotel fact sheet PDF](https://www.marriott.com/content/dam/marriott-digital/cy/cala/hws/g/gyecy/en_us/document/assets/cy-gyecy-11-23-hotel-fact-sheet-19316.pdf) — 144 rooms.

- Courtyard by Marriott Guayaquil (`GYECY`): [official event fact sheet PDF](https://www.marriott.com/content/dam/marriott-digital/cy/cala/hws/g/gyecy/en_us/document/assets/cy-gyecy-12-23-event-fact-sheet-10703.pdf) — reconfirms 144 rooms and 7 meeting rooms.

[6]
[2]
[20]
**Additional newsroom examples**

- JW Marriott Cancun Resort & Spa (`CUNJW`): [official press release](https://news.marriott.com/news/2019/01/30/jw-marriott-cancun-resort-spa-unveils-reimagined-guestrooms-infused-with-modern-design-elements) — 447 guest rooms including 74 suites.

- Marriott Cancun, An All-Inclusive Resort (`CUNMX`): [official press release](https://news.marriott.com/news/2024/07/30/an-all-inclusive-awakening-marriott-hotels-elevates-the-art-of-hospitality-with-new-cancun-resort) — 450 guestrooms.

- The St. Regis Costa Mujeres Resort, Cancun (`CUNCM`): [official opening press release](https://news.marriott.com/news/2026/06/23/the-st-regis-costa-mujeres-resort-cancun-debuts-as-a-refined-enclave-between-land-and-sea-in-mexico) — 163 guestrooms and 50 suites; the earlier [2022 announcement](https://news.marriott.com/news/2022/11/02/st-regis-hotels-resorts-to-debut-in-costa-mujeres-mexico) cited 158 guestrooms plus 80 branded residences, so the opening release is the stronger current room-count source.

[3]
[7]
[18][19]
## Pattern type: Marriott-affiliated microsites and brand domains

Pattern name
URL template
MARSHA / property keying method
Available fields
Bot-block risk
Autopilot extraction safety
Status

`modules.marriott.com` collection microsites
`https://modules.marriott.com/{collection-slug}/`
Usually key by collection slug; infer property membership from page copy and MARSHA from linked meeting-RFP URLs where present.
Address: Yes; Phone: Yes; Rooms: Sometimes, but often aggregated across multiple properties; Coords: No confirmed
Medium / Unknown
Conditional
**CONDITIONAL**

Ritz-Carlton property pages on `ritzcarlton.com`
`https://www.ritzcarlton.com
## Pattern type: Marriott-affiliated microsites and brand domains

Pattern name
URL template
MARSHA / property keying method
Available fields
Bot-block risk
Autopilot extraction safety
Status

modules.marriott.com` collection microsites
`https://modules.marriott.com/{collection-slug}/`
Key by collection slug; infer property membership from page copy and linked RFP URLs that contain `marshaCode=`.
Address: Yes; Phone: Yes; Rooms: Sometimes, but often only aggregated across multiple properties; Coords: No confirmed
Medium
Conditional
**CONDITIONAL**

Ritz-Carlton property pages on `ritzcarlton.com`
`https://www.ritzcarlton.com/en/hotels/{marsha}-{slug}/overview/`
MARSHA is explicit in the URL path for tested property pages.
Address: Yes via HTML parsing; Phone: Yes via HTML parsing; Rooms: Sometimes in body text; Coords: No confirmed
Medium
Conditional
**CONDITIONAL**

Brand subdomains such as `jw-marriott.marriott.com` and `st-regis.marriott.com`
Brand-home URLs, then outbound links to property pages
Discovery by brand and outbound property links, not directly by MARSHA.
Address: No; Phone: No; Rooms: No; Coords: No
Low
Conditional
**CONDITIONAL**

[23]
[24][25][26]
[4][23][24][1]
**Concrete CALA examples**

- Cancun collection microsite: [modules.marriott.com/cancun-resorts-and-spa-kukulcan-boulevard/](https://modules.marriott.com/cancun-resorts-and-spa-kukulcan-boulevard/) — aggregated data for `CUNJW` and `CUNMX`, including contact content and combined guestroom scale.

- Zadún Los Cabos, a Ritz-Carlton Reserve: [ritzcarlton.com/en/hotels/sjdzr-zadun-los-cabos-a-ritz-carlton-reserve/overview/](https://www.ritzcarlton.com/en/hotels/sjdzr-zadun-los-cabos-a-ritz-carlton-reserve/overview/) — URL contains `SJDZR`; body text states 113 rooms, suites and villas.

- The Ritz-Carlton, Mexico City: [ritzcarlton.com/en/hotels/mexrz-the-ritz-carlton-mexico-city/overview/](https://www.ritzcarlton.com/en/hotels/mexrz-the-ritz-carlton-mexico-city/overview/) — URL contains `MEXRZ`; viable official brand-domain pattern for HTML fallback extraction.

## Final summary table

Pattern type
Pattern name
Classification
Why

Sitemaps
Country hotel sitemap HTML + `__NEXT_DATA__`
**WORKING**
Best deterministic Marriott-controlled enumeration source for CALA; low bot risk; exposes MARSHA and canonical URLs.

Sitemaps
robots.txt → sitemap-index.xml → hotel sitemap XML chain
**WORKING**
Deterministic discovery chain for country sitemap coverage and locale URL enumeration.

Sitemaps
Locale XML sitemaps for property URLs
**CONDITIONAL**
Useful for URL discovery only; following locale overview pages inherits high block risk.

Property pages
Direct marriott.com overview HTML
**BLOCKED**
High Akamai risk and no structured payloads; address/phone require brittle HTML parsing.

Property pages
Marriott events / meetings pages
**CONDITIONAL**
Good for meeting metrics and MARSHA confirmation, but not guest room counts.

APIs
`phoenixShopHQVPropertyInfoCall`
**BLOCKED**
Safelisted GraphQL endpoint requiring harvested signatures and Akamai bypass.

APIs
`phoenixShopDatedSearchByDestinationQuery`
**BLOCKED**
Search API is behind the same protected stack; not suitable for lightweight public extraction.

APIs
`findHotels.mi`
**BLOCKED**
403 from direct access; operationally unsafe as an adapter dependency.

Official documents
Marriott DAM fact-sheet PDFs
**CONDITIONAL**
Excellent source for rooms/address/phone when URL is known, but discovery of the exact asset ID is not deterministic from MARSHA alone.

Official documents
Marriott DAM event / meeting brochures
**CONDITIONAL**
Low-risk and often useful, but URL discovery remains the blocker and room counts are inconsistent.

Official documents
news.marriott.com press releases
**WORKING**
Low-risk official source for exact room counts on openings and renovations, though coverage is selective.

Brand domains
`modules.marriott.com` microsites
**CONDITIONAL**
Official and sometimes rich, but often aggregate multiple properties and lack structured payloads.

Brand domains
`ritzcarlton.com` property pages
**CONDITIONAL**
Official fallback for Ritz-Carlton family properties, but extraction is HTML-only and room counts are not guaranteed.

Brand domains
Marriott brand subdomains
**CONDITIONAL**
Useful for discovery and cross-linking, not for Level 2 fields.

[1][11][4][8][6][3][23][24]
## Appendix: rejected source types

Rejected source type
Reason for rejection

OTAs such as Booking.com, Expedia, and Tripadvisor
Not official Marriott-controlled sources; the user explicitly excluded them from production sourcing.

Google Maps / Google hotel listings
Not official Marriott-controlled source material; excluded by instruction and unsuitable as canonical census data.

Third-party B2B hotel sales sites such as ConferenceHotelGroup
Can surface useful copies of official fact sheets and room counts, but they are not official Marriott domains, so they should be used only as research aids, not production sources.

Third-party scraping wrappers such as Apify and Parse.bot
Useful as proof that the data is technically extractable, but not acceptable as the underlying source of record for a Marriott adapter.

Standalone or community resort sites such as doradobeach.com and puntamita.com
Not canonical Marriott property sources for this task; they may carry supplemental marketing facts, but they are not reliable Marriott-controlled Level 2 data endpoints.

`mobile.marriott.com`
Rejected because the domain did not resolve and appears decommissioned.

Marriott developer portal
Official but gated behind enterprise authentication, so not publicly usable for autonomous extraction.

[27][28][29][30][31][32]
**Recommended engineering takeaway:** build the adapter around **sitemap-first MARSHA enumeration**, then attach **official DAM PDFs** and **news.marriott.com** enrichment where available. Treat direct marriott.com overview/API scraping as non-primary due to block risk, and reserve **ritzcarlton.com** or **modules.marriott.com** for narrow brand-specific fallback cases.

[1][6][7][23][24]

# Alternate URL & Sitemap Patterns

## Alternate URL & Sitemap Patterns

### Pattern 1: Country-Level Hotel Sitemap Pages — ⭐ WORKING / HIGH-VALUE

**Discovery path:** robots.txt → sitemap-index.xml → sitemap-hotel-sitemaps.xml → country-level sitemaps

**URL template:**

https://www.marriott.com/en-us/hotel-sitemap/{country-slug}-hotel-sitemap
**Keying:** By country name slug (e.g. "mexico", "costa-rica", "colombia", "dominican-republic", "panama").

**What it returns:** A fully-rendered HTML page listing every Marriott property in that country, grouped by brand tier (LUXURY, PREMIUM, SELECT, EXTENDED), with each hotel name linked to its overview page. The overview page URL embeds the MARSHA code directly in the path pattern: `/en-us/hotels/{marshaCode}-{property-slug}/overview/`

**Available fields:** Hotel name, brand category, MARSHA code (extractable from URL), property slug, and a direct link to the overview page.

**Bot-block risk:** LOW. These are SEO-friendly HTML pages served from marriott.com without Akamai bot-detection. Loaded successfully in testing.

**Autopilot suitability:** HIGH. Deterministic URL pattern, predictable HTML structure, country-level coverage. Can be used as a discovery mechanism to enumerate all MARSHA codes for a region.

**CALA examples confirmed working:**

- Mexico: [https://www.marriott.com/en-us/hotel-sitemap/mexico-hotel-sitemap](https://www.marriott.com/en-us/hotel-sitemap/mexico-hotel-sitemap) — lists 80+ hotels including JW Marriott Cancun (CUNJW), W Punta de Mita (PVRWH), The St. Regis Mexico City (MEXXR)

- Costa Rica: [https://www.marriott.com/en-us/hotel-sitemap/costa-rica-hotel-sitemap](https://www.marriott.com/en-us/hotel-sitemap/costa-rica-hotel-sitemap) — 25 hotels including Sheraton San Jose (SJOSI), JW Marriott Guanacaste (SJOJW)

[9][10]
### Pattern 2: Sitemap Index Hierarchy — ⭐ WORKING / DISCOVERY

**Discovery chain:**

- `https://www.marriott.com/robots.txt` → declares Sitemap: `https://www.marriott.com/sitemap-index.xml`

- `sitemap-index.xml` → contains references to `/content/dam/marriott-seo/en/marriott-tng/sitemap-hotel-sitemaps.xml`

- `sitemap-hotel-sitemaps.xml` → lists country-level hotel sitemap URLs including ALL CALA countries

- Also: `/content/dam/marriott-hws/sitemap-xmls/es-sitemap-hws-1.xml` through es-sitemap-hws-7.xml — locale-specific XML sitemaps with individual hotel page URLs, lastmod dates, and change frequencies

**Confirmed CALA country sitemaps from sitemap-hotel-sitemaps.xml:**

- mexico-hotel-sitemap

- costa-rica-hotel-sitemap

- colombia-hotel-sitemap

- dominican-republic-hotel-sitemap

- panama (listed as panama-hotel-sitemap would follow pattern)

- puerto-rico-hotel-sitemap

- aruba-hotel-sitemap

- chile-hotel-sitemap, peru-hotel-sitemap, ecuador-hotel-sitemap, brazil-hotel-sitemap, and many more

**XML sitemap format (from es-sitemap-hws-1.xml):** Each entry: URL, lastmod date, changefreq (e.g. "weekly"). URLs follow the pattern: `/es/hotels/{marshaCode}-{slug}/{pageType}/` where pageType is overview, rooms, dining, reviews, photos, events, etc.

[6][7][8][4]
### Pattern 3: Overview Page (Direct URL) — BLOCKED for automated use ⚠️

**URL template:** `https://www.marriott.com/{locale}/hotels/{marshaCode}-{property-slug}/overview/`

**Keying:** By MARSHA code embedded in URL path (e.g. CUNJW, SJOSI).

**What we found:** The overview page RENDERED successfully in this session (no Akamai block this time). However, the page HTML contains NO structured data payloads:

- No `__NEXT_DATA__` — NOT a Next.js app

- No `window.__INITIAL_STATE__`

- No `<script type="application/ld+json">`

- No `<script type="application/json">`

- No JSON-LD Hotel or LodgingBusiness objects

- No embedded MARSHA codes or property IDs in source

**Available fields (from the rendered HTML, CUNJW example):**

- Address: "Blvd. Kukulcan, Km 14.5, Lote 40-A, Zona Hotelera, Cancun, Quintana Roo, Mexico, 77500" — found in "GETTING HERE" section

- Phone: "+52 998-8489600" — found in "GETTING HERE" section

- No guest room count anywhere on overview page

- No geo-coordinates

- Carbon Footprint: 72.63 kg per room night (sustainability section)

- Water Footprint: 2403.26 liters per room night

**Bot-block risk:** HIGH for automated scraping. The page is Akamai-protected. While it loaded this session, the user reports Akamai blocking as the primary problem.

**Autopilot suitability:** LOW. No structured data to extract; address/phone require fragile HTML parsing of the "GETTING HERE" section. Room count is absent.

[5]
### Pattern 4: Locale Path Variations — LIKELY WORKING but same content

**URL templates:**

https://www.marriott.com/es/hotels/{marshaCode}-{slug}/overview/
https://www.marriott.com/ja/hotels/{marshaCode}-{slug}/overview/
https://www.marriott.com/fr/hotels/{marshaCode}-{slug}/overview/

**Confirmed:** The overview page for CUNJW links to these locale variants: Español (/es/...), 日本語 (/ja/...), 繁體中文 (/zh-hk/...). The XML sitemaps (es-sitemap-hws-1.xml through es-sitemap-hws-7.xml) confirm Spanish locale URLs exist for thousands of properties.

**Verdict:** Locale paths serve the same content on the same domain — same Akamai risk. No structural advantage over /en-us/.

[4]
### Pattern 5: mobile.marriott.com — BLOCKED / DEAD ❌

**URL:** `https://mobile.marriott.com/`

**Result:** DNS resolution failed. The domain does not exist or has been decommissioned. Not a viable pattern.

### Pattern 6: Ritz-Carlton Brand TLD — WORKING / DIFFERENT INFRA ⭐

**URL:** `https://www.ritzcarlton.com/en/hotels/{region}/{city}/`

**What we found:** The Ritz-Carlton site is on a separate domain (ritzcarlton.com) and serves destination/city listing pages. These list all Ritz-Carlton properties in a region. The CALA section includes: The Ritz-Carlton, Mexico City; The Ritz-Carlton, St. Thomas; The Ritz-Carlton, Grand Cayman; The Ritz-Carlton, Aruba; The Ritz-Carlton, Turks & Caicos; Zadun Los Cabos (Reserve); Dorado Beach (Reserve); The Ritz-Carlton, Santiago; and upcoming Nekajui and Siari Reserves.

**Crucial finding:** The Ritz-Carlton site does NOT use the MARSHA code in URLs. Property pages use a different URL scheme. This TLD may have different CDN/bot protection than marriott.com.

**Bot-block risk:** UNKNOWN but likely lower — different domain, different CDN configuration.

[2][3]
### Pattern 7: Marriott Events/Meetings Pages — WORKING / SUPPLEMENTAL

**URL template:** `https://www.marriott.com/{locale}/hotels/{marshaCode}-{slug}/events/`

**What it reveals:** Meeting room count, total event space (sq m/sq ft), largest capacity, breakout room count. The MARSHA code is also confirmed via the meeting RFP link: `https://www.marriott.com/meetings/erfp-schedule-meeting.mi?marshaCode=cunjw`

**Available fields:** Number of event rooms, total event space, max capacity, breakout rooms — but NOT guest room count.

**Usefulness:** Confirms MARSHA code; provides meeting space data for clustering but not guest rooms.

[1]
## Summary of Working vs. Blocked Patterns

Pattern
Status
MARSHA Extractable
Address
Phone
Guest Rooms
Bot Risk

Country Hotel Sitemaps
✅ WORKING
Yes (from URLs)
No
No
No
Low

Sitemap XML Index
✅ WORKING
Yes (from URLs)
No
No
No
Low

Overview Page (direct)
⚠️ BLOCKED
Yes (in URL)
Yes (HTML)
Yes (HTML)
No
High

Locale paths (/es/, /ja/)
⚠️ BLOCKED
Yes
Same as overview
Same as overview
No
High

mobile.marriott.com
❌ DEAD
N/A
N/A
N/A
N/A
N/A

Ritz-Carlton TLD
✅ WORKING
No (different scheme)
TBD
TBD
TBD
Likely Lower

Events/Meetings pages
✅ WORKING
Yes (in RFP link)
TBD
TBD
No (meeting rooms only)
Medium

**Additional confirmed CALA country sitemaps:**

- Colombia: [https://www.marriott.com/en-us/hotel-sitemap/colombia-hotel-sitemap](https://www.marriott.com/en-us/hotel-sitemap/colombia-hotel-sitemap) — 25 hotels including JW Marriott Bogota (BOGJW), W Bogota (BOGWH), Sheraton Bogota (BOGSI), Bogota Marriott (BOGMC)

- Dominican Republic: [https://www.marriott.com/en-us/hotel-sitemap/dominican-republic-hotel-sitemap](https://www.marriott.com/en-us/hotel-sitemap/dominican-republic-hotel-sitemap) — 30 hotels including JW Marriott Santo Domingo (SDQJW), W Punta Cana (PUJWH), The St. Regis Cap Cana (PUJXr), The Ocean Club Costa Norte (POPLC)

- Panama: pattern confirmed from sitemap-hotel-sitemaps.xml — `/en-us/hotel-sitemap/panama-hotel-sitemap`

**CALA MARSHA Code Catalog (10 confirmed):**

Property
MARSHA
Country
Sitemap Source

JW Marriott Cancun Resort & Spa
CUNJW
Mexico
mexico-hotel-sitemap

Marriott Cancun, An All-Inclusive Resort
CUNMX
Mexico
mexico-hotel-sitemap

W Punta de Mita
PVRWH
Mexico
mexico-hotel-sitemap

The St. Regis Mexico City
MEXXR
Mexico
mexico-hotel-sitemap

The St. Regis Costa Mujeres Resort
CUNCM
Mexico
mexico-hotel-sitemap

Sheraton San Jose Hotel, Costa Rica
SJOSI
Costa Rica
costa-rica-hotel-sitemap

JW Marriott Guanacaste Beach Resort
SJOJW
Costa Rica
costa-rica-hotel-sitemap

JW Marriott Hotel Bogota
BOGJW
Colombia
colombia-hotel-sitemap

JW Marriott Hotel Santo Domingo
SDQJW
Dominican Rep.
dominican-republic-hotel-sitemap

Fairfield by Marriott Luquillo Beach
SJULU
Puerto Rico
puerto-rico-hotel-sitemap

[11][12][8]

# API & Embedded JSON Payloads

## API & Embedded JSON Payloads — Marriott Property Data

### Pattern 1: Hotel-Sitemap __NEXT_DATA__ Embedded JSON — ⭐ WORKING / HIGH-VALUE

**Discovery:** The country-level hotel sitemap pages (e.g., `/en-us/hotel-sitemap/mexico-hotel-sitemap`) are rendered as a **Next.js application**. The page source contains a massive `<script id="__NEXT_DATA__" type="application/json">` block (~259KB for Mexico) with structured property data including MARSHA codes, brand codes, property titles, and canonical URLs.

**URL template:**

https://www.marriott.com/en-us/hotel-sitemap/{country-slug}-hotel-sitemap
**Keying:** By country slug only — returns ALL properties in that country.

**Extraction method:** Extract the `__NEXT_DATA__` script block, parse as JSON, then navigate to the property listing array within the pageProps model. Each entry has `marsha`, `title`, and `url` fields. Properties are grouped by brand tier and brand code.

**JSON structure (from Mexico sitemap):**

{"marsha":"cunjw","title":"JW Marriott Cancun Resort & Spa","url":"https://www.marriott.com/en-us/hotels/cunjw-jw-marriott-cancun-resort-and-spa/overview/"}
**Available fields:** MARSHA code, property name, brand tier, brand code, canonical overview URL. **NOT included:** address, phone, room count, coordinates.

**Bot-block risk:** LOW. The page loaded with a plain requests.get() with standard headers — no Akamai block. The `__NEXT_DATA__` JSON is in the raw page source before JS execution.

**Autopilot suitability:** HIGH for MARSHA discovery and property enumeration. Deterministic URL pattern, predictable JSON structure, low bot risk. Provides the core mapping needed to key other sources. Combine this with DAM PDF factsheets or press releases for room counts.

**CALA URLs confirmed:**

- [mexico-hotel-sitemap](https://www.marriott.com/en-us/hotel-sitemap/mexico-hotel-sitemap) — ~80+ properties

- [costa-rica-hotel-sitemap](https://www.marriott.com/en-us/hotel-sitemap/costa-rica-hotel-sitemap) — ~25 properties

- [colombia-hotel-sitemap](https://www.marriott.com/en-us/hotel-sitemap/colombia-hotel-sitemap) — ~25 properties

- [dominican-republic-hotel-sitemap](https://www.marriott.com/en-us/hotel-sitemap/dominican-republic-hotel-sitemap) — ~30 properties

### Pattern 2: phoenixShopHQVPropertyInfoCall GraphQL Endpoint — BLOCKED for direct use ❌

**Endpoint:** `POST https://www.marriott.com/mi/query/phoenixShopHQVPropertyInfoCall`

**What it does:** Apollo GraphQL query that returns per-property detail: address, phone, check-in/out times, policies, airports, amenities. This is Marriott's internal GraphQL API — the same endpoint that powers the Next.js property pages.

**Testing results (all CUNJW and SJOSI attempts):**

- Standard POST → 403 Access Denied (AkamaiGHost)

- POST with `graphql-operation-signature` header → 403 Access Denied

- POST with `graphql-require-safelisting: true` → 403 Access Denied

**Why it's blocked:** This endpoint is **safelisted** — the `graphql-operation-signature` header must match a valid signature that Marriott's Next.js app issues. Per Scrapfly's 2026 analysis: *"The detail call is safelisted. It needs a graphql-operation-signature header matching one Marriott issues, so a guessed value fails. The maintained scraper harvests it from the operationSignatures list in a rendered search page's __NEXT_DATA__ payload."*

**Required flow to use:**

- Render a Marriott search page (`findHotels.mi`) through a residential proxy with JS rendering to bypass Akamai

- Extract `__NEXT_DATA__` → `props.pageProps.operationSignatures`

- Find the signature for `phoenixShopHQVPropertyInfoCall`

- POST to the endpoint with `graphql-operation-signature` and `graphql-require-safelisting: true` headers

**Available fields (per Scrapfly):** `marriott_id`, `name`, `address`, `city`, `state`, `postal_code`, `country`, `phone`, `check_in`, `check_out`, `smoke_free`, `pets_allowed`, `parking`, `airports`. **Room count NOT confirmed** in this call — it may be in a separate query.

**Bot-block risk:** EXTREME. Requires residential proxy, JS rendering, Akamai bypass, and signature harvesting. Not viable for lightweight Autopilot extraction.

**Autopilot suitability:** VERY LOW. The signature changes over time and requires maintaining a full Akamai bypass pipeline.

### Pattern 3: phoenixShopDatedSearchByDestinationQuery — BLOCKED for direct use ❌

**Endpoint:** Apollo GraphQL query fired by Marriott's Next.js app after loading `findHotels.mi`

**What it returns:** Property list with MARSHA IDs, names, brand, latitude/longitude, distance, descriptions, review ratings, lead price, currency, bookable status, thumbnail URLs.

**Available fields:** `marriott_id`, `name`, `url`, `brand`, `latitude`, `longitude`, `distance_meters`, `description`, `review_rating`, `review_count`, `thumbnail`, `bookable`, `currency`, `lead_price`. **NOT included:** address, phone, room count.

**Bot-block risk:** EXTREME. Requires same Akamai bypass as Pattern 2.

**Autopilot suitability:** VERY LOW.

### Pattern 4: findHotels.mi Search Page — BLOCKED for direct use ❌

**URL template:** `https://www.marriott.com/search/findHotels.mi?searchType=InCity&destinationAddress.destination={city}&fromDate={MM/DD/YYYY}&toDate={MM/DD/YYYY}&numberOfRooms=1&numAdultsPerRoom=2`

**Testing:** Direct GET from datacenter IP → 403 Akamai. Requires residential proxy + JS rendering.

### Pattern 5: Marriott Developer Portal (devportalprod.marriott.com) — GATED 🔒

**URL:** `https://devportalprod.marriott.com/`

**Endpoints documented:**

- `GET /properties?location=&brand=&radius=` — Search properties

- `GET /properties/:propertyId` — Get property details (amenities, location, contact details)

- `GET /availability?propertyId=&checkIn=&checkOut=&guests=`

- `POST /reservations`

**Access:** Requires login with LDAP credentials. The portal states: *"Need access? Click here for instructions"* linking to a Marriott Atlassian wiki page. This is Marriott's internal/partner API, not a public endpoint.

**Verdict:** Not usable for public data extraction. Listed for reference as the official API exists but is gated behind enterprise authentication.

### Pattern 6: Parse.bot / Apify Wrapped APIs — THIRD-PARTY (reference only) 🔍

Two maintained third-party wrappers expose Marriott data via structured REST APIs:

- **Parse.bot:** `list_hotels` returns MARSHA codes, addresses, coordinates, phones by brand. `get_hotel_details` returns amenities, year_built, has_spa, has_golf, number_of_rooms.

- **Apify (BowTiedRaccoon):** Marriott Bonvoy Directory scraper extracts MARSHA IDs, addresses, GPS coordinates, phones, brand, star ratings from sitemaps + property pages.

**Verdict:** These prove the data IS extractable, but they rely on managed Akamai bypass infrastructure. For Dealality's own adapter: the underlying source is the hotel-sitemap __NEXT_DATA__ + individual property page scraping.

## Summary: API & JSON Payloads

Pattern
Status
Structured Data
MARSHA
Address/Phone
Guest Rooms
Coords
Bot Risk

Hotel-Sitemap __NEXT_DATA__
✅ WORKING
Yes (JSON)
Yes
No
No
No
Low

phoenixShopHQVPropertyInfoCall
❌ BLOCKED (safelisted)
Yes (GraphQL)
Yes
Yes
TBD
No
Extreme

phoenixShopDatedSearchByDestination
❌ BLOCKED
Yes (GraphQL)
Yes
No
No
Yes
Extreme

findHotels.mi
❌ BLOCKED (403)
Yes (via JS)
Yes
No
No
No
Extreme

Developer Portal
🔒 GATED
Yes (REST)
TBD
TBD
TBD
TBD
N/A (auth)

Destination Pages (.mi)
⚠️ TIMED OUT
TBD
TBD
TBD
TBD
TBD
High

### Key Architectural Insight (Scrapfly 2026)

Marriott's storefront is a **Next.js application backed by Apollo GraphQL**. All data loads as JSON after the page boots. The initial HTML is an empty shell. Two separate GraphQL calls serve data: one for search results (with lead price + coordinates), one for per-property detail (with address + phone). Both are behind Akamai. The **hotel-sitemap pages are the exception** — they serve __NEXT_DATA__ in the raw HTML without requiring JS execution.

# Official Factsheets & Media Kits

## Official Factsheets & Media Kits — Marriott Property Data

### Pattern 1: Marriott DAM PDF Factsheets — ⭐ WORKING / HIGH VALUE

**URL template:** `https://www.marriott.com/content/dam/marriott-digital/{locale}/{region}/hws/{marshaFirstLetter}/{marshaCode}/en_us/document/assets/fi-{marshaCode}-fact-sheet-{dateCode}.pdf`

**Example confirmed working:**

- **Fairfield by Marriott Luquillo Beach** (MARSHA: SJULU): [fi-sjulu-fact-sheet-23971.pdf](https://www.marriott.com/content/dam/marriott-digital/fi/cala/hws/s/sjulu/en_us/document/assets/fi-sjulu-fact-sheet-23971.pdf) — 104 rooms, address (110 Seaside Drive, Luquillo, PR 00773), phone (787.657.0000)

**Pattern breakdown:**

/content/dam/marriott-digital/{brand-prefix}/{region}/hws/{first-letter}/{marshaCode}/en_us/document/assets/{prefix}-{marshaCode}-fact-sheet-{id}.pdf

- `brand-prefix`: "fi" for Fairfield Inn, likely "jw", "si", "wh", etc.

- `region`: "cala" for Caribbean/Latin America, "us-canada", "emea", "apac"

- `first-letter`: First letter of MARSHA code (e.g., "s" for SJULU)

- `marshaCode`: Lowercase MARSHA code

- `dateCode/id`: Unclear — appears to be an internal asset ID

**Available fields in the PDF:** Guest room count (exact), suites count, meeting space dimensions/capacities, address, phone, website URL, amenities.

**Bot-block risk:** LOW. These PDFs are served from the /content/dam/ path (AEM DAM — Adobe Experience Manager Digital Asset Manager) which is a separate CDN path from the HWS Akamai-protected pages.

**Autopilot suitability:** HIGH. PDF extraction is deterministic. The PDF contains structured data tables. Room count and address are always present. The challenge is discovering the exact PDF URL — needs the MARSHA code and asset ID.

[3]
### Pattern 2: Meeting & Events Brochure PDFs — ⭐ WORKING / SUPPLEMENTAL

**URL template:** Similar to factsheets but named "meeting-event-brochure" instead of "fact-sheet".

**Example from search:** `si-amssi-meeting-event-brochure-35925.pdf` — confirms this naming convention exists.

**Available fields:** Meeting room counts, capacities, floor plans — rarely guest room counts.

**Bot-block risk:** LOW (same DAM path).

[2]
### Pattern 3: Guest Room Brochure PDFs — ⭐ WORKING

**Example from search:** `wi-snawi-guest-rooms-brochure-37741.pdf` — contains exact room type breakdowns ("376 total guest rooms... 261 with 1 King bed... 116 with 2 Queen beds... 12 One Bedroom Junior Suites")

**Bot-block risk:** LOW (same DAM path).

[1]
## Key Challenge: PDF URL Discovery

The DAM PDFs are the best source for room counts, but their URLs are not discoverable from the hotel overview page. Discovery strategies:

- **Search engine indexing:** Google indexes many Marriott DAM PDFs. A site:marriott.com search for "fact sheet" + property name often finds them.

- **Known URL pattern:** Once the brand prefix and MARSHA code are known, construct the DAM path and attempt common suffixes.

- **Marriott meeting/events pages:** Some events pages link to downloadable PDF brochures.

## Summary: Factsheets & Media Kits

Pattern
Status
Room Count
Address
Phone
Meeting Space
Bot Risk

DAM Factsheet PDF
✅ WORKING
Yes (exact)
Yes
Yes
Yes
Low

Meeting Event Brochure PDF
✅ WORKING
Rarely
Sometimes
Sometimes
Yes
Low

Guest Room Brochure PDF
✅ WORKING
Yes (detailed)
Sometimes
Sometimes
No
Low

URL Discovery
⚠️ CHALLENGE — PDF URLs not directly linked from property overview pages; require search engine or path guessing

### Additional Confirmed CALA Factsheet PDFs

- **Courtyard by Marriott Guayaquil** (MARSHA: GYECY, Ecuador): [cy-gyecy-11-23-hotel-fact-sheet-19316.pdf](https://www.marriott.com/content/dam/marriott-digital/cy/cala/hws/g/gyecy/en_us/document/assets/cy-gyecy-11-23-hotel-fact-sheet-19316.pdf) — 144 rooms (Standard, Junior Suites, Suites), 7 meeting rooms, rooftop pool & gym, shuttle service

- **Courtyard by Marriott Guayaquil Event Fact Sheet**: [cy-gyecy-12-23-event-fact-sheet-10703.pdf](https://www.marriott.com/content/dam/marriott-digital/cy/cala/hws/g/gyecy/en_us/document/assets/cy-gyecy-12-23-event-fact-sheet-10703.pdf) — confirms 144 rooms, 7 meeting rooms, largest capacity 140 theater

**Confirmed brand prefix codes from DAM URLs:**

- `fi` = Fairfield Inn (e.g., fi-sjulu-fact-sheet-23971.pdf)

- `cy` = Courtyard by Marriott (e.g., cy-gyecy-11-23-hotel-fact-sheet-19316.pdf)

- `wi` = Westin (guest rooms brochure: wi-snawi-guest-rooms-brochure-37741.pdf)

- `si` = Sheraton (meeting brochure: si-amssi-meeting-event-brochure-35925.pdf)

- `jw` = JW Marriott (expected pattern)

- `xr` = St. Regis (bar menu: xr-bdaxr-03-24-st-regis-bar-menu-33943.pdf)

- `rz` = Ritz-Carlton (kids schedule: rz-plsrt-ritz-kids-schedule-2023-22880.pdf)

- `wh` = W Hotels (expected pattern)

**Brand prefix inference for CALA properties:** With the MARSHA code and known brand prefix, the DAM PDF URL can be partially constructed. For example:

JW Marriott Cancun (CUNJW): /content/dam/marriott-digital/jw/cala/hws/c/cunjw/en_us/document/assets/jw-cunjw-fact-sheet-{id}.pdf
W Punta de Mita (PVRWH): /content/dam/marriott-digital/wh/cala/hws/p/pvrwh/en_us/document/assets/wh-pvrwh-fact-sheet-{id}.pdf
Sheraton San Jose (SJOSI): /content/dam/marriott-digital/si/cala/hws/s/sjosi/en_us/document/assets/si-sjosi-fact-sheet-{id}.pdf

The main unknown is the trailing asset ID ({id}) — typically a 5-digit number.

### CALA Factsheet Discovery Results — Asset ID Guessing Attempt

**Attempted:** Direct URL construction for JW Marriott Cancun (CUNJW), JW Marriott Bogota (BOGJW), Sheraton San Jose (SJOSI), JW Marriott Santo Domingo (SDQJW) using known asset IDs from SJULU (23971), GYECY (10703, 19316), Westin brochure (37741), and Sheraton brochure (35925).

**Result:** ALL 25 direct URL guesses returned 404 — "No resource found" from Apache Sling. Asset IDs are **property-specific and not reusable across MARSHA codes**. This confirms URL guessing by known IDs is not viable.

**However:** The DAM path pattern itself is confirmed valid for CUNJW — menu PDFs and floor plans exist at the expected path. The fact sheet PDF simply uses a different (unindexed) asset ID.

[10][11][12]
### Confirmed Working CALA DAM Factsheet PDFs — 5 Examples

Property
MARSHA
Country
Rooms
Working DAM PDF URL
Key Fields

Fairfield by Marriott Luquillo Beach
SJULU
Puerto Rico
104 rooms
[fi-sjulu-fact-sheet-23971.pdf](https://www.marriott.com/content/dam/marriott-digital/fi/cala/hws/s/sjulu/en_us/document/assets/fi-sjulu-fact-sheet-23971.pdf)
Address (110 Seaside Drive, Luquillo, PR 00773), Phone (787.657.0000), suites, meeting space

Courtyard by Marriott Guayaquil
GYECY
Ecuador
144 rooms
[cy-gyecy-11-23-hotel-fact-sheet-19316.pdf](https://www.marriott.com/content/dam/marriott-digital/cy/cala/hws/g/gyecy/en_us/document/assets/cy-gyecy-11-23-hotel-fact-sheet-19316.pdf)
Standard rooms, Junior Suites, Suites, 7 meeting rooms, rooftop pool, gym, shuttle

Courtyard by Marriott Guayaquil (Event)
GYECY
Ecuador
144 rooms
[cy-gyecy-12-23-event-fact-sheet-10703.pdf](https://www.marriott.com/content/dam/marriott-digital/cy/cala/hws/g/gyecy/en_us/document/assets/cy-gyecy-12-23-event-fact-sheet-10703.pdf)
7 meeting rooms, 140 person max capacity

St. Kitts Marriott Resort (Wedding Brochure)
SKBRB
St. Kitts
389 rooms
[mc-skbrb-03-24-wedding-brochure-12323.pdf](https://www.marriott.com/content/dam/marriott-digital/mc/cala/hws/s/skbrb/en_us/document/assets/mc-skbrb-03-24-wedding-brochure-12323.pdf)
389 guest rooms and luxurious suites

JW Marriott Cancun (via ConferenceHotelGroup)
CUNJW
Mexico
447 rooms (374 standard + 74 suites)
[Fact_Sheet_JW_2021_ING_organized.pdf](https://www.conferencehotelgroup.com/img/hotels/files/6647/Fact_Sheet_JW_2021_ING_organized.pdf)
Address (Blvd. Kukulcan Km 14.5, Zona Hotelera, Cancun), Phone, detailed room breakdown, full amenities

[3][4][8][9]
### Pattern 4: ConferenceHotelGroup & Third-Party Hotel Sales Sites — ⭐ WORKING

**URL template:** `https://www.conferencehotelgroup.com/hotels/{property-id}/{property-slug}/`

**Example (CUNJW):** [conferencehotelgroup.com/hotels/6647/](https://www.conferencehotelgroup.com/hotels/6647/JW-Marriott-Cancun-Resort---Spa)

**What it returns:** Full property profile with exact room count, address, phone, meeting room capacities, and downloadable official Marriott fact sheets. The CUNJW page confirms 447 bedrooms, 8 meeting rooms, address, and provides 3 downloadable PDFs including the official Marriott fact sheet.

**Bot-block risk:** LOW. These are third-party B2B sales platforms that want hoteliers to find them. No Akamai protection.

**Autopilot suitability:** MEDIUM-HIGH. Property ID is discoverable via search. Pages serve structured data. PDF attachments are hosted on the same domain. However, the property ID mapping requires search discovery — no deterministic URL template from MARSHA code alone.

[7]
### Key Challenge Update: DAM PDF URL Discovery

**Confirmed finding:** The DAM path pattern is valid and consistent across properties, but the trailing asset ID is **not predictable**. Only a small subset of DAM PDFs are indexed by Google. Strategies that partially work:

- **Google site search:** `site:marriott.com "content/dam/marriott-digital" "{marshaCode}" pdf` — finds some but not all. Works for CUNJW (menus found), SJULU (fact sheet found), GYECY (fact sheet found). Does NOT work for BOGJW, SJOSI, SDQJW — no DAM PDFs indexed for these.

- **ConferenceHotelGroup and similar B2B sites:** These third-party platforms often host official Marriott fact sheets as downloadable attachments. The JW Marriott Cancun fact sheet was found this way.

- **Google dorking names:** `"fact sheet" "JW Marriott Cancun" filetype:pdf` — finds third-party hosted versions.

- **The IFMM/Caribbean trade publication PDFs:** Industry magazines like IFMM contain room counts for major CALA properties. The IFMM PDF confirmed 447 rooms for JW Marriott Cancun.

[6][7]
[3][4][5]

# Hotel Microsites & JSON-LD

## Hotel Microsites & JSON-LD — Marriott Property Data

### Pattern 1: modules.marriott.com Microsites — ⭐ WORKING / SUPPLEMENTAL

**URL template:** `https://modules.marriott.com/{property-collection-slug}/`

**Example confirmed:** [Marriott Cancun Collection](https://modules.marriott.com/cancun-resorts-and-spa-kukulcan-boulevard/) — serves JW Marriott Cancun (CUNJW) and Marriott Cancun All-Inclusive (CUNMX) jointly.

**What it reveals:** These are dedicated microsites built on a separate subdomain for property clusters. The Cancun Collection states: *"Together JW Marriott Cancun Resort & Spa and Marriott Cancun, an All-Inclusive Resort comprise the most expansive beachfront facilities in Cancun's Hotel Zone with over 90,000 square feet of meeting space and nearly 900 guestrooms."*

**Available fields:** Aggregated guestroom count, individual restaurant listings, contact info (address + phone), meeting RFP links with MARSHA codes.

**JSON-LD check:** ❌ NONE. Searched full page source — no structured data at all.

**Bot-block risk:** UNKNOWN — separate subdomain from HWS. May have different CDN rules.

**Verdict:** Microsites aggregate property counts and confirm MARSHA codes but don't provide per-property exact room counts and have no JSON-LD.

### Pattern 2: Brand-Specific Subdomains — ⭐ WORKING / DISCOVERY-ONLY

**Confirmed subdomains:**

- `https://jw-marriott.marriott.com/` — JW Marriott brand page

- `https://marriott-hotels.marriott.com/` — Marriott Hotels brand page

- `https://the-luxury-collection.marriott.com/` — Luxury Collection

- `https://st-regis.marriott.com/` — St. Regis brand page

- `https://whotels.com/` — W Hotels brand page (redirects to marriott.com)

- `https://autograph-hotels.marriott.com/` — Autograph Collection

**What they contain:** Brand-level marketing pages that link to individual property overview pages on marriott.com. **No property-level structured data.**

**Bot-block risk:** LOW — these are marketing subdomains not behind the HWS Akamai configuration.

### Pattern 3: Ritz-Carlton ritzcarlton.com — ⭐ WORKING / DIFFERENT INFRASTRUCTURE

**URL template:** `https://www.ritzcarlton.com/en/hotels/{marshaCode}-{slug}/overview/`

**CALA examples confirmed:**

- **Zadún Los Cabos, a Ritz-Carlton Reserve:** `sjdzr` — [overview page](https://www.ritzcarlton.com/en/hotels/sjdzr-zadun-los-cabos-a-ritz-carlton-reserve/overview/) — 113 rooms, suites and villas (stated in body text). The MARSHA code appears in the URL.

- **The Ritz-Carlton, Mexico City:** `mexrz` — [overview page](https://www.ritzcarlton.com/en/hotels/mexrz-the-ritz-carlton-mexico-city/overview/)

- **Dorado Beach, a Ritz-Carlton Reserve:** `sjudo` — [overview page](https://www.ritzcarlton.com/en/hotels/sjudo-dorado-beach-a-ritz-carlton-reserve/overview/)

- **The Ritz-Carlton, Aruba:** Listed on Caribbean destinations page

- **The Ritz-Carlton, Grand Cayman:** Listed on Caribbean destinations page

**JSON-LD / structured data check:** ❌ COMPREHENSIVELY NEGATIVE. Searched the Zadún overview page source for: `application/ld+json`, `__NEXT_DATA__`, `LodgingBusiness`, `streetAddress`, `numberOfRooms`, `latitude`, `longitude`, `telephone` — **ZERO matches for any**. The Ritz-Carlton site does NOT implement Schema.org structured data.

**Key architectural difference:** The Ritz-Carlton site uses a different page framework than marriott.com's Next.js app — it does NOT have `__NEXT_DATA__` payloads. Property detail (address, phone) would need HTML parsing.

**Bot-block risk:** LIKELY LOWER — different domain, different CDN configuration than marriott.com HWS. The Zadún and Mexico City pages loaded successfully.

**Autopilot suitability:** MEDIUM. MARSHA codes are in the URL. Room counts sometimes appear in body text (e.g., "113 rooms" for Zadún). Address/phone would require HTML parsing of a "Getting Here" section.

### Pattern 4: Standalone Hotel Domains (Non-Marriott) — ❌ DEAD / REAL ESTATE ONLY

**Tested domains:**

- **doradobeach.com** — This is the Dorado Beach **real estate** website, not the Ritz-Carlton Reserve hotel. Links back to ritzcarlton.com for hotel bookings. No JSON-LD hotel data.

- **puntamita.com** — Punta Mita resort **community** website. Links to Four Seasons and St. Regis. Contains St. Regis room count (89 rooms + 31 suites) in body text, but no JSON-LD. The St. Regis booking link is `stregis.com/puntamita/` — a brand TLD, not a standalone hotel domain.

- **fairfieldluquillobeach.com** — Listed on factsheet PDF but domain does not resolve / not maintained.

**Verdict:** Marriott luxury properties do NOT maintain standalone hotel booking websites independent of marriott.com or ritzcarlton.com. "Official websites" found on factsheets are either defunct or redirect to the brand TLD.

### Pattern 5: Hotel Room Counts from Body Text / Community Sites — SUPPLEMENTAL

**St. Regis Punta Mita (PVRXR):** `puntamita.com/stay/` states: *"the resort features discerning service, 89 spacious rooms and 31 luxury suites"* — total 120 keys. This is a resort community site, not an official Marriott domain.

**Zadún Los Cabos (SJDZR):** `ritzcarlton.com` overview page states: *"Each of the resort's 113 rooms, suites and villas"* — 113 keys, confirmed from official Marriott domain.

**W Punta de Mita (PVRWH):** Third-party trade publications cite 119–126 rooms. No official Marriott source found with exact count in this session.

## Summary: Hotel Microsites & JSON-LD

Pattern
Status
JSON-LD
Room Count
Address/Phone
MARSHA
Bot Risk

modules.marriott.com Microsites
✅ WORKING
❌ None
Aggregated only
Yes (HTML)
Yes (RFP links)
Unknown

Brand Subdomains
✅ WORKING (discovery)
N/A
No
No
No
Low

Ritz-Carlton ritzcarlton.com
✅ WORKING
❌ None
Body text only
HTML only
Yes (URL)
Likely Lower

Standalone Hotel Domains
❌ DEAD/NOT HOTEL SITES
❌ None
No
No
No
N/A

Resort Community Sites
⚠️ SUPPLEMENTAL
❌ None
Body text
Sometimes
No
Low

## Cross-Platform Finding: ZERO JSON-LD Anywhere

Across ALL platforms tested — marriott.com overview pages, marriott.com hotel-sitemaps, modules.marriott.com microsites, ritzcarlton.com property pages, doradobeach.com, puntamita.com — **ZERO Schema.org Hotel/LodgingBusiness JSON-LD structured data was found on any property page**. Marriott does not implement structured data markup for hotels. The only structured JSON found is the `__NEXT_DATA__` payload on Next.js-rendered pages (sitemaps, search results), which contains internal app state but not Schema.org markup.

**Implication for Dealality:** Structured data extraction must come from (a) hotel-sitemap __NEXT_DATA__ for MARSHA discovery, (b) DAM PDF factsheets for room counts + addresses, (c) press releases for new openings, or (d) HTML parsing of ritzcarlton.com and marriott.com pages where bot protection allows.

# Press Release Room Counts

## Press Release Room Counts — Marriott CALA Properties

### Pattern 1: news.marriott.com Press Releases with Room Counts — ⭐ WORKING

**URL pattern:** `https://news.marriott.com/news/{YYYY}/{MM}/{DD}/{slug}`

**Keying:** By property name search — no MARSHA code in press release URLs directly, but property names and MARSHA codes are usually mentioned in the body text.

**What it returns:** Full press release HTML with exact property details including room counts, address, and amenities. These are Wordpress-based pages on a separate CDN from marriott.com HWS.

**Bot-block risk:** LOW. news.marriott.com is a separate WordPress site not behind the same Akamai HWS protections. Loaded successfully with full content.

**Autopilot suitability:** MEDIUM-HIGH. Deterministic URL pattern (date + slug), structured article content. Room counts typically appear as "450 newly redesigned guestrooms" or similar phrasing — requires NLP extraction but content is reliable.

[1]
### CALA Example — Working

- **Marriott Cancun, An All-Inclusive Resort** (MARSHA: CUNMX) — 450 guestrooms, 124 premium ocean view, 38 suites, 2 Presidential Suites. Press release: [July 30, 2024](https://news.marriott.com/news/2024/07/30/an-all-inclusive-awakening-marriott-hotels-elevates-the-art-of-hospitality-with-new-cancun-resort)

### Pattern 2: news.marriott.com Search/Directory — ⭐ WORKING

**URL:** `https://news.marriott.com/news/` — the main news index.

**Keying:** Browse by category or search. Press releases are tagged by brand, region, and topic.

**Typical room count mentions in body text:**

- "The resort features [N] newly redesigned guestrooms"

- "[N] guest rooms and suites"

- "boasting [N] keys"

**Limitation:** Only covers properties at opening/announcement time. Not useful for existing properties that opened years ago. Requires NLP extraction.

[1]
## Summary: Press Release Room Counts

Pattern
Status
Room Count
Address
Phone
MARSHA
Bot Risk

news.marriott.com Press Releases
✅ WORKING
Yes (NLP)
Sometimes
Sometimes
Sometimes (in body)
Low

Coverage limitation
Only covers new openings and major renovations. No historical data for pre-existing properties.

### More CALA Press Release Examples

- **The St. Regis Costa Mujeres Resort, Cancun** (MARSHA: CUNCM) — 158 guest rooms + 80 branded residential units. Press release: [November 2, 2022](https://news.marriott.com/news/2022/11/02/st-regis-hotels-resorts-to-debut-in-costa-mujeres-mexico)

### Additional Confirmed CALA Press Release Examples with Room Counts

- **JW Marriott Cancun Resort & Spa** (MARSHA: CUNJW, Mexico) — **447 guest rooms**, including 74 suites (374 standard). Press release: [January 30, 2019](https://news.marriott.com/news/2019/01/30/jw-marriott-cancun-resort-spa-unveils-reimagined-guestrooms-infused-with-modern-design-elements). Confirms "447 lavish ocean-facing guestrooms and suites", detailed breakdown of room types. Also mentions MARSHA code: `mhrs.cunjw.social.sales.mgr@marriotthotels.com` email, and phone +52 (998) 881 2014.

- **The St. Regis Costa Mujeres Resort, Cancún** (MARSHA: CUNCM, Mexico) — **163 guestrooms + 50 suites** = 213 keys. Press release: [June 23, 2026](https://news.marriott.com/news/2026/06/23/the-st-regis-costa-mujeres-resort-cancun-debuts-as-a-refined-enclave-between-land-and-sea-in-mexico). Confirms "163 guestrooms and 50 suites". Note: the 2022 announcement said 158 rooms + 80 residences — the actual opening count is 213 total keys.

- **Marriott Cancun, An All-Inclusive Resort** (MARSHA: CUNMX, Mexico) — **450 guestrooms**. Press release: [July 30, 2024](https://news.marriott.com/news/2024/07/30/an-all-inclusive-awakening-marriott-hotels-elevates-the-art-of-hospitality-with-new-cancun-resort).

- **The St. Regis Costa Mujeres Resort, Cancún** (MARSHA: CUNCM, Mexico) — Pre-opening announcement: [November 2, 2022](https://news.marriott.com/news/2022/11/02/st-regis-hotels-resorts-to-debut-in-costa-mujeres-mexico). Initially planned 158 guest rooms + 80 branded residential units.

[5][6][2]
### Upcoming/Pipeline CALA Properties with Room Counts from Press Releases

- **JW Marriott All-Inclusive Resort, Costa Elena (Costa Rica)** — **415 guest rooms** (planned). [March 25, 2025](https://news.marriott.com/news/2025/03/25/marriott-international-signs-agreement-with-mullen-real-estate-capital-to-debut-a-jw-marriott-all-inclusive-resort-in-costa-rica). Expected opening Spring 2026.

- **The St. Regis Papagayo (Costa Rica)** — **120 hotel rooms + 143 residential units** (planned). [May 22, 2025](https://news.marriott.com/news/2025/05/22/marriott-international-signs-agreement-to-debut-st-regis-hotels-resorts-brand-in-costa-rica). Expected opening early 2027.

[3][4]
[2]
### News Search Strategy for Room Counts

To find room counts for existing CALA properties via press releases, use the pattern: `site:news.marriott.com "{property name}" guest rooms` or `site:news.marriott.com "{property name}" guestrooms`. Most opening announcements contain "feature [N] guest rooms" or similar phrasing. However, coverage is limited to properties opened or renovated after ~2022.

[1][2]

