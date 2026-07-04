# Deal Readiness Scoring — Field & Response Audit

**Generated:** 2026-05-18T20:37:46.081Z  
**Purpose:** Inventory readiness-relevant fields, sample response quality from live Deals data, and propose a scoring model for review.  
**Status:** Audit / recommendation only — no scoring or renderer changes are applied by this document alone.

---

## Summary findings

1. **89 required fields** drive readiness today (from `REQUIRED_DEAL_SETUP_FIELDS` in `api/my-deals.js`), merged across Deals + linked Location, Market Performance, Strategic Intent, Contact/Uploads, and optional Lease Structure tables.
2. **Server-side engine** (`api/deal-readiness-review.js`) already implements **weighted-v2** (domain weights, foundational caps, gap severity). This audit documents that model and recommends refinements based on **43 sampled deals** (live Airtable query).
3. **Schema drift:** `REQUIRED_DEAL_SETUP_FIELDS` and `REQUIRED_DEAL_SETUP_FIELDS_BY_SECTION` in `deal-setup-fields.js` are **not identical** (e.g. Primary Demand Drivers is required in the flat list but omitted from section 5; City/Country/Ownership Type appear in the flat list but not in section 2 block). This can confuse UI vs scoring.
4. **Response quality (n=43):** Most deals show **3–6 blank required fields**; weak/placeholder text is **rare** in the sample (often 0 per deal). Highest blank rates cluster in **conditional or enhancement fields** (see Part 2).
5. **Foundational gaps** (Project Type, Stage, Brand Status) are **uncommon** in the sample (~5% blank each) but when present they should **cap score and block Ready stage** — the weighted-v2 engine is designed for that.
6. **Xavier v2.0** (`recu99T39b47d74X7`) is in the sample; see Part 4 for current vs recommended treatment.

---

## Part 1 — Field inventory

| Airtable / form field | Airtable column (if different) | Deal Setup tab | Required | Conditional | Current scoring (weighted-v2) | Proposed importance | Proposed severity | Proposed cap | Narrative impact | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Property Name | Property Name | Project Overview | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Foundational | Blocking | 74 | Header, Summary, Cover | |
| Project Type | Project Type | Project Overview | Yes | — | weighted-v2 domain + blocking gap; foundational cap may apply | Foundational | Blocking | 74 | Header, Summary, Breakdown, Clarifications | |
| Stage of Development | Stage of Development | Project Overview | Yes | — | weighted-v2 domain + limiting gap; foundational cap may apply | Foundational | Limiting | 78 | Summary, Clarifications, Breakdown | |
| Has there ever been a franchise, branded management, affiliation or similar agreeement pertaining to the proposed hotel or site? | Has there ever been a franchise, branded management, affiliation or similar agreeement pertaining to the proposed hotel or site? | Brand & Op. Status | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Foundational | Enhancement | — | Technical missing list; tab score grid | |
| Is the hotel currently branded? | Is the hotel currently branded? | Brand & Op. Status | Yes | — | weighted-v2 domain + limiting gap; foundational cap may apply | Foundational | Limiting | 84 | Summary gaps, Clarifications, Breakdown | |
| Is the hotel currently managed by a third-party operator? | Is the hotel currently managed by a third-party operator? | Brand & Op. Status | Yes | — | weighted-v2 domain + limiting gap; foundational cap may apply | Foundational | Limiting | 86 | Breakdown (operator), Clarifications | |
| Are you open to lesser-known or emerging brands with favorable terms? | Are you open to lesser-known or emerging brands with favorable terms? | Brand & Op. Status | Yes | Form alias: Are you open to considering other brands with favorable terms? | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Have you worked with any of your preferred brands/operators before? | Have you worked with any of your preferred brands/operators before? | Brand & Op. Status | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Foundational | Enhancement | — | Technical missing list; tab score grid | |
| Full Address | Full Address | Location & Site Details | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| City & State | City | Location & Site Details | Yes | — | weighted-v2 domain + limiting gap; foundational cap may apply | Important | Limiting | — | Header, Summary, location line | |
| Country | Country | Location & Site Details | Yes | — | weighted-v2 domain + blocking gap; foundational cap may apply | Foundational | Blocking | 59 | Header, Summary, Strengths (market anchor) | |
| Hotel Chain Scale | Hotel Chain Scale | Location & Site Details | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Hotel Type | Hotel Type | Property Specs | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Foundational | Enhancement | — | Technical missing list; tab score grid | |
| Hotel Submarket & Location | Hotel Submarket & Location | Location & Site Details | Yes | — | weighted-v2 domain + limiting gap; foundational cap may apply | Important | Limiting | — | Summary, market anchor fallback | |
| Hotel Service Model | Hotel Service Model | Property Specs | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Ownership/Brand History or Track Record | Ownership/Brand History or Track Record | Location & Site Details | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Foundational | Enhancement | — | Technical missing list; tab score grid | |
| Zoned for Hotel Development | Zoned for Hotel Development | Location & Site Details | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Site/Development Restrictions? | Site/Development Restrictions? | Location & Site Details | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Total Site Size | Total Site Size | Location & Site Details | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Total Site Size Unit | Tota Site Size Unit | Location & Site Details | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Max height Allowed By Zoning | Max Height Allowed By Zoning | Location & Site Details | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Max height Unit | Max Height Allowed By Zoning Unit | Location & Site Details | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Current Form of Site Control | Current Form of Site Control | Location & Site Details | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Ownership Type | Ownership Type | Location & Site Details | Yes | — | weighted-v2 domain + blocking gap; foundational cap may apply | Foundational | Blocking | 79 | Breakdown (ownership) | |
| Zoning Status | Zoning Status | Location & Site Details | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Parking Ratio | Parking Ratio | Location & Site Details | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Access to Transit or Highway | Access to Transit / Highway | Location & Site Details | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Total Number of Rooms/Keys | Total Number of Rooms/Keys | Property Specs | Yes | — | weighted-v2 domain + blocking gap; foundational cap may apply | Foundational | Blocking | 79 | Header, Summary, Strengths (scale) | |
| Number of Standard Rooms | Number of Standard Rooms | Property Specs | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Number of Suites | Number of Suites | Property Specs | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Building Type | Building Type | Property Specs | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Foundational | Enhancement | — | Technical missing list; tab score grid | |
| Number of Stories | # of Stories | Property Specs | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| F&B Outlets? | F&B Outlets? | Amenities & Facilities | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Meeting Space | Meeting Space | Amenities & Facilities | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Number of Meeting Rooms | Number of Meeting Rooms | Amenities & Facilities | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Condo Residences? | Condo Residences? | Amenities & Facilities | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Hotel Rental Program? | Hotel Rental Program? | Amenities & Facilities | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Parking Amenities? | Parking Amenities? | Amenities & Facilities | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Enhancement | Enhancement | — | Technical missing list; tab score grid | |
| Additional Amenities | Additional Amenities | Amenities & Facilities | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Enhancement | Enhancement | — | Clarifications when missing; Technical missing list | |
| Primary Demand Drivers | Primary Demand Drivers | Market & Performance | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Primary Demand Drivers Other | Primary Demand Drivers Other | Market & Performance | Yes | Required only when Primary Demand Drivers includes Other | weighted-v2 domain completion only; enhancement gap if missing | Enhancement | Enhancement | — | Conditional on Other driver | |
| Estimated or Actual RevPAR | Estimated or Actual RevPAR | Market & Performance | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Regulatory or Permitting Issues? | Regulatory or Permitting Issues? | Market & Performance | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Regulatory or Permitting Issues Description | Regulatory or Permitting Issues Description | Market & Performance | Yes | Required only when Regulatory or Permitting Issues? ≠ No | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Key Competitors | Key Competitors | Market & Performance | Yes | — | weighted-v2 domain + limiting gap; foundational cap may apply | Important | Limiting | — | Clarifications (competitive set) | |
| Group vs Transient Mix | Group vs Transient Mix | Market & Performance | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Total Project Cost Range | Total Project Cost Range | Deal & Capital Structure | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| PIP Budget Range (if conversion) | PIP Budget Range (if conversion) | Deal & Capital Structure | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Foundational | Enhancement | — | Technical missing list; tab score grid | |
| Equity vs Debt Split | Equity vs Debt Split | Deal & Capital Structure | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Ownership Structure | Ownership Structure | Deal & Capital Structure | Yes | — | weighted-v2 domain + blocking gap; foundational cap may apply | Foundational | Blocking | 79 | Breakdown (ownership) | |
| Preferred Deal Structure | Preferred Deal Structure | Deal & Capital Structure | Yes | — | weighted-v2 domain + blocking gap; foundational cap may apply | Foundational | Blocking | 84 | Summary, Breakdown (agreement) | |
| PIP / CapEx Status | PIP / CapEx Status | Deal & Capital Structure | Yes | — | weighted-v2 domain + limiting gap; foundational cap may apply | Foundational | Limiting | 86 | Breakdown (capex), Clarifications | |
| Lease Type | Lease Type | Lease Structure | Yes | Required only when Preferred Deal Structure is Lease / Flexible/Open (lease tab visible) | weighted-v2 domain completion only; enhancement gap if missing | Foundational | Enhancement | — | Technical missing list; tab score grid | |
| Soft vs Hard Brand Preference | Soft vs Hard Brand Preference | Strategic Intent | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Preferred Brands (up to 4) | Preferred Brands (up to 4) | Strategic Intent | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| IRR/Yield Goals | IRR/Yield Goals | Strategic Intent | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Open to Outside Capital or Partnerships? | Open to Outside Capital or Partnerships? | Strategic Intent | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Plan to Self-Manage or Hire Third Party? | Plan to Self-Manage or Hire Third Party? | Strategic Intent | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Preferred Chain Scales | Preferred Chain Scales | Strategic Intent | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Open to Soft Brand First Then Reflag? | Open to Soft Brand First Then Reflag? | Strategic Intent | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Target Guest Segment | Target Guest Segment | Strategic Intent | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Brand Flexibility vs Prestige | Brand Flexibility vs Prestige | Strategic Intent | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Planned Hold Period | Planned Hold Period | Strategic Intent | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Primary Goal for the Hotel | Primary Goal for the Hotel | Strategic Intent | Yes | — | weighted-v2 domain + limiting gap; foundational cap may apply | Foundational | Limiting | 88 | Summary, Strengths (objective) | |
| Who should receive bids for this project? | Who should receive bids for this project? | Contact Info | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Minimum Operator Experience (years) | Minimum Operator Experience (years) | Operational Expectations | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Foundational | Enhancement | — | Technical missing list; tab score grid | |
| Preferred Third-Party Operators (names) | Preferred Third-Party Operators (names) | Operational Expectations | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Foundational | Enhancement | — | Technical missing list; tab score grid | |
| Preferred Third-Party Operator Profile | Preferred Third-Party Operator Profile | Operational Expectations | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Foundational | Enhancement | — | Technical missing list; tab score grid | |
| Services Required From Operator | Services Required From Operator | Operational Expectations | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Foundational | Enhancement | — | Technical missing list; tab score grid | |
| Other Operator Criteria or Notes | Other Operator Criteria or Notes | Operational Expectations | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Foundational | Enhancement | — | Technical missing list; tab score grid | |
| Level of Involvement in Day-to-Day Ops | Level of Involvement in Day-to-Day Ops | Operational Expectations | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Top Priorities for Project | Top Priorities for Project | Challenges & Priorities | Yes | — | weighted-v2 domain + limiting gap; foundational cap may apply | Important | Enhancement | — | Technical missing list; tab score grid | |
| Top Concerns for this Project | Top Concerns for this Project | Challenges & Priorities | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Top 3 Success Metrics | Top 3 Success Metrics | Challenges & Priorities | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Top 3 Deal Breakers | Top 3 Deal Breakers | Challenges & Priorities | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Must-haves From Brand or Operator | Must-haves From Brand or Operator | Challenges & Priorities | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Foundational | Enhancement | — | Technical missing list; tab score grid | |
| Decision Timeline for Brand/Operator | Decision Timeline for Brand/Operator | Challenges & Priorities | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Foundational | Enhancement | — | Technical missing list; tab score grid | |
| Would you like to filter out brands without key money? | Would you like to filter out brands without key money? | Support & Comm. | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Enhancement | Enhancement | — | Technical missing list; tab score grid | |
| Would you like to meet consultants? | Would you like to meet consultants? | Support & Comm. | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Enhancement | Enhancement | — | Technical missing list; tab score grid | |
| Legal Support Needed? | Legal Support Needed? | Support & Comm. | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Enhancement | Enhancement | — | Technical missing list; tab score grid | |
| Financial Model Available? | Financial Model Available? | Support & Comm. | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Proposal Deadline | Proposal Deadline | Support & Comm. | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Would you like to receive regular updates? | Would you like to receive regular updates? | Support & Comm. | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Enhancement | Enhancement | — | Technical missing list; tab score grid | |
| Working with Broker/Advisor? | Working with Broker/Advisor? | Support & Comm. | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Other Projects Nearing Contract Expiration? | Other Projects Nearing Contract Expiration? | Support & Comm. | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Enhancement | Enhancement | — | Technical missing list; tab score grid | |
| Main Contact Name | Main Contact Name | Contact Info | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Entity or Company Name | Entity or Company Name | Contact Info | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Company HQ Location | Company HQ Location | Contact Info | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |
| Email Address | Email Address | Contact Info | Yes | — | weighted-v2 domain completion only; enhancement gap if missing | Important | Enhancement | — | Technical missing list; tab score grid | |

### Domain assignment (current weighted-v2)

| Domain | Weight | Primary fields |
| --- | ---: | --- |
| Core Project Definition | 18 | Property Name, Project Type, Stage of Development |
| Location & Market Context | 12 | Address, city/country, submarket, site controls, market performance fields |
| Asset / Property Profile | 12 | Keys, rooms, building, amenities |
| Ownership & Control | 10 | Ownership Type/Structure, ownership history |
| Brand & Operator Starting Point | 12 | Brand/operator status, franchise history, operator criteria |
| Deal / Capital / Agreement Structure | 14 | Costs, equity/debt, preferred structure, PIP/capex, lease type |
| Strategic Intent & Owner Priorities | 10 | Brand preferences, goals, priorities, timelines |
| Documentation & Supporting Materials | 8 | Support & Comm. fields (proxy for documentation readiness) |
| Contact / Communication Readiness | 4 | Main contact, email, entity, bid recipient |

### Foundational fields (Ready gate + caps)

| Field / rule | Cap if missing | Severity |
| --- | ---: | --- |
| Market/country anchor (Country OR City+Submarket) | 59 | Blocking |
| Project Type | 74 | Blocking |
| Stage of Development | 78 | Limiting |
| Key count | 79 | Blocking |
| Ownership Type **or** Structure | 79 | Blocking |
| Current brand status | 84 | Limiting |
| Current operator status | 86 | Limiting |
| Preferred deal structure | 84 | Blocking |
| Capex / PIP status | 86 | Limiting |
| Owner objectives (Primary Goal **or** Top Priorities) | 88 | Limiting |
| Contact name **and** email | 90 | Limiting |
| Documentation signals (Financial Model **or** Broker/Advisor) | 92 | Enhancement |

---

## Part 2 — Actual response quality (sampled deals)

**Sample size:** 43 deals with merged linked records (`fetchDealWithMergedLinkedRecords`).  
**Source file:** `scripts/output/deal-readiness-field-audit-data.json`

### Deal-level summary

| Metric | Observation |
| --- | --- |
| Blank required fields per deal | Typically **3–6** of 89 |
| Weak placeholder values | **Low** in sample (most deals: 0 weak flags) |
| Common populated values | Project Type: often "Conversion / Reflag"; Stage: often "Stabilized Operating Asset" |

### Fields with blanks in the sample (partial table)

| Field | Sampled | Blank | Populated | Weak | Blank % | Common values |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Has there ever been a franchise, branded management, affiliation or similar agreeement pertaining to the proposed hotel or site? | 43 | 43 | 0 | 0 | 100% | — |
| Are you open to lesser-known or emerging brands with favorable terms? | 43 | 43 | 0 | 0 | 100% | — |
| Regulatory or Permitting Issues Description | 43 | 31 | 12 | 0 | 72% | Dev purpose only (4); Environmental review in progress. (2); Historic designation review. (2) |
| Lease Type | 43 | 23 | 20 | 0 | 53% | Fixed Rent (6); Percentage Rent (4); Other (3) |
| Primary Demand Drivers Other | 43 | 20 | 23 | 0 | 47% | Film production (6); Long-term stay (3); Bleisure (3) |
| Preferred Third-Party Operators (names) | 43 | 9 | 34 | 0 | 21% | Charlestowne Hotels, Island Hospitality (13); White Lodging, Aimbridge (6); Pyramid Hotel Group, Crescent Hotels (4) |
| Other Operator Criteria or Notes | 43 | 5 | 38 | 0 | 12% | Prefer operator with strong technology stack and (8); Must have experience with conversion projects; p (8); Experience in same market or asset class preferr (8) |
| Total Site Size | 43 | 3 | 40 | 0 | 7% | 8000 (4); 10000 (4); 6000 (3) |
| Project Type | 43 | 2 | 41 | 0 | 5% | Conversion / Reflag (22); New Build (19) |
| Stage of Development | 43 | 2 | 41 | 0 | 5% | Stabilized Operating Asset (27); Land Under Control Only (6); Under Construction (6) |
| Is the hotel currently branded? | 43 | 2 | 41 | 0 | 5% | Yes (22); No (19) |
| Zoned for Hotel Development | 43 | 2 | 41 | 0 | 5% | Yes (40); No (1) |
| Site/Development Restrictions? | 43 | 2 | 41 | 0 | 5% | No (30); Yes (11) |
| Additional Amenities | 43 | 2 | 41 | 0 | 5% | Not Applicable / None (7); Outdoor Area / Courtyard (2); Bar or Beverage Concept, Meeting/Event Space, Sp (1) |
| PIP / CapEx Status | 43 | 2 | 41 | 7 | 5% | Completed (24); Planned (7); None (7) |
| Property Name | 43 | 1 | 42 | 0 | 2% | Arsalan Group Ltd. (1); Marriott Times Square New York (1); The Langham London (1) |
| Is the hotel currently managed by a third-party operator? | 43 | 1 | 42 | 0 | 2% | Yes (23); No (19) |
| Have you worked with any of your preferred brands/operators before? | 43 | 1 | 42 | 0 | 2% | Yes (22); No (20) |
| Full Address | 43 | 1 | 42 | 0 | 2% | Main Street (1); 1535 Broadway New York NY 10036 USA (1); 1C Portland Pl London W1B 1JA UK (1) |
| City & State | 43 | 1 | 42 | 0 | 2% | New York (4); London (3); Miami (2) |
| Country | 43 | 1 | 42 | 0 | 2% | United States (19); Canada (5); United Kingdom (3) |
| Hotel Chain Scale | 43 | 1 | 42 | 0 | 2% | Upscale (14); Upper Upscale (7); Luxury (7) |
| Hotel Type | 43 | 1 | 42 | 0 | 2% | Urban (26); Airport (5); Suburban (3) |
| Hotel Submarket & Location | 43 | 1 | 42 | 0 | 2% | Downtown (4); Airport Area (3); Centro Historico (2) |
| Hotel Service Model | 43 | 1 | 42 | 0 | 2% | Full-Service (21); Select-Service (18); All-Inclusive (3) |
| Ownership/Brand History or Track Record | 43 | 1 | 42 | 0 | 2% | Experienced - Multi-Property Owner (30); Developer Experience - Non-Hospitality (6); New to Branded Hotels - Independent Only (2) |
| Total Site Size Unit | 43 | 1 | 42 | 0 | 2% | Sq. Ft. (42) |
| Max height Allowed By Zoning | 43 | 1 | 42 | 0 | 2% | 8 (9); 6 (5); 9 (5) |
| Max height Unit | 43 | 1 | 42 | 0 | 2% | Stories (41); Meters (1) |
| Current Form of Site Control | 43 | 1 | 42 | 0 | 2% | Fee Simple Ownership (11); Ground Lease (10); Under Contract (7) |
| Ownership Type | 43 | 1 | 42 | 0 | 2% | Fee Simple (9); Operator with Option to Buy (6); Development Agreement (5) |
| Zoning Status | 43 | 1 | 42 | 0 | 2% | Not Yet Zoned (17); Approved for Hotel (13); Conditional / in Progress (12) |
| Parking Ratio | 43 | 1 | 42 | 0 | 2% | 60 spaces (6); 0.7 per key (5); 120 spaces (4) |
| Access to Transit or Highway | 43 | 1 | 42 | 0 | 2% | On-Site or Adjacent Light Rail / Metro (5); Walking Distance to Transit (5); Adjacent to Transit Station or Stop (4) |
| Total Number of Rooms/Keys | 43 | 1 | 42 | 0 | 2% | 450 (4); 45 (3); 100 (3) |

### Response quality observations

- **TBD / N/A / Unknown:** Rare in sampled populated fields; engine `isWeakText` list matches audit patterns.
- **"Not Applicable / None"** on Additional Amenities: valid select answer (7 deals) — should **not** count as missing; may still be flagged if empty string in merge.
- **"Other" without detail:** Primary Demand Drivers Other is conditionally required; audit for deals with Other selected but blank Other text.
- **Alias keys:** Franchise affiliation field uses typo column in Airtable; merged GET normalizes to form key. Brand openness uses alternate Airtable column name.
- **Location vs Deals storage:** Many keys live on **Location & Property** linked table — scoring uses merged fields; blank may mean link missing or column not synced.
- **Lease Type:** Excluded from required set when lease structure not applicable — scoring respects `isLeaseStructureDealApplicableFromMergedFields`.

### Fields with highest blank % in sample

- **Has there ever been a franchise, branded management, affiliation or similar agreeement pertaining to the proposed hotel or site?** — 100% blank (43/43)
- **Are you open to lesser-known or emerging brands with favorable terms?** — 100% blank (43/43)
- **Regulatory or Permitting Issues Description** — 72% blank (31/43)
- **Lease Type** — 53% blank (23/43)
- **Primary Demand Drivers Other** — 47% blank (20/43)
- **Preferred Third-Party Operators (names)** — 21% blank (9/43)
- **Other Operator Criteria or Notes** — 12% blank (5/43)

---

## Part 3 — Proposed scoring model (for approval)

This aligns with **weighted-v2 already in code**; refinements below are recommendations after audit.

### 1. Weighted readiness domains (total 100)

Same nine domains and weights as implemented in `deal-readiness-review.js`.

### 2–3. Field → domain mapping

See Part 1 domain table; full mapping in `FIELD_TO_READINESS_DOMAIN` in `api/deal-readiness-review.js`.

### 4. Foundational fields

Union of **FOUNDATIONAL_READY_FIELDS** + ownership composite + market anchor rule (see Part 1).

### 5. Score caps

Use **lowest applicable cap** from `computeFoundationalCaps`; extend audit to treat **weak foundational** same as missing (not yet implemented).

### 6. Gap severity

| Severity | Meaning |
| --- | --- |
| **Blocking** | Prevents advanced review / formal external use |
| **Limiting** | Internal review OK; external readiness constrained |
| **Enhancement** | Improves brief; light score impact |

Populate `blockingIssues`, `limitingIssues`, `enhancementIssues` (already in API response).

### 7. Conditional requirements

| Rule | Fields |
| --- | --- |
| Regulatory description | Only if permitting issues ≠ No |
| Demand drivers other | Only if drivers include Other |
| Lease block | Only if deal structure is lease/flexible |

### 8. Weak-response penalties

| Severity | Penalty (current) |
| --- | ---: |
| Blocking | 3 |
| Limiting | 2 |
| Enhancement | 1 |

**Recommendation:** Apply weak penalty only when field is populated but weak; consider **cap reduction** when foundational field is weak (future).

### 9. Stage thresholds

| Stage | Rule |
| --- | --- |
| **Discovery** | Score &lt; 50 **or** any blocking gaps |
| **Shaping** | 50–69 (blocking caps at Advancing if score ≥ 70) |
| **Advancing** | 70–84 |
| **Ready for External Review** | 85–94 **or** 95+ with foundational gaps |
| **Ready** | 95–100 **and** zero foundational gaps **and** zero blocking |

### 10. Narrative alignment

| Stage | Lead interpretation (renderer) |
| --- | --- |
| Ready | Substantially complete; selective external conversations after validation |
| Ready for External Review | Broadly complete; validation before broader circulation |
| Advancing | Internal review OK; clarify before formal external outreach |
| Shaping | Early structure; more info before reliable brand/operator review |
| Discovery | Early intake; core information needed |

---

## Part 4 — Xavier v2.0 example

**Record:** recu99T39b47d74X7 — Xavier v2.0

**Sampled field check:** [{"field":"Project Type","filled":false},{"field":"Stage of Development","filled":false},{"field":"Is the hotel currently branded?","filled":false},{"field":"Additional Amenities","filled":false}]

**Current engine (weighted-v2 in repo):** score **74**, stage **Advancing**

**Weighted completion:** 86

**Caps applied:** Missing project type (max 74); Missing stage of development (max 78); Missing current brand status (max 84)

**Gap counts:** blocking 1, limiting 2, enhancement 1

**Proposed audit model (same caps):** lowest cap likely **74** (Project Type) → final **~74**, stage **Advancing** (not Ready while foundational gaps remain).

### Missing fields called out (typical outreach gaps)

| Field | Proposed severity | Cap | Why narrative/score must align |
| --- | --- | ---: | --- |
| Project Type | Blocking | 74 | Defines screening path; appears in clarification themes |
| Stage of Development | Limiting | 78 | Timeline and risk framing |
| Current Brand Status | Limiting | 84 | Brand vs conversion pathway |
| Additional Amenities | Enhancement | — | Product definition; should not alone block Ready |

**Credibility comparison**

| | Legacy equal-weight (pre weighted-v2) | Current / proposed weighted-v2 |
| --- | --- | --- |
| Score with 4 gaps including 3 foundational | Could read **~97** | Capped toward **74–84** depending on which foundational |
| Stage | Could show **Ready** | **Advancing** or **Ready for External Review** — not **Ready** until foundational complete |
| Narrative | "Not ready for broad outreach" while score says 97 | Score, stage, and copy align on validation needed |

---

## Recommended implementation steps (after approval)

1. Align `REQUIRED_DEAL_SETUP_FIELDS` with `REQUIRED_DEAL_SETUP_FIELDS_BY_SECTION` (single source of truth).
2. Validate merged-field coverage for Location/Market/Strategic Intent links on production deals.
3. Tune enhancement vs limiting classification for low-blank fields (amenities, support questions).
4. Add **weak foundational = cap** rule if audit shows placeholder text on core fields.
5. Re-run `node scripts/audit-deal-readiness-fields.mjs --max=100` after changes; compare score/stage distribution.
6. User acceptance on Xavier + 3–5 representative deals before treating weighted-v2 as final.

---

## Risks and open questions

1. **Documentation domain** uses Support & Comm. fields as proxy — no required Uploads/Deal Room fields in scoring set.
2. **Primary Demand Drivers** required in flat list but not in section schema block — is it required in UI?
3. **Operator fields** duplicated across Strategic Intent tab in schema vs Operational Expectations tab in UI mapping.
4. **Cap stacking** can dominate score (lowest cap 74) while tab percentages still look high — technical page must explain caps (renderer already does for weighted-v2).
5. **Sample size** (43 deals) may not represent production portfolio — expand audit to 100+ for statistical confidence.
6. **Weak detection** does not flag "Flexible" / "Open" / "Not Applicable" as weak — intentional for selects?

---

## How to reproduce this audit

```bash
node scripts/audit-deal-readiness-fields.mjs --max=100
node scripts/generate-readiness-audit-report.mjs
```

Requires `.env` with `AIRTABLE_API_KEY` and `AIRTABLE_BASE_ID`.
