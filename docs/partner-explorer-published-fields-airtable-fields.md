# Partner Intelligence — Published Explorer Fields (Airtable fields)

**Table name (exact):** `Partner Intelligence - Published Explorer Fields`  
**Base:** Primary (`AIRTABLE_BASE_ID`)  
**Status:** Proposed.

**Purpose:** **Only human-approved values** that may overlay live Brand Explorer and Operator Explorer reads. This table is the publish destination — not the Extracted Facts staging table.

Replaces the need for separate wide "Brand Explorer Profile" and "Operator Explorer Profile" tables in MVP while supporting all sections listed in PART 3 and PART 4 via normalized rows.

---

## Links

| Link field | Links to | Required |
|------------|----------|----------|
| **Brand** | Brand Setup - Brand Basics | Required when Explorer Type = Brand Explorer |
| **Operator / Management Company** | Operator Setup - Master | Required when Explorer Type = Operator Explorer |
| **Supporting Facts** | Partner Intelligence - Extracted Facts (multiple) | ≥1 approved fact before publish |
| **Primary Source** | Partner Intelligence - Source Library | Highest-quality supporting source |

---

## Field specification

| Airtable field name | Type | Notes |
|---------------------|------|-------|
| **Profile Type** | Single select | Brand, Operator, Parent Company, Service Provider, Other |
| **Explorer Type** | Single select | Brand Explorer, Operator Explorer, Internal Intelligence |
| **Explorer Section** | Single line | e.g. `Franchise Economics`, `Leadership` |
| **Field Name** | Single line | Canonical registry key — **unique per brand/operator + field** |
| **Approved Value** | Long text | Live value shown in Explorer (after visibility filter) |
| **Display Label** | Single line | Optional owner-facing label override |
| **Public Visibility** | Single select | Public, Internal Only, Restricted |
| **Overall Source Confidence** | Single select | High, Medium, Low (rollup from supporting facts) |
| **Last Reviewed Date** | Date | |
| **Reviewed By** | Collaborator / Users | |
| **Publish Status** | Single select | Draft, Published, Superseded, Withdrawn |
| **Published At** | Date | |
| **Stale?** | Checkbox | Set when linked source marked Stale |
| **Data Gap?** | Checkbox | True when approved value is gap copy |
| **Reviewer Notes** | Long text | Internal |
| **Registry Version** | Number | Schema version for field registry migrations |

---

## Brand Explorer sections supported (PART 3)

Each section is represented by one or more rows (`Explorer Section` + `Field Name`):

Brand Identity, Brand Positioning, Parent Company, Chain Scale, Brand Type, Target Guest, Target Owner Profile, Best-Fit Project Types, New Build Fit, Conversion Fit, Resort Fit, Urban Fit, Airport Fit, Mixed-Use Fit, Franchise Economics, Mandatory Fees, Optional Fees, Training Requirements, Opening Support, Sales / Revenue / Distribution Support, Loyalty / Reservations / Technology Systems, Brand Standards Flexibility, Operating Model, Staffing / Service Model, F&B Requirements, Design / Public Space / Room Requirements, Regional Relevance, Existing Footprint / Proof Points, Brand Differentiators, Deal Watchouts, Data Gaps, Questions to Confirm with Brand, Overall Source Confidence (rollup row), Last Reviewed Date (rollup row).

---

## Operator Explorer sections supported (PART 4)

**Canonical UI:** 11 tabs on `operator-explorer-gold-mock.html` (list popup). Published fields map to tabs 1–10 only.

| Tabs | Publish scope |
|------|---------------|
| 1–9 | Core factual profile sections (see list below) |
| 10 Operator Materials | Published material slots + Source Library links |
| 11 Dealality Insights | **Not published via Partner Intelligence** — Dealality-derived analysis |
| Alignment Context (`?dealId=`) | **Not published** — OAS deal alignment |

Section list (tabs 1–9 content): Company Snapshot, Ownership / Corporate Structure, Leadership, Regional Presence, Portfolio Size, Portfolio Composition, Brand Relationships, Independent / Soft Brand Experience, Resort Experience, Urban Experience, Select-Service Experience, Full-Service Experience, Lifestyle / Boutique Experience, Extended-Stay Experience, All-Inclusive Experience, Pre-Opening Support, Transition / Takeover Support, Conversion Support, Revenue Management, Sales & Marketing, Digital / Distribution, Accounting / Finance, HR / Talent, Procurement, Technology Stack, Owner Reporting, Budgeting / Forecasting, CAPEX Planning, Performance Reviews, Owner Engagement Cadence, Owner Portals / Tools, Reference Process, Deal Fit Profile, Preferred Project Types, Minimum / Ideal Hotel Size, Geographic Priorities, Differentiators, Watchouts, Data Gaps, Questions to Confirm, Overall Source Confidence, Last Reviewed Date.

**Deal Fit Profile** stores **factual operator-stated preferences** from sources — not computed OAS alignment or Dealality Insights scores.

---

## Read merge behavior (Phase 7)

`GET /api/brand-library/brand` and operator detail API will:

1. Load existing Setup + presentation data (unchanged).
2. Load Published rows where `Publish Status = Published` and `Stale? = false`.
3. Overlay `Approved Value` onto response paths defined in `partner-intelligence-explorer-field-registry.js`.
4. Attach internal-only `_partnerIntelligence` metadata for admin users (evidence refs, confidence — never in public owner views).

**Default:** Setup data wins when no published row exists (no regression).

**Optional flag:** `PARTNER_INTELLIGENCE_PUBLISH_OVERLAY=1` to enable merge.

---

## API mapping

`MAP_PARTNER_PUBLISHED` in `api/lib/partner-intelligence-field-map.js`.
