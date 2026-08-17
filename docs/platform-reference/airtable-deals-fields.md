# Airtable Deals Field Reference

Practical markdown reference for the **Deals** table and linked deal child tables used by Deal Setup, My Deals, snapshots, and deal requests.

> **Do not invent fields from this document alone.** Live Airtable + code field maps remain authoritative.

---

## Purpose

This document summarizes deal-related Airtable tables and fields so Cursor and AI builds can:

- Route Deal Setup PATCH writes to the correct table
- Understand snapshot and alignment dependencies on deal inputs
- Avoid inventing columns or mislabeling platform-derived outputs as owner facts
- Respect access, workflow, and protected-field rules

For cross-table index and base topology, see [DATA_DICTIONARY.md](./DATA_DICTIONARY.md).

---

## Source of Truth

| Priority | Location |
|----------|----------|
| 1 | **Live Airtable** — operational SSOT |
| 2 | **`api/schemas/deal-setup-fields.js`** — Deal Setup table names, routing, form↔column maps, readiness fields |
| 3 | **`api/my-deals.js`** — GET merge, list shaping, Deal Brand Cache, NDA template on Deals |
| 4 | **`api/schemas/intake-deal-fields.js`** — intake create defaults (`User_ID`, status/stage field IDs) |
| 5 | **`lib/operator-capability-inputs.js`** — Operator Capability P0 canonical names |
| 6 | **`lib/operator-alignment-field-options.js`** — OAS deal field names (incl. proposed Phase 5B) |
| 7 | **`api/operator-deal-requests-fields.js`**, **`api/brand-deal-requests.js`** — request table fields |
| 8 | **`api/deal-readiness-review.js`**, **`api/brand-alignment-snapshot.js`**, **`api/operator-alignment-snapshot.js`** — snapshot read/compute paths |

**Derived doc:** This markdown is compiled from the files above plus [operator-alignment-recommended-airtable-fields.md](../operator-alignment-recommended-airtable-fields.md) and [operator-capability-inputs-v1.md](../operator-capability-inputs-v1.md). If code and Airtable diverge, trust Airtable and update code/docs together.

---

## Deals Table

| | |
|--|--|
| **Table name** | `Deals` (`AIRTABLE_TABLE_DEALS`) |
| **Base** | `AIRTABLE_BASE_ID` (Deal Capture MVP) |
| **Purpose** | Owner opportunity record; deal identity; amenities on Deals; readiness persistence; access scoping |
| **Primary code** | `api/schemas/deal-setup-fields.js`, `api/my-deals.js` |

### Key identity & linking fields

| Field | Purpose | Source | Read/write | Notes |
|-------|---------|--------|------------|-------|
| `Property Name` | Primary deal label in UI | deal-setup, my-deals | R/W (owner PATCH, intake) | **Confirmed live** — list also reads `Project Name`, `Name` as aliases |
| `Project Name` | Alternate deal name | my-deals | R | **Alias** of `Property Name` in audited base |
| `Name` | Alternate deal name | my-deals | R | **Alias** of `Property Name` in audited base |
| `User_ID` | Link to Users (legacy secondary access) | intake-deal-fields | R/W intake; R access | New pilots: prefer `Company Profile` |
| `Company Profile` | Link to Company Profile (deal ownership) | pilot-field-registry, deal-record-access | R/W provisioning | **Required for new pilots** |
| `Deal Status` | Workflow status (`AIRTABLE_DEALS_STATUS_FIELD`) | deal-setup-fields | R/W | Live column; env may alias to legacy `Status` |
| Stage field (intake) | e.g. `Concept` on create | intake-deal-fields (`flde0PSEQUhA9Jl5a`) | W intake | **Needs Verification** for live column name |
| `Linked Market Record ID` | Cross-base market link (optional) | `.env.example`, market-demand-fields | R/W | Platform base mirror |
| `NDA Template File` | Attachment for BDR NDA send flow | my-deals | R/W | Used when brand requests NDA |

**Link fields to child tables** (on Deals row):

| Link field | Child table |
|------------|-------------|
| `Location & Property` (alias: `Location and Property`) | Location & Property |
| `Market - Performance - Deal & Capital Structure` | Market - Performance |
| `Strategic Intent - Operational - Key Challenges` | Strategic Intent |
| `Contact & Uploads` | Contact & Uploads |

> **Lease Structure link:** Code references `Lease Structure` on Deals (`LEASE_STRUCTURE_LINK_FIELD`), but the **audited live base** links lease rows from the child table via `Deal_ID` only — no `Lease Structure` link field on the Deals row. Treat child `Deal_ID` as the live link pattern.

### Owner / opportunity fields (Deals table only)

From `DEALS_ONLY_FORM_FIELDS` in `deal-setup-fields.js`:

| Field | Purpose | Snapshot use |
|-------|---------|--------------|
| `Property Name` | Deal identity | Readiness, BAS, My Deals |
| `Project Type` | New build / conversion / etc. | Readiness, BAS, OAS pathways |
| `Stage of Development` | Development stage | Readiness |
| `Expected Opening or Rebranding Date` | Timing | Readiness (contextual) |
| `F&B Complexity` | F&B complexity | OAS scoring (planned/live per base) |
| `Opening Timeline` | Timeline band | OAS scoring (planned/live per base) |
| `Current Operating Model` | Operator Capability P0 | Readiness (conditional), OCS, OAS |
| `Opening / Transition Phase` | Operator Capability P0 | Readiness, OCS |
| `Has there ever been a franchise… agreement…?` | Brand history (note typo column `agreeement` in some bases) | Readiness |
| `Is the hotel currently branded?` | Brand state | Readiness, BAS |
| `Is the hotel currently managed by a third-party operator?` | Operator state | Readiness, OAS |
| `Are you open to considering other brands with favorable terms?` | Brand openness | Readiness (maps to `…emerging brands…` column) |
| `Have you worked with any of your preferred brands/operators before?` | Relationship history | Readiness |
| `Operator Name Current` | Current operator name | Readiness, OAS |
| `Current Brand Affiliation` | Current brand | BAS |
| `Parent Company Name` | Parent company | BAS |
| `F&B Outlets?`, `Meeting Space`, `Number of Meeting Rooms`, `Condo Residences?`, `Hotel Rental Program?`, `Parking Amenities?`, `Additional Amenities` | Amenities & facilities (section 5 → **Deals**, not Location) | Readiness, BAS |

### Deal readiness fields (platform-derived, on Deals)

Written by `POST /api/ai/deal-readiness-review` (`api/deal-readiness-review.js`):

| Field | Purpose | Type | Governance |
|-------|---------|------|------------|
| `Deal Readiness Score` | 0–98 capped completeness score | Number | **Interpretation** — draft for validation, not fact |
| `Deal Readiness Stage` | Stage label (e.g. maps `Ready for External Review` → `Ready` on write) | Single select | **Interpretation** |
| `Deal Readiness Last Reviewed` | Timestamp of last review save | Date/datetime | Metadata |
| `Deal Readiness Summary` | Optional long-text summary | Long text | **Interpretation** — only if `DEAL_READINESS_SUMMARY_FIELD` env set |
| `Deal Readiness Missing Count` | Missing field count | Number | **Needs Verification** — read in list if column exists |
| `Deal Readiness Blocking Count` | Blocking gap count | Number | **Needs Verification** |

Env overrides: `DEAL_READINESS_*_FIELD` in `deal-setup-fields.js`.

### Snapshot-related supporting fields (Deals)

Brand Alignment Snapshot (`api/brand-alignment-snapshot.js`) reads merged deal fields — **does not persist BAS narrative to Deals** (computed API response). Key Deals-side inputs include `Project Type`, brand/operator state fields, amenities, and identity fields.

Operator Alignment uses deal context from merged fields + Strategic Intent + Market Performance; named-operator scores live on **Operator Deal Requests**, not Deals.

### Missing data / clarification (computed, not always stored)

Deal Readiness Review returns `missingInformation`, `weakInformation`, `blockingIssues` in API payload from `REQUIRED_DEAL_SETUP_FIELDS` + conditional operator-capability requirements (`operatorCapabilityConditionalRequiredFields`). Brand Alignment reuses readiness gaps as `validationItems` / `clarificationAreas`.

---

## Deal Child Tables

### Location & Property

| | |
|--|--|
| **Link** | `Location & Property` on Deals |
| **Purpose** | Address, site, building specs, geo/market region |
| **Key fields** | `Full Address`, `City`, `Country`, `Hotel Type`, `Hotel Chain Scale`, `Hotel Submarket & Location`, `Hotel Service Model`, `Primary Market Region`, room counts, zoning, site control, `Total Number of Rooms/Keys`, `# of Stories`, etc. |
| **Naming notes** | `Site/Development Restrictions Description` (form) → live `Site Restrictions Describe` (mapped in `LOCATION_FORM_TO_AIRTABLE`). `Ownership Type Other Text` / `Zoning Status Other Text` created via Phase 5B P1 setup when UI Other path is used. |
| **Source** | `LOCATION_FORM_TO_AIRTABLE` in `deal-setup-fields.js` |
| **Read/write** | Owner PATCH routes Location form keys here |
| **Snapshots** | Readiness, BAS (location/scale), OAS (`Primary Market Region`, `Country`) |
| **Status** | Verified via code map |

### Market - Performance - Deal & Capital Structure

| | |
|--|--|
| **Link** | `Market - Performance - Deal & Capital Structure` on Deals |
| **Child link** | `Deal_ID` → Deals |
| **Purpose** | Demand, costs, capital structure, fee expectations |
| **Key fields** | `Primary Demand Drivers`, `Estimated or Actual RevPAR`, `Total Project Cost Range`, `Preferred Deal Structure`, `PIP / CapEx Status`, `Royalty Fee Expectations`, `Marketing Fee Expectations`, `Loyalty Fee Expectations`, encumbrance fields, etc. |
| **Source** | `MARKET_PERFORMANCE_FIELD_NAMES`, `MP_FORM_TO_TABLE` in `deal-setup-fields.js` |
| **Read/write** | Owner PATCH; merged onto `deal.fields` for reads |
| **Snapshots** | BAS (`Preferred Deal Structure`), lease visibility, OAS commercial factors |
| **Caution** | `Preferred Deal Structure` is **brand/franchise economics** — OAS docs warn not to use alone for operator structure score |
| **Proposed** | `Preferred Operator Management Structure` — **Phase 5B P1**; exposed in Deal Setup (`deal-setup.html`, `new-deal-setup.html`) |

### Strategic Intent - Operational - Key Challenges

| | |
|--|--|
| **Link** | `Strategic Intent - Operational - Key Challenges` on Deals |
| **Purpose** | Brand path, operator path, owner priorities, deal breakers |
| **Key fields (brand path)** | `Soft vs Hard Brand Preference`, `Preferred Brands` (form: `Preferred Brands (up to 4)`), `Preferred Chain Scales`, `Target Guest Segment`, importance sliders, `Must-Haves From Brand/Operator`, etc. |
| **Key fields (operator path)** | `Plan to Self-Manage or Hire Third Party?`, `Preferred Future Operating Model`, `Operator Strategy Status`, `Services Required From Operator`, `Operator Capability Priorities`, `Preferred Third-Party Operator Profile`, `Preferred Management Structure`, `Required Operator Services`, `Must-Have Operator Services`, reporting fields, etc. |
| **Source** | `STRATEGIC_INTENT_FORM_FIELDS`, `SI_FORM_TO_AIRTABLE` in `deal-setup-fields.js` |
| **Read/write** | Owner PATCH |
| **Snapshots** | Readiness, BAS (preferred brands, pathway), OAS (most operator alignment inputs), Operator Capability Snapshot |
| **Proposed fields** | Phase 5B P1: `Operator Structure Intent` exposed in Deal Setup (Strategic Intent section). `Brand Affiliation Path` (P2) still optional. **F&B:** use Deals `F&B Complexity` — not SI `F&B Complexity Level`. |

### Contact & Uploads

| | |
|--|--|
| **Link** | `Contact & Uploads` on Deals |
| **Child link** | `Deal_ID` |
| **Purpose** | Contacts, advisor flags, supporting document attachments |
| **Key fields** | `Main Contact Name`, `Email Address`, `Entity or Company Name`, filter/consultant flags, `Pro Forma or Financials` (default attachment bucket), other columns in `AIRTABLE_CU_ATTACHMENT_FIELDS` |
| **Naming notes** | `Secondary Contact` → live `Secondary Contact (Name & Email)`. `Broker/Advisor Company and Contract Details` (form) → `Working with Broker/Advisor? Text`; `Broker/Firm Name` is separate live column. |
| **Source** | `CONTACT_UPLOADS_FORM_FIELDS`, `CU_ATTACHMENT_*` in `deal-setup-fields.js` |
| **Read/write** | Owner PATCH; Tab 13 uploads to attachment field(s) |
| **Status** | Verified |

### Lease Structure

| | |
|--|--|
| **Child link** | `Deal_ID` → Deals (confirmed live) |
| **Purpose** | Lease terms when `Preferred Deal Structure` is `Lease` or `Flexible/Open` |
| **Key fields** | `Lease Type`, term dates, rent, CAM, renewal, termination, notes |
| **Source** | `LEASE_STRUCTURE_FORM_FIELDS` in `deal-setup-fields.js` |
| **Read/write** | Owner PATCH when lease section applicable |
| **Readiness** | Lease fields required only when `isLeaseStructureDealApplicableFromMergedFields` is true |

---

## Request & Cache Tables (deal-linked)

### Brand Deal Requests

| | |
|--|--|
| **Table** | `Brand Deal Requests` (`AIRTABLE_TABLE_BRAND_DEAL_REQUESTS`) |
| **Link** | `Deal` → Deals |
| **Purpose** | Per-deal brand outreach, NDA/deal room, proposals |
| **Key fields** | `Brand Name`, `Status`, `Request Sent At`, `Match Score`, `Owner Notes`, `Response Notes`, NDA fields (`NDA Required?`, `NDA Status`, `Deal Room Access`), proposal fields (`Proposal Status`, fee/term proposal columns), follow-up notes (internal/external) |
| **Naming notes** | `Next Follow-up Notes (External)` → live `Next Follow-up Notes`. Internal notes: `Next Follow-up Notes (Internal)` only (`Brand Internal Notes` deprecated). |
| **Source** | `api/brand-deal-requests.js` |
| **Read/write** | Owner/brand workflows; proposal draft must not be blindly overwritten |
| **Governance** | `Match Score` is platform-derived — user-facing **Brand Alignment** language, not “validated fit” |
| **Status** | Verified from API constants (large proposal field set — see code for full list) |

### Operator Deal Requests

| | |
|--|--|
| **Table** | `Operator Deal Requests` |
| **Link** | `Deal`, `Operator Setup` |
| **Purpose** | Operator alignment requests per deal |
| **Key fields** | `Operating Company Name`, `Status`, `Alignment Score`, `Alignment Band`, `Data Confidence`, NDA/deal-room mirrors, follow-up notes |
| **Source** | `api/operator-deal-requests-fields.js` (`MAP_ODR_AIRTABLE`) |
| **Read/write** | `lib/dealality/odr-owner-create.js`, ODR APIs |
| **Governance** | Scores are **interpretation**; label **Operator Alignment Snapshot** in product copy |
| **Status** | Verified for `MAP_ODR_AIRTABLE` |

### Deal Brand Cache

| | |
|--|--|
| **Table** | `Deal Brand Cache` (`AIRTABLE_TABLE_DEAL_BRAND_CACHE`) |
| **Link** | `Deal` |
| **Purpose** | Cached brand alignment scoring for My Deals performance |
| **Key fields** | `Preferred Brands`, `Preferred Scores`, `Top Alternatives`, `Preferred Score`, `Best Match Brand`, `Best Match Score`, `Breakdown Details By Brand`, `Last Computed At` |
| **Source** | `api/my-deals.js` |
| **Read/write** | Written by alignment compute path — **platform-derived** |
| **Caution** | Internal naming uses “Best Match” — avoid user-facing “best brand” claims per naming guide |
| **Status** | **Needs Verification** if table absent in some bases |

### Deal Activity Log / Proposal Submissions / Deal Room Documents

| Table | Purpose | Env | Audit status (2026-07) |
|-------|---------|-----|------------------------|
| `Deal Activity Log` | Audit trail (brand/deal actions) | `AIRTABLE_TABLE_DEAL_ACTIVITY_LOG` | **Table confirmed live.** Fields: `Deal`, `Brand Name`, `Action`, `Details`, `Stakeholder`, `Created At`, `Subject`, `Message_Summary`, `Operating Company Name`, `Seed Batch ID` |
| `Proposal Submissions` | Proposal history snapshots | `AIRTABLE_TABLE_PROPOSAL_SUBMISSIONS` | **Not found** in audited base (`appvtnDurnMSjINP6`) — optional; keep in audit scope |
| `Deal Room Documents` | Deal room files | `AIRTABLE_TABLE_DEAL_ROOM_DOCUMENTS` | **Table confirmed live.** Fields: `Document Name`, `Deal`, `Category`, `Confidentiality`, `File`, `Uploaded By`, `Party Folder`, `Uploaded At` |

Field lists for Activity Log and Deal Room Documents are from live Meta API audit; write-path maps in code are **not yet complete**.

---

## Snapshot Dependencies

### Deal Readiness Snapshot

| Input source | Fields used |
|--------------|-------------|
| Deals | Identity, brand/operator state, amenities, readiness persisted fields |
| Location & Property | Address, scale, `Primary Market Region`, room counts, zoning |
| Market - Performance | RevPAR, costs, `Preferred Deal Structure`, regulatory |
| Strategic Intent | Brand + operator strategy, priorities, must-haves |
| Contact & Uploads | Contact completeness |
| Lease Structure | Conditional lease fields |

**Output:** Score/stage (+ optional summary) **written to Deals** on save. API also returns gap lists (interpretation).

**Code:** `api/deal-readiness-review.js`, `api/deal-readiness-context.js`, `REQUIRED_DEAL_SETUP_FIELDS_BY_SECTION` in `deal-setup-fields.js`.

### Brand Alignment Snapshot

| Input source | Fields used |
|--------------|-------------|
| Merged deal fields | `Project Type`, positioning, key count, `Preferred Brands`, soft/hard preference, `Preferred Deal Structure`, readiness-driven gaps |
| Brand library | Brand reference / presentation data (not Deals columns) |
| Deal Brand Cache | Cached scores when present |

**Output:** API response only (executive summary, pathway view, per-brand rationale). **Needs Verification** for any persistent BAS columns on Deals.

**Code:** `api/brand-alignment-snapshot.js`, `lib/brand-alignment-rationale.js`.

### Operator Alignment Snapshot

| Input source | Fields used |
|--------------|-------------|
| Strategic Intent | Operator services, management structure, reporting, scope fields (see `OAS_DEAL_SI_FIELD_NAMES`) |
| Deals | `F&B Complexity`, `Opening Timeline`, operating model fields | **F&B SSOT:** `F&B Complexity` on Deals (`OAS_DEAL_DEALS_FIELD_NAMES.fbComplexity`) — not SI `F&B Complexity Level` |
| Location | `Country`, `Primary Market Region` |
| Market - Performance | `Preferred Deal Structure` (legacy conflation risk) |
| Operator Setup | Operator profile fields (separate table) |

**Output:** Profile-mode API (`/api/operator-alignment-snapshot/:dealId/profile`); company-mode scores on **Operator Deal Requests**.

**Code:** `api/operator-alignment-snapshot.js`, `lib/operator-alignment-scoring-weight-config.js`.

### My Deals preview

List rows use Deals identity fields, `Deal Status`, readiness list fields (`extractDealReadinessListFields`), Company Profile / User_ID access, optional Deal Brand Cache hydration, linked BDR counts.

**Code:** `api/my-deals.js`, `lib/dealality/filter-my-deals.js`.

### Owner activation / readiness logic

- **Access:** `Company Profile` link on Deals (+ optional `User_ID`) via `lib/dealality/deal-record-access.js`
- **Readiness gating:** BAS and external review copy reference readiness stage; no separate “activation” column documented on Deals — **Needs Verification** for any `Activation*` fields in live base

---

## AI / Platform-Derived Deal Fields

| Field / output | Table | Fact / interpretation / action | Confidence metadata |
|----------------|-------|--------------------------------|---------------------|
| `Deal Readiness Score` | Deals | **Interpretation** (completeness) | Stage + missing counts in API |
| `Deal Readiness Stage` | Deals | **Interpretation** | — |
| `Deal Readiness Summary` | Deals | **Interpretation** | Optional |
| Readiness gap arrays | API only | **Next action** (what to clarify) | Severity from context profile |
| `Match Score` | Brand Deal Requests | **Interpretation** | Not “validated” |
| `Alignment Score` / `Alignment Band` | Operator Deal Requests | **Interpretation** | `Data Confidence` on ODR |
| Deal Brand Cache JSON/scores | Deal Brand Cache | **Interpretation** | `Last Computed At` |
| BAS / OAS narrative | API response | **Interpretation** + questions to clarify | Readiness + data gaps in payload |

**Governance:** Do not overwrite owner-entered deal inputs when refreshing scores. Recompute from inputs; label outputs per [INTELLIGENCE_GOVERNANCE.md](../data-intelligence/INTELLIGENCE_GOVERNANCE.md).

---

## Protected / Caution Fields

| Field / area | Caution |
|--------------|---------|
| `Company Profile` on Deals | Admin/provisioning — required for pilot scoping; do not reassign casually |
| `User_ID` | Legacy access path; do not use as sole access for new pilots |
| `Deal Status` | Workflow implications for My Deals filters |
| Readiness score/stage on Deals | Overwriting discards human review context — only via readiness review save path |
| Deal Brand Cache | Platform cache — safe to recompute, not owner source of truth |
| BDR proposal fields | Do not overwrite existing Draft/Submitted proposals (`api/brand-deal-requests.js`) |
| ODR `Alignment Score` | Workflow/analytics — not company-validated fit |
| Franchise affiliation column | Typo `agreeement` vs `agreement` — set `AIRTABLE_DEALS_FRANCHISE_AFFILIATION_FIELD` if base differs |
| Phase 5B Strategic Intent / MP fields | Documented in alignment doc — **do not write** until columns exist |

Users/Company Profile protected fields (not on Deals but affect deal access): see [DATA_DICTIONARY.md](./DATA_DICTIONARY.md#protected--never-write-fields).

---

## Live Schema Verification

Export the live Airtable schema and diff it against repo field maps (read-only — no data or schema writes):

```bash
npm run audit-airtable-deals-schema
```

**Requires:** `AIRTABLE_BASE_ID` and `AIRTABLE_API_KEY` (or `AIRTABLE_PAT` / `AIRTABLE_TOKEN`) with `schema.bases:read`.

**Outputs:**

- `reports/airtable-deals-schema-live.json` — live Meta API field inventory
- `reports/airtable-deals-schema-diff.md` — exact matches, alias matches, missing expected fields, undocumented live fields, Phase 5B confirmation

**First audited base (`appvtnDurnMSjINP6`, 2026-07):** 11/12 tables found; `Proposal Submissions` absent (optional). Alias matches resolved naming drift (`Deal Status`/`Status`, `Property Name`/`Project Name`, SI `* Other Text` columns, etc.). Phase 5B: 12/16 fields confirmed (4 P1 fields not live).

Use the diff report to close **Needs Verification** items in this doc and [DATA_DICTIONARY.md](./DATA_DICTIONARY.md). Re-run after schema changes or env column overrides.

**Setup script:** `npm run setup-deals-schema-phase5b-p1 -- --dry-run` then `--apply` for approved P1 fields + Location Other text columns.

**Remaining gaps:** See [deals-schema-finalization-plan.md](./deals-schema-finalization-plan.md) for founder decisions, Airtable vs code actions, and build sequencing.

---

## Open Questions / Gaps

1. **Rollups/formulas on live Deals** — many live-only columns (undocumented in registry) are expected; add to registry when code reads them.
2. **`Project Name` / `Name` vs `Property Name`** — confirmed as read aliases; write path primarily uses `Property Name`.
3. **Phase 5B P1** — `Preferred Operator Management Structure`, `Operator Structure Intent` via setup script; `Brand Affiliation Path` still P2 optional.
4. **F&B** — Deals `F&B Complexity` is OAS SSOT; do not create SI `F&B Complexity Level`.

---

## Recommended Next Steps

1. Review [deals-schema-finalization-plan.md](./deals-schema-finalization-plan.md) for founder decisions and sequencing.
2. Run `npm run audit-airtable-deals-schema` and review `reports/airtable-deals-schema-diff.md`.
3. Run `npm run verify-deal-setup-routing` to validate form→table routing.
4. Confirm Phase 5B P1 fields per finalization plan before OAS scoring work.
5. Add `docs/platform-reference/airtable-brand-deal-requests-fields.md` if BDR proposal surface grows.

---

## Related

- [DATA_DICTIONARY.md](./DATA_DICTIONARY.md)
- [operator-alignment-recommended-airtable-fields.md](../operator-alignment-recommended-airtable-fields.md)
- [operator-capability-inputs-v1.md](../operator-capability-inputs-v1.md)
- [NAMING_AND_COPY_GUIDE.md](../ai-build-system/NAMING_AND_COPY_GUIDE.md)
