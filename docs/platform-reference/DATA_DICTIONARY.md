# Dealality Data Dictionary

Practical field and table reference for Cursor, AI builds, Airtable-safe work, validation-aware content, and future platform changes.

> **v1** — Summaries + source links. Per-table field authority remains in `docs/*-airtable-fields.md`, `.env.example`, and central `map_*` / `api_*` objects in code. Do not invent fields from this file alone.

---

## Purpose

This dictionary helps builders quickly answer:

- Which Airtable base and table owns a concept?
- What is safe to read vs write?
- Which fields are protected, AI-derived, or governance-sensitive?
- Where is the full field list documented?

Use alongside [INTELLIGENCE_GOVERNANCE.md](../data-intelligence/INTELLIGENCE_GOVERNANCE.md) for validation levels and trust labels, and [brand-operator-validation-fields-plan.md](../data-intelligence/brand-operator-validation-fields-plan.md) for Brand/Operator governance rollout (post–Deals schema cleanup).

---

## Source of Truth Rules

- **Airtable** is the operational source of truth unless a future migration changes this.
- **Memberstack** is identity/auth only — not business-data SSOT.
- **Do not invent** Airtable fields, tables, or select options.
- **Do not write** protected fields unless explicitly approved (see below).
- **Company-validated data** must not be overwritten automatically by AI or sync jobs.
- **AI-assisted / source-informed** content should carry validation and confidence metadata where the schema supports it (see [Validation / Governance Fields](#validation--governance-fields)).
- **Schema authority:** matching `docs/*-airtable-fields.md` + `.env.example` + code field maps before any write path.
- **Dry-run first:** scripts with `--apply` or live upsert require `--dry-run` review.

---

## How To Use This Document

**Read this file before tasks touching:**

- Airtable reads/writes or schema changes
- API routes that PATCH Airtable
- Users, Company Profile, deal access
- Brand Explorer, Operator Explorer, Brand/Operator Setup
- Owner deals / My Deals, deal requests, snapshots
- Hotel census, demand anchors, travel infrastructure, brand alias mapping
- GTM / Pilot Target List (internal only)
- Partner intelligence extraction or publish workflows

**Then drill into:**

1. The linked schema doc or code `map_*` file for that table.
2. [DATA_VALIDATION_PROTOCOL.md](../data-intelligence/DATA_VALIDATION_PROTOCOL.md) if content/intelligence is involved.
3. [TESTING_PROTOCOL.md](./TESTING_PROTOCOL.md) / [dealality-pr-validation-matrix.md](../dealality-pr-validation-matrix.md) before merge.

---

## Airtable Base Topology

| Base env | Typical name | Role |
|----------|--------------|------|
| `AIRTABLE_BASE_ID` | Deal Capture MVP | Users, Deals, Brand/Operator Setup, partner intelligence, deal workflow |
| `AIRTABLE_BASE_ID_ALT` | Deal Capture Platform | Hotel Census, Brand Alias Mapping, Demand Anchors, Travel Infrastructure, Market Demand |
| `AIRTABLE_GTM_BASE_ID` | GTM / Owner Targets | CoStar GTM, Pilot Target List, Founder Project Plan — **internal only, never product-facing** |

---

## Known Airtable Bases / Tables

### Users

| | |
|--|--|
| **Table** | `Users` (`USERS_TABLE_ID` in `.env.example`) |
| **Base** | `AIRTABLE_BASE_ID` |
| **Purpose** | Identity mirror, onboarding, link to Company Profile |
| **Key fields** | `Unique Webflow ID` / Memberstack Member ID (primary), `Slug` (mirror), `Account Status`, `User Type` / role hints, `Company Profile` (link), `Company Name`, contact fields |
| **Read/write** | Signup/Memberstack sync: onboarding fill-if-blank only; **never** write permission fields |
| **Protected** | See [Protected / Never-Write Fields](#protected--never-write-fields) |
| **APIs / code** | `lib/signup-airtable-upsert.js`, `lib/airtable-users-protected-patch.js`, `lib/dealality/resolve-user.js`, `/api/me` |
| **Source docs** | `.env.example`, `lib/pilot-provisioning/pilot-field-registry.js`, `lib/support/owner-pilot-provisioning-runbook.js` |
| **Status** | Verified (live base references in runbook) |

---

### Company Profile

| | |
|--|--|
| **Table** | `Company Profile` |
| **Base** | `AIRTABLE_BASE_ID` |
| **Purpose** | **Workspace permissions SSOT** — company type, region/deal access, workspace capabilities |
| **Key fields** | `Workspace Access` (`fldhZqzi0LskI0MpK`), `Company Type`, `Region Access`, `Deal Access` (Needs Verification on exact column set) |
| **Read/write** | Provisioned by admin/pilot scripts — **not** from signup/webhook |
| **Protected** | Treat all permission fields as admin-only writes |
| **APIs / code** | `lib/pilot-provisioning/pilot-field-registry.js`, `lib/dealality/deal-record-access.js`, `lib/dealality/resolve-user.js` |
| **Source docs** | `lib/support/owner-pilot-provisioning-runbook.js` |
| **Status** | Verified for Workspace Access sourcing rule |

---

### Deals (Owner / My Deals)

| | |
|--|--|
| **Table** | `Deals` (`AIRTABLE_TABLE_DEALS`) |
| **Base** | `AIRTABLE_BASE_ID` |
| **Purpose** | Owner opportunity records; deal readiness; access scoping |
| **Key fields** | `Deal Status`, `Company Profile` (link — **required for new pilots**), `User_ID` (legacy secondary), Deal Readiness: `Deal Readiness Score`, `Deal Readiness Stage`, `Deal Readiness Last Reviewed` (optional summary/count fields via env) |
| **Linked child tables** | `Location & Property`, `Market - Performance - Deal & Capital Structure`, `Strategic Intent - Operational - Key Challenges`, `Contact & Uploads`, `Lease Structure` |
| **Read/write** | Owner PATCH via `api/my-deals.js` + `api/schemas/deal-setup-fields.js`; readiness fields written by review flows |
| **Protected** | Access fields on Users/Company Profile — not on Deals; do not bypass `deal-record-access.js` |
| **Snapshots** | Deal Readiness Snapshot reads/writes readiness fields on Deals (not separate table) |
| **APIs / code** | `api/schemas/deal-setup-fields.js`, `lib/dealality/filter-my-deals.js`, `lib/dealality/owner-deal-id-access.js` |
| **Source docs** | [airtable-deals-fields.md](./airtable-deals-fields.md), `api/schemas/deal-setup-fields.js`, [operator-alignment-recommended-airtable-fields.md](../operator-alignment-recommended-airtable-fields.md), [deals-schema-finalization-plan.md](./deals-schema-finalization-plan.md) |
| **Status** | Verified for core link + readiness fields |

---

### Brand Deal Requests

| | |
|--|--|
| **Table** | `Brand Deal Requests` (`AIRTABLE_TABLE_BRAND_DEAL_REQUESTS`) |
| **Base** | `AIRTABLE_BASE_ID` |
| **Purpose** | Brand-side deal requests / target list per deal |
| **Key fields** | `Deal` (link), brand/status fields — see `api/schemas/brand-deal-request-fields.js` |
| **Read/write** | BDR APIs; access via `lib/dealality/bdr-record-access.js` |
| **APIs / code** | `api/schemas/brand-deal-request-fields.js`, `lib/dealality/target-list-batch-access.js` |
| **Status** | Needs Verification for full field inventory in this dictionary |

---

### Operator Deal Requests (My Operator Deals)

| | |
|--|--|
| **Table** | `Operator Deal Requests` (`AIRTABLE_TABLE_OPERATOR_DEAL_REQUESTS`) |
| **Base** | `AIRTABLE_BASE_ID` |
| **Purpose** | Operator alignment requests per deal |
| **Key fields** | `Deal`, `Operating Company Name`, `Operator Setup` (link), `Status`, `Alignment Score`, `Alignment Band`, `Data Confidence`, NDA/deal-room fields, follow-up notes (internal vs external) |
| **Read/write** | `api/operator-deal-requests-fields.js`, `lib/dealality/odr-owner-create.js` |
| **AI / governance** | `Alignment Score`, `Alignment Band`, `Data Confidence` — platform-derived; label in UI per [NAMING_AND_COPY_GUIDE.md](../ai-build-system/NAMING_AND_COPY_GUIDE.md) |
| **Source docs** | `api/operator-deal-requests-fields.js`, [operator-deal-requests-phase-2-scoping.md](../operator-deal-requests-phase-2-scoping.md) |
| **Status** | Verified for mapped fields in `MAP_ODR_AIRTABLE` |

---

### Brand Setup / Brand Explorer

| | |
|--|--|
| **Tables** | `Brand Setup - Brand Basics`, `Brand Footprint`, `Project Fit`, `Portfolio & Performance`, `Brand Standards`, `Fee Structure`, `Deal Terms`, `Operational Support`, `Legal Terms`, `Loyalty & Commercial`, `Sustainability & ESG`, **`Brand Setup - Brand Explorer Presentation`** |
| **Base** | `AIRTABLE_BASE_ID` |
| **Purpose** | Brand intake + Explorer presentation rows |
| **Key fields (Presentation)** | Section/slot presentation content — see [brand-explorer-presentation-slots.md](../brand-explorer-presentation-slots.md); fixtures in `fixtures/brand-explorer-presentation-*` |
| **Read/write** | GET/PATCH via `api/brand-library.js`; factory/apply scripts for presentation patches |
| **AI / governance** | Presentation content often AI-assisted or source-informed until company-validated; apply gap audits before PR |
| **APIs / code** | `api/brand-library.js`, `lib/brand-explorer/*`, `npm run brand-explorer:factory` |
| **Source docs** | [brand-explorer-factory.md](../brand-explorer-factory.md), [choice-explorer-presentation-gap-audit.md](../choice-explorer-presentation-gap-audit.md) |
| **Status** | Verified for table names in `api/brand-library.js` |

---

### Operator Setup / Operator Explorer

| | |
|--|--|
| **Master table** | `Operator Setup - Master` (`AIRTABLE_OPERATOR_SETUP_MASTER_TABLE`) |
| **Child / related tables** | `Operator Setup - Platform & Markets`, `Profile & Positioning`, `Commercial Fit & Terms`, `Governance, Delivery & Diligence`, `Engagement & Reporting`, `Explorer Materials`, `Leadership Platform`, `Leadership Team Members`, case studies — see per-tab schema docs |
| **Base** | `AIRTABLE_BASE_ID` |
| **Purpose** | Operator intake (13-tab setup) + gold Explorer profiles keyed by Master `rec…` |
| **Key fields (Master)** | `company_name`, `submission_status` (active filter), **`Data Confidence Level`** (profile governance confidence alias — not a separate `Confidence Level` column), `Profile Last Reviewed` / `Last Reviewed Date` (recommended in alignment docs) |
| **JSON explorer fields** | 22 `*_json` multilineText fields — [operator-dna-explorer-json-fields.md](../operator-dna-explorer-json-fields.md), `lib/operator-dna-explorer-json-fields.js` |
| **Read/write** | Legacy writer primary (`OPERATOR_SETUP_USE_NEW_BASE_WRITER=0`); new-base writer + shadow flag optional |
| **Snapshots** | Operator Alignment Snapshot — API-computed; ODR stores `Alignment Score` / `Alignment Band` / `Data Confidence` |
| **APIs / code** | `api/lib/operator-setup-new-base-writer.js`, `public/js/operator-explorer*.js`, `api/operator-alignment-snapshot.js` |
| **Source docs** | `docs/operator-*-explorer-airtable-fields.md`, [operator-setup-to-explorer-field-mapping-audit.md](../operator-setup-to-explorer-field-mapping-audit.md), [operator-alignment-recommended-airtable-fields.md](../operator-alignment-recommended-airtable-fields.md) |
| **Status** | Verified for Master + documented child tables; Phase 5B+ fields marked **proposed** in alignment doc |

---

### AI Visibility

| | |
|--|--|
| **Tables** | `AI Visibility - Prompts` (`tblsQyfNuNPkSR2G1`), `AI Visibility - Opportunities` (`tblGAoMaPqHwlYtyM`) |
| **Base** | `AIRTABLE_BASE_ID` |
| **Purpose** | Governed prompt SSOT + opportunity workflow structure. Raw runs/responses/mentions/citations stay in non-Airtable evidence store. |
| **Key fields (Prompts)** | `Prompt ID`, `Version`, `Prompt Family`, `Intent Territory`, `Geography Scope`, `Commercial Region`, `Country`, `Monitoring Eligible`, `Governance Status` — full live map in [ai-visibility-airtable-fields.md](../ai-visibility-airtable-fields.md) |
| **Key fields (Opportunities)** | `Opportunity ID`, geography fields, `Observation`, `Evidence Descriptor`, `Diagnostic Reason`, `Status`, `Interpretation Status` — no auto-generated commercial opportunities yet |
| **Read/write** | Schema: `scripts/ensure-ai-visibility-schema.mjs`; seed: `scripts/ai-visibility-seed-prompts.mjs`; loaders: `lib/ai-visibility/load-prompts.js`, `prompt-cohort.js` |
| **AI / governance** | Admin-governed prompts; no client Airtable access; no composite GEO score; Citation Rate PARTIAL pending brand-domain governance |
| **Source docs** | [FEATURE_BRIEF_AI_VISIBILITY.md](../ai-build-system/FEATURE_BRIEF_AI_VISIBILITY.md), [ai-visibility-airtable-fields.md](../ai-visibility-airtable-fields.md), `BUILD_DECISIONS.md` Phase 2D |
| **Status** | **Live** (Phase 2D apply 2026-08-13); 39 seeded prompts; 0 opportunity records |

---

### Partner Intelligence (proposed tables)

| | |
|--|--|
| **Tables** | `Partner Intelligence - Source Library`, `Partner Intelligence - Extracted Facts`, `Partner Intelligence - Published Explorer Fields`, `Partner Intelligence - Helena Outreach Intake` |
| **Base** | `AIRTABLE_BASE_ID` |
| **Purpose** | Source capture → extraction staging → human-approved Explorer overlay |
| **Governance fields** | Source Library: `Source Quality`, `Status`, `Verified Source?`, `Approved for Extraction?`, `Approved for Explorer Use?`; Facts: `Confidence Level`, `Human Review Status`, `Data Gap?`; Published: `Publish Status`, `Overall Source Confidence`, `Stale?` |
| **Read/write** | Feature-flagged (`PARTNER_INTELLIGENCE_*` in `.env.example`); **do not** write OAS scores into Facts/Published |
| **Source docs** | [partner-source-library-airtable-fields.md](../partner-source-library-airtable-fields.md), [partner-extracted-facts-airtable-fields.md](../partner-extracted-facts-airtable-fields.md), [partner-explorer-published-fields-airtable-fields.md](../partner-explorer-published-fields-airtable-fields.md) |
| **Status** | **Proposed** — not created until `ensure-partner-intelligence-tables.mjs --apply` with approval |

---

### Hotel Census

| | |
|--|--|
| **Table** | `Hotel Census` (`AIRTABLE_HOTEL_CENSUS_TABLE`) |
| **Base** | `AIRTABLE_BASE_ID_ALT` |
| **Purpose** | Legacy STR-backed production census (~15k rows); brand footprint rollups, Scout — **read-heavy** |
| **Key fields** | `name`, `Affiliation`, `Parent Company`, `status`, `rooms`, `country`, `city`, `Market`, `Submarket`, `Chain Scale`, `Management Company`, optional `Include in Brand Explorer`, `Data Confidence` |
| **Read/write** | Census enrichment scripts fill-blank only; STR import paths do not touch Brand Explorer or Brand Alias Mapping |
| **Protected (enrichment)** | `name`, `Affiliation`, `Parent Company` — `lib/hotel-census/brand-directory-enrichment-contract.js` |
| **Geography** | Product uses Dealality `Market` + corridor `Submarket` — not STR Market/Submarket as enrichment target ([AGENTS.md](../../AGENTS.md)) |
| **APIs / code** | `lib/hotel-census/fields.js`, `lib/hotel-census/*` |
| **Source docs** | `lib/hotel-census/fields.js`, [hotel-census-geography-population-rules.md](../hotel-census-geography-population-rules.md) |
| **Status** | Verified for core census fields in `CENSUS_FIELDS` |

---

### Brand Alias Mapping

| | |
|--|--|
| **Table** | `Brand Alias Mapping` (`AIRTABLE_BRAND_ALIAS_TABLE`) |
| **Base** | `AIRTABLE_BASE_ID_ALT` |
| **Purpose** | Map affiliation strings → canonical brand/parent for census rollups and Scout |
| **Key fields** | `Canonical Brand Name`, `Alias / Source Brand Name`, `Parent Company`, `Active`, `Match Confidence`, `Notes` |
| **Read/write** | Read in Scout/census scripts; writes via steward/backfill scripts only |
| **APIs / code** | `lib/hotel-census/brand-alias-resolve.js`, `lib/hotel-census/fields.js` (`ALIAS_FIELDS`) |
| **Status** | Verified |

---

### Verified Independent Hotel Census

| | |
|--|--|
| **Tables** | `Verified Independent Hotel Census` (golden master), `Independent Hotel Source Candidates`, `Independent Hotel Source Evidence` |
| **Base** | `AIRTABLE_BASE_ID_ALT` (per schema doc) |
| **Purpose** | Future governed hotel master — staging → verified promotion |
| **Governance fields** | `Can Use In Product`, `Can Show To Users`, `Can Use For Scoring`, `Internal Only?`, source-type columns per field |
| **Read/write** | **Documentation only** for many fields; promotion requires `--approved-by`; never write to legacy `Hotel Census` from ingest |
| **Source docs** | [verified-independent-hotel-census-schema.md](../verified-independent-hotel-census-schema.md) |
| **Status** | Proposal / phased — **Needs Verification** for live Airtable column parity |

---

### Demand Anchors

| | |
|--|--|
| **Table** | `Demand Anchors` (`AIRTABLE_TABLE_DEMAND_ANCHORS`) |
| **Base** | `AIRTABLE_BASE_ID_ALT` |
| **Purpose** | Radar map demand points (attractions, venues, etc.) |
| **Key fields** | `Demand Anchor Name`, `Radar Category`, `Point Type`, `Linked Market`, `Submarket`, geo fields, `Source`, `Source URL / Reference`, `Data Confidence`, `Last Verified`, `Visibility` |
| **Read/write** | `lib/demand-anchors/airtable-demand-anchors-fields.js`; ensure script: `scripts/ensure-demand-anchors-schema.mjs` |
| **Status** | Verified via field map |

---

### Travel Infrastructure Data

| | |
|--|--|
| **Table** | `Travel Infrastructure Data` (legacy alias: `Travel Infrastructure data`) |
| **Base** | `AIRTABLE_BASE_ID_ALT` |
| **Purpose** | Airports, ports, roads — radar map infrastructure layer |
| **Key fields** | Legacy: `Name`, `Type`, `City`, `Country`, `Region`, `Latitude`, `Longitude`; extensions: `Radar Category`, `Point Type`, `Linked Market`, `Submarket`, `Data Confidence`, `Last Verified`, `Visibility` |
| **Read/write** | `lib/travel-infrastructure/airtable-travel-infrastructure-fields.js`; `scripts/ensure-travel-infrastructure-schema.mjs` |
| **APIs** | `GET /api/travel-infrastructure`, `GET /api/radar-map-points/travel-infrastructure` |
| **Status** | Verified via field map |

---

### Market Demand Intelligence

| | |
|--|--|
| **Tables** | `Markets`, `Demand Centers`, `Demand Categories`, `Nearby Hotel Supply`, `Market Demand Snapshots` |
| **Base** | `AIRTABLE_BASE_ID_ALT` |
| **Purpose** | Market-level demand intelligence linked to deals via `Deal Record ID` |
| **Cross-base link** | `Deal Record ID` on platform tables; optional `Linked Market Record ID` on MVP Deals |
| **Read/write** | `lib/market-demand/airtable-market-demand-fields.js`; `scripts/ensure-market-demand-schema.mjs` |
| **Status** | Verified for table names; field detail in code map |

---

### GTM Owner Targets (internal)

| | |
|--|--|
| **Tables** | `Owner Targets` / `GTM Owner Targets`, `Properties`, `GTM Import Batches`, `Contacts` |
| **Base** | `AIRTABLE_GTM_BASE_ID` |
| **Purpose** | CoStar-sourced internal owner rollup — **never product-facing** |
| **Key fields** | Outreach Status, Dealality Pitch Status, Priority Tier, portfolio rollups — `lib/gtm-owner-target/field-map.js` |
| **Read/write** | Import: `scripts/import-gtm-owner-target-costar.mjs` (refuses product base IDs) |
| **Source docs** | [gtm-owner-target-list.md](../gtm-owner-target-list.md) |
| **Status** | Verified |

---

### Pilot Target List (internal)

| | |
|--|--|
| **Table** | `Pilot Target List` (`tblgsKWuI25MWohAP`) |
| **Base** | `AIRTABLE_GTM_BASE_ID` |
| **Purpose** | Curated founder pilot outreach contacts (owners, advisors, operators) |
| **Key fields** | `Name`, `Company`, `Pilot Region`, `Outreach Status`, `Email Draft`, `Final Approved Email`, `Ready for Mail Merge`, `Do Not Contact` — full map in `lib/gtm-owner-target/pilot-target-list-field-map.js` |
| **Protected writes** | Draft-fill script never writes `Final Approved Email`, `Ready for Mail Merge`, or upgrades `Outreach Status` to Approved/Sent |
| **Source docs** | [gtm-owner-target-list.md](../gtm-owner-target-list.md) (Pilot Target List section) |
| **Status** | Verified |

---

### Founder Project Plan / Master To-Do

| | |
|--|--|
| **Table** | `Founder Project Plan` (`tblpCg0QZ0kIPXihE`) |
| **Base** | `AIRTABLE_GTM_BASE_ID` |
| **Purpose** | Founder/GTM operational tasks (includes ChatGPT Master To-Do rows) |
| **Key fields** | `Task`, `Status` (use **`Completed`**, not `Done`), `Workstream`, `Phase`, `Assigned To`, `Source` (recommended) |
| **Read/write** | `lib/dealality-master-todo/`; upsert with `--dry-run` first |
| **Source docs** | [dealality-master-todo.md](../dealality-master-todo.md) |
| **Status** | Verified |

---

### Other product tables (brief)

| Table | Purpose | Env / code reference | Live audit (2026-07) |
|-------|---------|----------------------|----------------------|
| `Deal Activity Log` | Deal audit trail | `AIRTABLE_TABLE_DEAL_ACTIVITY_LOG` | **Confirmed** — see [airtable-deals-fields.md](./airtable-deals-fields.md) |
| `Deal Room Documents` | NDA / deal room files | `AIRTABLE_TABLE_DEAL_ROOM_DOCUMENTS` | **Confirmed** — 8 columns documented from audit |
| `Proposal Submissions` | Proposal history snapshots | `AIRTABLE_TABLE_PROPOSAL_SUBMISSIONS` | **Not in audited base** — optional; keep in schema audit |
| `MarketAlerts` / `UserAlertStatus` | Market alerts RSS | `api/lib/market-alerts-rss-airtable.js` |
| `Brand Explorer Favorites` / `Operator Explorer Favorites` | Per-user saved items | `ensure-*-favorites-table.mjs` |
| `Capital Setup - Deal Capital Provider List` | Capital explorer favorites | `lib/capital-setup/` |
| `3rd Party Operator - *` | Legacy operator tables | `lib/third-party-operator-airtable-fields-used.js` |

---

## Protected / Never-Write Fields

### Users table — signup / Memberstack sync

From `lib/airtable-users-protected-patch.js` (`USERS_PROTECTED_NEVER_WRITE`):

- `Workspace Access` (legacy on Users — **set on Company Profile only**)
- `Company Type`, `Company Type Tags`
- `Third-Party Management Availability`, `Operating Model`
- `Region Access`, `Deal Access`
- `Permission Level`
- Role flags: `Demo`, `Admin`, `Owner`, `Operator`, `Brand`

**Also:** never store `mem_sb_` Test Mode Memberstack IDs on production Users rows (`.env.example`, runbook).

### Company Profile — admin provisioning only

- `Workspace Access`, `Company Type`, `Region Access`, `Deal Access` — not from signup/webhook

### Hotel Census — directory enrichment

From `lib/hotel-census/brand-directory-enrichment-contract.js`:

- `name`, `Affiliation`, `Parent Company` — skip overwrite when values exist (unless explicit force)

### Pilot Target List — draft automation

From `lib/gtm-owner-target/pilot-target-list-draft-fill.js`:

- Never auto-write: `Final Approved Email`, `Ready for Mail Merge`
- Never auto-set `Outreach Status` to `Approved` or `Sent`

### Partner Intelligence

- Do not write Operator Alignment Snapshot scores into Extracted Facts or Published Explorer Fields ([partner-extracted-facts-airtable-fields.md](../partner-extracted-facts-airtable-fields.md))

### General rule

Any field marked **Company Validated** or human-approved publish state — AI may flag stale/conflict but must not auto-overwrite ([INTELLIGENCE_GOVERNANCE.md](../data-intelligence/INTELLIGENCE_GOVERNANCE.md)).

---

## AI-Generated / Platform-Derived Fields

| Area | Fields / outputs | Notes |
|------|------------------|-------|
| **Deal Readiness Snapshot** | `Deal Readiness Score`, `Deal Readiness Stage`, optional summary/count/last-reviewed on `Deals` | Platform-derived from deal inputs; `api/schemas/deal-setup-fields.js` |
| **Brand Alignment Snapshot** | API response (may persist rationale fields — Needs Verification per base) | `api/brand-alignment-snapshot.js`, `lib/brand-alignment-rationale.js` |
| **Operator Alignment Snapshot** | Profile-mode API; ODR: `Alignment Score`, `Alignment Band`, `Data Confidence` | User-facing: **Operator Alignment Snapshot** — not "Operator Match" |
| **Brand Explorer Presentation** | Section slot content in `Brand Setup - Brand Explorer Presentation` | Often fixture- or factory-patched; label AI-Assisted / Source-Informed in UI |
| **Operator Explorer JSON** | `*_json` fields on Setup child tables | Empty → module DEFAULTS until operator saves |
| **Partner Intelligence** | `Partner Intelligence - Extracted Facts` (staging), optional LLM extraction | Not live in Explorer until Published workflow |
| **Market alerts RSS** | Enriched summaries on `MarketAlerts` | Optional `MARKET_ALERTS_RSS_ENRICH_SUMMARIES` |
| **Pilot outreach drafts** | `Email Draft`, `LinkedIn DM Draft`, etc. on Pilot Target List | Human must approve before send/export |

---

## Validation / Governance Fields

### Documented in Partner Intelligence (proposed)

| Concept | Example Airtable names |
|---------|------------------------|
| Validation / trust | `Verified Source?`, `Source Quality`, `Status`, `Human Review Status`, `Publish Status` |
| Confidence | `Confidence Level`, `Confidence Score`, `Overall Source Confidence`, `Data Confidence` |
| Source metadata | `Source Type`, `Source Date`, `Capture Date`, `Region` |
| Review | `Last Reviewed`, `Last Reviewed Date`, `Reviewed By`, `Reviewed At` |
| Gaps / staleness | `Data Gap?`, `Stale?`, `Follow-up Question` |
| Usage | `Public Visibility`, `Approved for Explorer Use?`, `Internal Only` (verified census proposal) |

### Recommended / not yet confirmed globally

These concepts from [INTELLIGENCE_GOVERNANCE.md](../data-intelligence/INTELLIGENCE_GOVERNANCE.md) and [BRAND_PROFILE_DATA_MODEL.md](../data-intelligence/BRAND_PROFILE_DATA_MODEL.md) are **not** uniformly implemented as Airtable columns across Brand/Operator Setup yet:

- `Validation Status`, `Usage Permission`, `Company Validated`, `Company Validation Date`
- `Evidence Notes`, `Missing Data Flags` (as standard column names)
- `Source Region` as a dedicated field (Partner Intelligence uses `Region`)

Use partner-intelligence and verified-census schemas where present; elsewhere treat as **target governance shape** until ensure-scripts add columns.

**Rollout plan:** [brand-operator-validation-fields-plan.md](../data-intelligence/brand-operator-validation-fields-plan.md) — P1 profile governance on four Setup root tables; Partner Intelligence remains source-level SSOT.

**Live audit:** `npm run audit-brand-operator-validation-fields` → `reports/brand-operator-validation-schema-diff.md`

**P1 setup (dry-run default):** `npm run setup-brand-validation-fields -- --dry-run`, `npm run setup-operator-validation-fields -- --dry-run`

**P1 applied:** Profile governance columns live on four Setup root tables (see [brand-operator-validation-fields-plan.md](../data-intelligence/brand-operator-validation-fields-plan.md)).

**Read path + trust labels:** [governance-read-path-trust-label-plan.md](../data-intelligence/governance-read-path-trust-label-plan.md) — Phase 1 API read path live (`lib/profile-governance/normalize-profile-governance.js`); Explorer header trust chips Phase 3; E2E pilot applied 2026-07-06 (`npm run pilot-profile-governance-values`).

**Operator Master confidence alias:** Profile `governance.confidenceLevel` reads from **`Data Confidence Level`** on `Operator Setup - Master` (not `Confidence Level`). Pilot script maps this alias on write.

**PI → profile governance publish:** [partner-intelligence-profile-governance-publish-plan.md](../data-intelligence/partner-intelligence-profile-governance-publish-plan.md) — `npm run publish-partner-intelligence-profile-governance` (dry-run default; `--apply` for writes). Reports: `reports/partner-intelligence-profile-governance-publish.{md,json}`

**PI publish readiness audit (read-only):** `npm run audit-partner-intelligence-publish-readiness` → `reports/partner-intelligence-publish-readiness.{md,json}`

### Operator Setup (recommended / partial)

From [operator-alignment-recommended-airtable-fields.md](../operator-alignment-recommended-airtable-fields.md) — **proposed until Phase 5B approved**:

- **`Data Confidence Level`** on `Operator Setup - Master` — live column used as **profile governance confidence alias** (P1 `Confidence Level` not on this table)
- `Profile Last Reviewed` / `Last Reviewed Date` on `Operator Setup - Master`

---

## Deprecated / Legacy / Caution Fields

| Pattern | Guidance |
|---------|----------|
| **Operator Fit Assessment / Operator Match** | User-facing: **Operator Alignment Snapshot** only |
| **Brand Match** | User-facing: **Brand Alignment** / **Brand Alignment Snapshot** |
| **Validated** | Do not use unless validation level supports it ([NAMING_AND_COPY_GUIDE.md](../ai-build-system/NAMING_AND_COPY_GUIDE.md)) |
| **STR Market / Submarket** on census | Legacy reference — product geography = Dealality `Market` + corridor `Submarket` |
| **`User_ID`-only deal access** | Legacy — new pilots require `Deals → Company Profile` |
| **`Workspace Access` on Users** | Does not exist on current base — permissions on Company Profile only |
| **`3rd Party Operator - *` tables** | Legacy path; new-base `Operator Setup - *` is Explorer SSOT when writer enabled |
| **`Done` status** (Master To-Do) | Use **`Completed`** in Airtable |
| **Franchise affiliation typo** | Many bases use `agreeement` column — see `AIRTABLE_DEALS_FRANCHISE_AFFILIATION_FIELD` in deal-setup-fields |
| **CoStar / GTM fields** | Never copy into product census or public APIs |

---

## Open Questions / Needs Verification

1. **Deals field detail** — see [airtable-deals-fields.md](./airtable-deals-fields.md); run `npm run audit-airtable-deals-schema` for live parity (`reports/airtable-deals-schema-diff.md`). **Closure plan:** [deals-schema-finalization-plan.md](./deals-schema-finalization-plan.md).
2. **Brand/Operator governance fields** — run `npm run audit-brand-operator-validation-fields` → `reports/brand-operator-validation-schema-diff.md`; plan: [brand-operator-validation-fields-plan.md](../data-intelligence/brand-operator-validation-fields-plan.md).
3. **Brand Alignment Snapshot persistence** — which rationale fields are stored on Airtable vs computed-only?
4. **Partner Intelligence tables** — proposed; live base may not have all tables/fields yet.
5. **Operator Alignment Phase 5B** — P1 fields live + exposed in Deal Setup; `Brand Affiliation Path` (P2) still optional.
6. **Global governance columns** on Brand/Operator Setup — conceptual in data-intelligence docs; not base-wide yet.
7. **Company Profile** — full column list not in a dedicated `*-airtable-fields.md` (runbook + pilot registry only).
8. **Users table** — full column list scattered across intake/signup maps.
9. **Verified Independent Hotel Census** — proposal vs live columns; phased ensure scripts.
9. **Root-level `docs/`** — ~130 files; future migration to `platform-reference/` subfolders ([docs/README.md](../README.md)).
10. **Two operator writer paths** — which fields sync legacy vs new-base under shadow write?

---

## Links To Source Docs

### Schema markdown (`docs/`)

| Domain | Path |
|--------|------|
| Operator Explorer tabs | `docs/operator-*-explorer-airtable-fields.md` |
| Operator DNA JSON | [operator-dna-explorer-json-fields.md](../operator-dna-explorer-json-fields.md) |
| Operator alignment (proposed fields) | [operator-alignment-recommended-airtable-fields.md](../operator-alignment-recommended-airtable-fields.md) |
| Partner intelligence | `docs/partner-*-airtable-fields.md` |
| Hotel census (verified proposal) | [verified-independent-hotel-census-schema.md](../verified-independent-hotel-census-schema.md) |
| GTM / pilot | [gtm-owner-target-list.md](../gtm-owner-target-list.md) |
| Master to-do | [dealality-master-todo.md](../dealality-master-todo.md) |

### Code field maps (authoritative for writes)

| Module | Path |
|--------|------|
| Deal setup / My Deals | [airtable-deals-fields.md](./airtable-deals-fields.md), `api/schemas/deal-setup-fields.js` |
| Operator deal requests | `api/operator-deal-requests-fields.js` |
| Brand deal requests | `api/schemas/brand-deal-request-fields.js` |
| Hotel census | `lib/hotel-census/fields.js` |
| Demand anchors | `lib/demand-anchors/airtable-demand-anchors-fields.js` |
| Travel infrastructure | `lib/travel-infrastructure/airtable-travel-infrastructure-fields.js` |
| Market demand | `lib/market-demand/airtable-market-demand-fields.js` |
| GTM / pilot target list | `lib/gtm-owner-target/pilot-target-list-field-map.js`, `field-map.js` |
| Users protected patch | `lib/airtable-users-protected-patch.js` |
| Pilot provisioning | `lib/pilot-provisioning/pilot-field-registry.js` |
| Partner intelligence | `lib/partner-intelligence/airtable-*.js` |

### Governance & build OS

- [INTELLIGENCE_GOVERNANCE.md](../data-intelligence/INTELLIGENCE_GOVERNANCE.md)
- [brand-operator-validation-fields-plan.md](../data-intelligence/brand-operator-validation-fields-plan.md)
- [governance-read-path-trust-label-plan.md](../data-intelligence/governance-read-path-trust-label-plan.md)
- `lib/profile-governance/normalize-profile-governance.js` — Brand/Operator detail `governance` object
- [DATA_VALIDATION_PROTOCOL.md](../data-intelligence/DATA_VALIDATION_PROTOCOL.md)
- [AGENTS.md](../../AGENTS.md)
- [.env.example](../../.env.example)

---

## Related

- [ARCHITECTURE_MAP.md](./ARCHITECTURE_MAP.md)
- [TESTING_PROTOCOL.md](./TESTING_PROTOCOL.md)
- [docs/README.md](../README.md)
