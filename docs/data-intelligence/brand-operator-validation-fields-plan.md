# Brand + Operator Validation Fields Plan

**Date:** 2026-07-06  
**Status:** Planning only — no Airtable changes, no write paths, no UI trust labels in this phase.

> **Authority:** [INTELLIGENCE_GOVERNANCE.md](./INTELLIGENCE_GOVERNANCE.md), [DATA_VALIDATION_PROTOCOL.md](./DATA_VALIDATION_PROTOCOL.md), [BRAND_PROFILE_DATA_MODEL.md](./BRAND_PROFILE_DATA_MODEL.md), [OPERATOR_PROFILE_DATA_MODEL.md](./OPERATOR_PROFILE_DATA_MODEL.md), [DATA_DICTIONARY.md](../platform-reference/DATA_DICTIONARY.md)

---

## Purpose

This plan defines how Dealality should add **source, validation, confidence, review, and usage-permission** fields to Brand and Operator profile/content tables so Explorer pages, alignment snapshots, and future Opportunity Intelligence can rely on **trusted platform intelligence** — not unlabeled AI output or stale third-party copy.

It answers:

- Which tables exist today and which already carry governance metadata.
- Which **recommended** governance fields are missing vs. proposed in Partner Intelligence.
- Where governance should live (profile-level first; field-level later).
- What setup scripts, UI trust labels, and scoring rules should follow **after** a read-only schema audit.

**Out of scope for this document:** implementing Airtable columns, write paths, Explorer UI labels, or scoring weight changes.

---

## Current State Summary

### Brand data/content

| Area | What exists | Governance today |
|------|-------------|----------------|
| **Brand Setup child tables** (Basics, Footprint, Project Fit, etc.) | Intake + Explorer source data via `api/brand-library.js` | **No standard Validation Status / Usage Permission columns** on Setup tables per code maps. Content is often fixture- or factory-patched. |
| **Brand Setup - Brand Explorer Presentation** | Slot-based presentation rows (`slotKey`, `title`, `body`, `sort`) | **No governance columns documented** — [brand-explorer-presentation-slots.md](../brand-explorer-presentation-slots.md). Treat presentation copy as **AI-Assisted / Source-Informed until reviewed**. |
| **Partner Intelligence** (proposed) | Source Library → Extracted Facts → Published Explorer Fields | **Richest governance design in repo** — proposed tables with review/status/confidence fields; not uniformly live until `ensure-partner-intelligence-tables.mjs --apply`. |
| **Brand Alias Mapping** | Census affiliation → canonical brand (`AIRTABLE_BASE_ID_ALT`) | `Match Confidence`, `Notes`, `Active` — **mapping trust**, not profile validation. |
| **Hotel Census** | Footprint rollups for Brand Explorer | Optional `Data Confidence`, `Include in Brand Explorer` (governance script exists). |
| **Brand Alignment Snapshot** | API response (`api/brand-alignment-snapshot.js`) | Platform-derived interpretation; **no persistent BAS narrative columns on Deals** confirmed. |
| **Deal Brand Cache** | Cached brand scores per deal | `Last Computed At` — platform cache, not company validation. |

### Operator data/content

| Area | What exists | Governance today |
|------|-------------|----------------|
| **Operator Setup - Master** | Identity, `submission_status`, Explorer list gate | **Partial live:** `Data Confidence Level`, `Source Type`, `Last Updated Date` (confirmed in operator-alignment audits). **Recommended / partial:** `Profile Last Reviewed`. |
| **Operator Setup child tables** (Profile, Platform, Commercial, Governance, Engagement, Materials, Leadership) | 13-tab intake + Explorer gold profiles | **22 `*_json` explorer fields** — content without per-block governance. Visibility gates: `submission_status`, `readyForInvestorPublication`, `displayLeadershipOnExplorer`. |
| **Legacy `3rd Party Operator - *` tables** | Still referenced in code | **Needs Verification** for live parity; no unified governance model. |
| **Operator Alignment Snapshot** | API-computed profile/deal modes | ODR stores `Alignment Score`, `Alignment Band`, `Data Confidence` — **deal-request interpretation**, not operator profile validation. |
| **Partner Intelligence** | Same publish pipeline as brands | Proposed staging/publish with Human Review Status, Confidence Level, Publish Status. |

### Company Profile data

| Area | What exists | Governance today |
|------|-------------|----------------|
| **`Company Profile`** | Workspace permissions SSOT (`Workspace Access`, `Company Type`, `Region Access`, `Deal Access`) | **No validation/source fields documented.** Admin provisioning only — [DATA_DICTIONARY.md](../platform-reference/DATA_DICTIONARY.md). **Needs Verification** for full column inventory. |
| **Users** | Memberstack mirror, link to Company Profile | Protected permission fields — not intelligence governance. |

### Brand Alias Mapping

- Purpose: resolve census `Affiliation` strings → canonical brand/parent.
- Existing trust fields: `Canonical Brand Name`, `Alias / Source Brand Name`, `Parent Company`, `Active`, **`Match Confidence`**, `Notes`.
- **Not** a substitute for brand profile Validation Status — affects rollups/Scout, not Explorer claims.

### Snapshot / cache tables (relevant)

| Artifact | Storage | Governance note |
|----------|---------|-----------------|
| Brand Alignment Snapshot | API (mostly) | Label outputs as interpretation; do not imply company validation. |
| Operator Alignment Snapshot | API + ODR scores | `Data Confidence` on ODR is scoring metadata, not operator profile validation. |
| Deal Brand Cache | Airtable table | Recomputable platform cache. |
| Partner Intelligence Published rows | Proposed overlay | Intended future SSOT for approved Explorer overlay values. |

---

## Tables Reviewed

| Table | Purpose | Source docs/code | Known content/profile fields | Existing validation/source fields | Status |
|-------|---------|------------------|------------------------------|--------------------------------|--------|
| `Brand Setup - Brand Basics` | Brand identity, positioning | `api/brand-library.js`, Brand Setup UI | `Brand Name`, parent, chain scale, positioning, logos | None in central maps | **Needs Verification** (live columns) |
| `Brand Setup - Brand Footprint` | Geography, markets | brand-library | Markets, footprint narrative | None in maps | **Needs Verification** |
| `Brand Setup - Project Fit` | Asset fit | brand-library | New build, conversion, resort, etc. | None in maps | **Needs Verification** |
| `Brand Setup - Portfolio & Performance` | Proof points | brand-library | Performance metrics | None in maps | **Needs Verification** |
| `Brand Setup - Brand Standards` | Standards themes | brand-library | Design, F&B, room reqs | None in maps | **Needs Verification** |
| `Brand Setup - Fee Structure` / `Deal Terms` / `Operational Support` / `Legal Terms` / `Loyalty & Commercial` / `Sustainability & ESG` | Economics & terms | brand-library | Tab-specific columns | None in maps | **Needs Verification** |
| **`Brand Setup - Brand Explorer Presentation`** | Explorer slot copy | [brand-explorer-presentation-slots.md](../brand-explorer-presentation-slots.md), fixtures | `slotKey`, `title`, `body`, `sort`, brand link | **None documented** | Verified table name; **governance missing** |
| **`Partner Intelligence - Source Library`** | Source registry | [partner-source-library-airtable-fields.md](../partner-source-library-airtable-fields.md) | URLs, files, metadata | `Source Type`, `Source Quality`, `Verified Source?`, `Status`, `Last Reviewed`, `Approved for Extraction?`, `Approved for Explorer Use?` | **Proposed** |
| **`Partner Intelligence - Extracted Facts`** | Extraction staging | [partner-extracted-facts-airtable-fields.md](../partner-extracted-facts-airtable-fields.md) | Field-level extracted values | `Confidence Level`, `Human Review Status`, `Data Gap?`, `Evidence Text`, `Reviewed At` | **Proposed** |
| **`Partner Intelligence - Published Explorer Fields`** | Approved Explorer overlay | [partner-explorer-published-fields-airtable-fields.md](../partner-explorer-published-fields-airtable-fields.md) | Per-field approved values | `Publish Status`, `Overall Source Confidence`, `Stale?`, `Last Reviewed Date` | **Proposed** |
| **`Partner Intelligence - Helena Outreach Intake`** | Outreach log | [partner-helena-intake-airtable-fields.md](../partner-helena-intake-airtable-fields.md) | Follow-up requests | Status fields per doc | **Proposed** |
| **`Brand Alias Mapping`** | Affiliation resolution | `lib/hotel-census/fields.js` (`ALIAS_FIELDS`) | Canonical/alias/parent | `Match Confidence`, `Notes`, `Active` | Verified (mapping layer) |
| **`Hotel Census`** | Directory / rollups | `lib/hotel-census/fields.js` | Property attributes | `Data Confidence`, `Include in Brand Explorer` | Partial (ALT base) |
| **`Operator Setup - Master`** | Operator identity | `api/lib/operator-setup-new-base-writer.js`, explorer list | `company_name`, `submission_status` | **`Data Confidence Level`**, **`Source Type`**, **`Last Updated Date`** (live per audits); **`Profile Last Reviewed`** (recommended) | **Partial** |
| **`Operator Setup - Profile & Positioning`** | Narrative, publication gate | operator-*-explorer docs | `readyForInvestorPublication`, brand links | No standard validation columns in maps | **Needs Verification** |
| **`Operator Setup - Platform & Markets`** | Footprint, markets | operator platform docs | `activeCountries`, `activeMarkets`, `specificMarkets` | Phase 5B fields partially live | **Partial** |
| **`Operator Setup - Commercial Fit & Terms`** | Deal fit prefs | commercial docs | `bf_*` multis | None in governance maps | **Needs Verification** |
| **`Operator Setup - Governance, Delivery & Diligence`** | Infra, risk, services | governance docs | `infra_*`, `risk_*` | None in governance maps | **Needs Verification** |
| **`Operator Setup - Engagement & Reporting`** | Owner interface | engagement docs | Reporting fields | None in governance maps | **Needs Verification** |
| **`Operator Setup - Explorer Materials`** | Materials slots | [operator-materials-explorer-airtable-fields.md](../operator-materials-explorer-airtable-fields.md) | Slot pattern like brand presentation | **None documented** | **Needs Verification** |
| **`Operator Setup - Leadership *`** | Leadership platform/team | leadership docs | Team bios | None in governance maps | **Needs Verification** |
| **Legacy `3rd Party Operator - *`** | Legacy intake | `lib/third-party-operator-airtable-fields-used.js` | Large field surface | Legacy `Explorer Profile JSON` | **Deprecated path — Needs Verification** |
| **`Company Profile`** | Permissions | `lib/pilot-provisioning/pilot-field-registry.js` | Workspace/region/deal access | **None documented** | **Needs Verification** |
| **`Deal Brand Cache`** | Cached BAS-related scores | `api/my-deals.js` | Score JSON | `Last Computed At` | Platform cache |
| **`Operator Deal Requests`** | Per-deal OAS | `api/operator-deal-requests-fields.js` | Alignment scores | `Data Confidence` | Deal-scoped interpretation |

---

## Existing Validation / Source Fields Found

### Partner Intelligence (proposed — richest set)

| Field | Table | Role |
|-------|-------|------|
| `Source Type`, `Source URL`, `Source File`, `Source Date`, `Capture Date` | Source Library | Provenance |
| `Verified Source?`, `Source Quality`, `Status` | Source Library | Trust + workflow |
| `Last Reviewed`, `Reviewed By` | Source Library | Review metadata |
| `Approved for Extraction?`, `Approved for Explorer Use?` | Source Library | Usage gates |
| `Confidence Level`, `Confidence Score`, `Human Review Status` | Extracted Facts | Field-level staging |
| `Evidence Text`, `Data Gap?`, `Reviewed At` | Extracted Facts | Evidence / gaps |
| `Publish Status`, `Overall Source Confidence`, `Stale?`, `Last Reviewed Date` | Published Explorer Fields | Live overlay governance |

### Operator Setup - Master (partial live)

| Field | Status | Notes |
|-------|--------|-------|
| `Data Confidence Level` | **Live** (per alignment audits) | **Profile governance confidence column on Operator Master** — P1 spec `Confidence Level` is not created on this table; use this column as the alias for `governance.confidenceLevel` (read path + `pilot-profile-governance-values.mjs`) |
| `Source Type` | **Live** | Admin/source classification |
| `Last Updated Date` | **Live** | Staleness signal |
| `Profile Last Reviewed` | **Recommended** in Phase 5B docs | **Needs Verification** on all bases |
| `submission_status` | **Live** | Workflow gate (`Active` for Explorer/OAS list) — not validation level |

### Brand / presentation (minimal)

| Field | Table | Notes |
|-------|-------|-------|
| *(none standard)* | Brand Setup tables | No shared validation columns in code maps |
| *(none documented)* | Brand Explorer Presentation | Slot rows lack governance metadata |
| `Match Confidence` | Brand Alias Mapping | Alias resolution only |
| `Data Confidence` | Hotel Census | Row-level QA for census, not brand Setup |

### Company Profile

- **No validation/source/confidence fields found** in documented maps. Permissions fields only.

### Snapshot / deal-scoped (not profile truth)

| Field | Table | Notes |
|-------|-------|-------|
| `Data Confidence` | Operator Deal Requests | OAS interpretation on a deal request |
| `Deal Readiness *` | Deals | Owner opportunity completeness — separate from brand/operator profile |
| Score fields | Deal Brand Cache | Platform-derived cache |

---

## Recommended Governance Fields

Core profile-level set aligned with [INTELLIGENCE_GOVERNANCE.md](./INTELLIGENCE_GOVERNANCE.md) and profile data models. **Recommended** until live audit confirms duplicates.

| Field | Purpose | Airtable type | Options (if select) | Required? | AI may write? | Visible externally? |
|-------|---------|---------------|---------------------|-----------|---------------|---------------------|
| **Validation Status** | Single profile/content trust level | Single select | See [Validation Status Options](#validation-status-options) | Recommended on Master + Presentation root | Suggest only; human sets Company Validated | Yes — as trust label |
| **Usage Permission** | What the row may power | Single select | See [Usage Permission Options](#usage-permission-options) | Recommended | Suggest only | Internal + derived UI behavior |
| **Source Type** | Document/channel class | Single select | FDD, Development Brochure, Company Website, Capability Deck, … | Optional | Yes (from extraction) | Sometimes (generic label) |
| **Source Date** | Document date | Date | — | Optional | Yes (from extraction) | Rarely |
| **Source Region** | Geographic scope of evidence | Single select or text | CALA, US, Global, … | Optional | Yes | Yes — region trust label |
| **Source URL / File Path** | Primary source locator | URL + attachment / long text | — | Optional when sourced | Yes (Source Library) | No direct URL in owner UI initially |
| **Last Reviewed Date** | Human review timestamp | Date | — | Recommended | No | Yes — “Last reviewed” label |
| **Refresh Due Date** | Staleness planning | Date | — | Optional | Suggest only | No |
| **Confidence Level** | Profile-level confidence | Single select | High, Medium, Low, Unknown | Recommended | Suggest only | Yes — cautious wording |
| **Evidence Notes** | Internal source summary | Long text | — | Optional | Draft only | No |
| **Missing Data Flags** | Known gaps | Long text or multi-select | Gap taxonomy TBD | Optional | Yes (gaps from extraction) | Yes — “data gaps” section |
| **Company Validated** | Direct company confirmation | Checkbox or single select | Yes / No | Optional until outreach | **Never auto-set true** | Yes — strongest trust label |
| **Company Validation Date** | When confirmed | Date | — | When Company Validated | **Never** | Yes — with Company Validated |
| **Reviewed By** | Reviewer | Collaborator / link | — | Optional | No | No |
| **External Display Status** | UI gating for trust labels | Single select | See below | Recommended | Human/admin | Controls UI only |
| **Internal Notes** | Reviewer commentary | Long text | — | Optional | Yes (draft) | **Never** |

---

## Validation Status Options

Use exactly these conceptual levels (map 1:1 to Airtable select options):

- **Company Validated**
- **Company Published**
- **Source-Informed**
- **Owner-Provided**
- **AI-Assisted**
- **Needs Review**
- **Stale / Refresh Needed**
- **Do Not Use**

**Default for new AI/factory content:** `AI-Assisted` or `Needs Review` — never `Company Validated` without explicit confirmation.

---

## Usage Permission Options

- **Internal Only**
- **Platform Display Allowed**
- **Scoring Allowed**
- **External Snapshot Allowed**
- **Company Validated** (implies do-not-overwrite protection)
- **Do Not Use**

---

## External Display Status Options

- **Show Trust Label**
- **Hide Trust Label**
- **Internal Only**
- **Needs Review**
- **Do Not Display**

Use on profile root or published overlay rows to decouple **data existence** from **owner-visible trust chrome**.

---

## Priority Implementation Order

### 1. Brand Explorer / Brand Library / Brand Setup source tables

**Why first:** Brand Explorer is customer-facing; presentation slots are already populated by factory/fixtures without governance. Partner Intelligence publish path is designed to overlay Brand Setup reads.

**First targets:**
- `Brand Setup - Brand Explorer Presentation` — add profile-level or per-slot governance minimum (`Validation Status`, `Last Reviewed Date`, `External Display Status`).
- `Brand Setup - Brand Basics` — root identity row governance (rollup/display anchor).
- **Partner Intelligence tables** — enable source → fact → publish pipeline (already specified).

### 2. Operator Setup Master / Operator Explorer source tables

**Why second:** OAS and Operator Explorer depend on Master + child tables; partial governance exists on Master (`Data Confidence Level`, `Source Type`).

**First targets:**
- `Operator Setup - Master` — complete profile-level governance (`Validation Status`, `Usage Permission`, `Profile Last Reviewed`, `Company Validated`).
- `Operator Setup - Explorer Materials` — mirror brand presentation slot governance.
- High-impact child tables: **Platform & Markets**, **Commercial Fit**, **Governance** (scoring-critical inputs).

### 3. Company Profile

**Why third:** Governs access, not public profile claims — but may need **`Company Validated`** for pilot onboarding attestations and to avoid conflating workspace permissions with intelligence trust.

**Approach:** Minimal fields only (`Company Validated`, `Company Validation Date`, `Internal Notes`) — **do not** duplicate full Explorer governance on Company Profile.

### 4. Brand Alias Mapping

**Why fourth:** Affects census/Scout trust, not Explorer narrative. Extend only if alias confidence must surface in product (low priority).

### 5. Snapshot output / cache tables

**Why last:** `Deal Brand Cache`, ODR scores are **platform-derived**. Add governance labels in API responses first; persist only if shareable snapshots require audit trail.

**Do not** write OAS/BAS scores into Partner Intelligence Facts/Published tables.

---

## Field-Level Confidence

**Phase 1 — profile-level only**

- Store `Confidence Level` + `Validation Status` on:
  - Brand Basics row (brand root)
  - Operator Setup - Master (operator root)
  - Partner Intelligence Published rows (per field overlay)

**Phase 2 — field-level confidence (later)**

Add only for high-impact fields:

| Brand examples | Operator examples |
|----------------|-------------------|
| CALA presence / regional relevance | CALA experience / active markets |
| Conversion requirements | Third-party management availability |
| Franchise economics claims | Conversion / pre-opening capability |
| Brand standards themes | F&B complexity capability |
| Footprint proof points | Management structures supported |

Use Partner Intelligence **Extracted Facts** `Confidence Level` as staging; roll up to Published `Overall Source Confidence`.

---

## Do-Not-Overwrite Rules

AI may: suggest updates, flag conflicts, create pending revisions, mark content stale, populate Extracted Facts with `Human Review Status = Pending`.

AI must **not** automatically overwrite:

| Category | Examples |
|----------|----------|
| Company-validated fields | `Company Validated`, `Company Validation Date` |
| Company-provided Setup fields | Operator Master identity, brand-submitted Setup tabs |
| Published overlay values | `Partner Intelligence - Published Explorer Fields` where `Publish Status = Published` |
| Protected permissions | Company Profile access fields, Users protected fields |
| Scoring-critical confirmed inputs | Phase 5B structured deal fields after owner save |
| Source-backed facts with approved evidence | Published rows linked to approved facts |

**Hierarchy:** See [INTELLIGENCE_GOVERNANCE.md](./INTELLIGENCE_GOVERNANCE.md) conflict resolution — company-validated regional data beats AI inference.

---

## UI Trust Labels

Recommended owner-facing labels (map from `Validation Status` + dates + region):

| Label | When to show |
|-------|----------------|
| **Company Validated** | `Company Validated` true + permission allows display |
| **Company Published** | Validation Status = Company Published |
| **Source-Informed** | Credible third-party / official materials |
| **AI-Assisted** | AI-Assisted or factory-generated, not yet reviewed |
| **Needs Review** | Internal QA flag; optional muted badge |
| **Last Reviewed: [Date]** | When `Last Reviewed Date` set |
| **Region: CALA-specific** / **Region: Global Reference** | From `Source Region` |

### Where labels appear (after fields exist)

| Surface | Placement |
|---------|-----------|
| Brand Explorer profile header | Validation Status + Last Reviewed |
| Operator Explorer profile header | Same |
| Brand/operator consideration cards | Compact trust chip |
| Snapshot footnotes | “Source notes” / confidence caveat |
| Admin/internal review view | Full governance fields + Evidence Notes |

**Rule:** Do not show trust labels until `External Display Status` allows it — default new content to **Hide Trust Label** or **Needs Review**.

---

## Scoring / Alignment Impact

| Validation Status | Brand Alignment Snapshot | Operator Alignment Snapshot | Opportunity Intelligence (future) |
|-------------------|--------------------------|----------------------------|-----------------------------------|
| Company Validated | May support scoring narrative | May support scoring | High trust inputs |
| Company Published | May support scoring | May support scoring | Medium-high |
| Source-Informed | May support scoring | May support scoring | Medium |
| Owner-Provided | Deal-scoped only | Deal-scoped only | Context for **this opportunity** only |
| AI-Assisted | Weak signals only; label clearly | Weak signals only | Not primary evidence |
| Needs Review | **Must not** drive scoring | **Must not** drive scoring | Blocked |
| Do Not Use | **Never** | **Never** | **Never** |
| Stale / Refresh Needed | Downgrade confidence / show caveat | Downgrade confidence | Refresh before high-stakes use |

**Usage Permission gates:**

- `Scoring Allowed` required for OAS/BAS factor inputs sourced from profile overlay.
- `External Snapshot Allowed` required for printable/shareable snapshot claims tied to profile facts.

**Current code:** OAS already uses `Data Confidence` on ODR and operator `Data Confidence Level` on Master — treat these as **separate** from profile `Validation Status` until unified in a later phase.

---

## Content Review Queue

### Recommendation

**Start without a separate queue** while profile volume is low and Partner Intelligence Facts table serves as staging.

**Add `Partner Intelligence - Content Review Queue` (or Airtable interface)** when:

- AI extraction runs across multiple brands/operators concurrently, or
- Company validation outreach begins, or
- More than one reviewer needs assignment/priority tracking.

### Suggested fields (later)

`Item Type`, `Company / Brand / Operator`, `Suggested Update`, `Source`, `Validation Level`, `Usage Permission`, `Risk Level`, `Priority`, `Reviewer`, `Review Status`, `Decision`, `Approved Fields`, `Rejected Fields`, `Notes`, `Next Action`

Until then, use **Extracted Facts** + `Human Review Status` + Published `Publish Status` as the workflow.

---

## Airtable Setup Plan

**Do not run these until** read-only audit reports are reviewed and founder approves column sets.

### `scripts/audit-brand-operator-validation-fields.mjs` (first)

- Read-only Meta API export for Brand Setup tables, Operator Setup tables, Company Profile, Brand Alias Mapping, Partner Intelligence tables (if live).
- Diff vs recommended governance field registry (similar to Deals audit).
- Output: `reports/brand-operator-validation-schema-live.json`, `reports/brand-operator-validation-schema-diff.md`.

### `scripts/setup-brand-validation-fields.mjs` (later)

- Idempotent ensure on **Brand Setup - Brand Basics** + **Brand Explorer Presentation** (+ optional Partner Intelligence if not using `ensure-partner-intelligence-tables.mjs`).
- Profile-level governance columns only in Phase 2.
- `--dry-run` default; `--apply` after review.
- Report: `reports/brand-validation-fields-setup.md`.

### `scripts/setup-operator-validation-fields.mjs` (later)

- Idempotent ensure on **Operator Setup - Master** (+ optional Materials table).
- Reuse existing option sets from `lib/operator-alignment-field-options.js` where aligned (`OAS_DATA_CONFIDENCE_OPTIONS`, `OAS_SOURCE_TYPE_OPTIONS`).
- Do not duplicate live `Data Confidence Level` / `Source Type` if already present.
- Report: `reports/operator-validation-fields-setup.md`.

### Existing related scripts (reference only)

| Script | Role |
|--------|------|
| `scripts/ensure-partner-intelligence-tables.mjs` | Full PI table create (proposed) |
| `scripts/ensure-hotel-census-governance-fields.mjs` | Census `Data Confidence` (ALT base) |
| `scripts/ensure-brand-alias-mapping-table.mjs` | Alias table structure |

---

## Implementation Phases

### Phase 1 — Audit Existing Brand/Operator Schema

- Build `audit-brand-operator-validation-fields.mjs`.
- Document live vs. recommended gaps per table.
- Confirm which Operator Master governance fields are already live on `appvtnDurnMSjINP6`.

### Phase 2 — Add Profile-Level Governance Fields

- Apply setup scripts (brand root + operator master + presentation/materials anchors).
- Wire read paths only; no AI writes.

### Phase 3 — Add UI Trust Labels

- Brand Explorer / Operator Explorer headers and snapshot footnotes.
- Respect `External Display Status`.

### Phase 4 — Add Content Extraction Workflow

- Enable Partner Intelligence extraction → facts → publish overlay (`PARTNER_INTELLIGENCE_PUBLISH_OVERLAY=1` when ready).
- Use [CONTENT_EXTRACTION_TEMPLATE.md](./CONTENT_EXTRACTION_TEMPLATE.md).

### Phase 5 — Add Company Validation Workflow

- Outreach + Helena intake → company confirmation → set `Company Validated` manually.
- Never auto-promote from AI.

### Phase 6 — Add Field-Level Confidence

- Only after profile-level governance is stable and PI publish path is trusted.
- High-impact fields first (see [Field-Level Confidence](#field-level-confidence)).

---

## Risks / Open Questions

| Risk / question | Notes |
|-----------------|-------|
| **Live column parity unknown** | Brand Setup tables lack a Deals-style audit; many fields **Needs Verification**. |
| **Governance granularity** | Profile-level vs. slot-level (Presentation/Materials) vs. per Published row — may need all three. |
| **Company Profile inheritance** | Should deal access company inherit brand validation from linked brand operator relationships? **Not decided.** |
| **Trust labels timing** | Showing labels before content review may expose `AI-Assisted` everywhere — default to hidden/minimal until review cadence exists. |
| **Snapshot scoring thresholds** | Should OAS/BAS require `Scoring Allowed` on underlying profile facts? **Deferred.** |
| **Company-provided update review** | Operator/brand self-serve edits may need `Needs Review` before `Platform Display Allowed`. |
| **Duplicate governance models** | Partner Intelligence vs. columns on Setup tables — avoid two sources of truth; prefer **Published overlay** for owner-visible claims, **Setup** for company-provided intake. |
| **Legacy 3rd Party Operator tables** | Code still references; governance plan should target **Operator Setup - *** only unless legacy is retired. |
| **Factory-generated brand presentation** | Large fixture corpus may default to `AI-Assisted` / `Needs Review` bulk until reviewed. |

---

## Live Schema Audit

Before creating governance columns, run the read-only Brand/Operator validation schema audit (mirrors the Deals audit):

```bash
npm run audit-brand-operator-validation-fields
```

**Requires:** `AIRTABLE_API_KEY` with `schema.bases:read` on `AIRTABLE_BASE_ID`; optional `AIRTABLE_BASE_ID_ALT` for Brand Alias Mapping + Hotel Census.

**Outputs:**

- `reports/brand-operator-validation-schema-live.json` — live Meta API field inventory
- `reports/brand-operator-validation-schema-diff.md` — governance gap report

**Registry:** `lib/brand-operator-validation-audit/expected-brand-operator-validation-registry.js`

The audit does **not** read records or modify schema. Use the diff report to confirm which governance fields already exist, which are missing, and which tables need setup scripts later.

---

## P1 Profile Governance (Approved)

**Status:** P1 columns **applied** on primary base (2026-07-06). Setup scripts remain idempotent (`--dry-run` default).

### P1 tables (profile-level trust only)

| Brand | Operator |
|-------|----------|
| `Brand Setup - Brand Basics` | `Operator Setup - Master` |
| `Brand Setup - Brand Explorer Presentation` | `Operator Setup - Explorer Materials` |

### P1 fields (14 columns per P1 table)

`Validation Status`, `Usage Permission`, `Source Type`, `Source Region`, `Last Reviewed Date`, `Refresh Due Date`, `Confidence Level`, `Evidence Notes`, `Missing Data Flags`, `Company Validated`, `Company Validation Date`, `Reviewed By`, `External Display Status`, `Internal Notes`

**Intentionally excluded from Setup roots:** `Source URL / File Path`, `Source Date` — Partner Intelligence remains source-level SSOT.

**Operator Master confidence alias:** P1 field list includes `Confidence Level`, but live **`Operator Setup - Master`** uses existing **`Data Confidence Level`** as the profile-governance confidence column (`Confidence Level` was skipped at setup — see `P1_GOVERNANCE_FIELD_ALIASES` in `p1-profile-governance-field-specs.js`). `normalize-profile-governance.js` reads via this alias; `npm run pilot-profile-governance-values` writes pilot confidence to **`Data Confidence Level`** automatically.

**E2E pilot (2026-07-06):** `pilot-profile-governance-values` applied approved QA values to Best Western Plus (Brand Basics) and Viento Sur Gestión Hotelera (Operator Master); Explorer trust chips validated end-to-end.

**PI → profile governance publish (planned):** [partner-intelligence-profile-governance-publish-plan.md](./partner-intelligence-profile-governance-publish-plan.md) — defines how reviewed Partner Intelligence updates Setup profile governance fields (readiness audit + publish script later; separate from field-level `publish-overlay.js`).

### Setup scripts (schema only)

```bash
npm run setup-brand-validation-fields -- --dry-run
npm run setup-operator-validation-fields -- --dry-run
```

| Script | Reports |
|--------|---------|
| `scripts/setup-brand-validation-fields.mjs` | `reports/brand-validation-fields-setup.{json,md}` |
| `scripts/setup-operator-validation-fields.mjs` | `reports/operator-validation-fields-setup.{json,md}` |

**Field specs:** `lib/brand-operator-validation-audit/p1-profile-governance-field-specs.js`

**Layers:**

- **Partner Intelligence** — source capture, extraction, review, publish (already schema-ready).
- **Setup root tables** — lean profile-level trust/status for Explorer and snapshots.

---

## Recommended Next Step

**Founder review dry-run reports**, then apply P1 schema if approved:

```bash
npm run setup-brand-validation-fields -- --dry-run
npm run setup-operator-validation-fields -- --dry-run
# After approval only:
# npm run setup-brand-validation-fields -- --apply
# npm run setup-operator-validation-fields -- --apply
npm run audit-brand-operator-validation-fields
```

After `--apply`:

1. Re-run audit to confirm P1 fields live on four tables.
2. **Read path + trust labels** — [governance-read-path-trust-label-plan.md](./governance-read-path-trust-label-plan.md) (Phase 1: API helpers only).
3. Add Explorer trust labels (Phase 3 in that plan).

---

## Related

- [INTELLIGENCE_GOVERNANCE.md](./INTELLIGENCE_GOVERNANCE.md)
- [DATA_VALIDATION_PROTOCOL.md](./DATA_VALIDATION_PROTOCOL.md)
- [BRAND_PROFILE_DATA_MODEL.md](./BRAND_PROFILE_DATA_MODEL.md)
- [OPERATOR_PROFILE_DATA_MODEL.md](./OPERATOR_PROFILE_DATA_MODEL.md)
- [partner-intelligence-repository-mvp-plan.md](../partner-intelligence-repository-mvp-plan.md)
- [deals-schema-finalization-plan.md](../platform-reference/deals-schema-finalization-plan.md)
- [DATA_DICTIONARY.md](../platform-reference/DATA_DICTIONARY.md)
