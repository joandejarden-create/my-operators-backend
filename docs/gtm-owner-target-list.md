# GTM Owner Target List (internal-only Airtable base)

**Purpose:** Internal Dealality sales/GTM list of hotel **asset owners** to pitch the platform. Sourced from licensed **CoStar** exports. **Never** published in Dealality product UI, APIs, Hotel Census, or Scout.

**Related — Decision Radar:** Project/decision-level acquisition opportunities live in the same GTM base as tables `Decision Opportunities` + `Decision Opportunity Evidence`. They represent **live owner decisions**, not owner accounts. SoT: [`docs/gtm-decision-radar.md`](./gtm-decision-radar.md). Ensure: `npm run ensure:gtm-decision-opportunities-schema`. Do not confuse with product Target OS (`api/target-list.js`).

**Related — Acquisition Intelligence:** User-scoped LinkedIn network → Contacts + `Acquisition Network Relationships`. SoT: [`docs/acquisition-intelligence.md`](./acquisition-intelligence.md). Complementary to Decision Radar (person-first vs project-first). Stage 1 is CSV ingestion only.

---

## Architecture

| Layer | Location | Notes |
|-------|----------|--------|
| **Airtable base** | `AIRTABLE_GTM_BASE_ID` | Completely separate from `AIRTABLE_BASE_ID` and `AIRTABLE_BASE_ID_ALT` |
| **CoStar files** | `data/internal/gtm-costar-imports/` | Gitignored; licensed internal reference only |
| **Import tooling** | `scripts/import-gtm-owner-target-costar.mjs` | Dry-run by default; `--apply` writes to GTM base only |
| **Field mapping** | `lib/gtm-owner-target/field-map.js` | Single source of truth for Airtable column names |
| **Decision Radar** | `lib/gtm-owner-target/decision-opportunity-field-map.js` | Decision Opportunities + Evidence (internal acquisition) |

Import script **refuses** to run if `AIRTABLE_GTM_BASE_ID` matches a product base ID.

---

## Setup (one-time)

### 1. Create the Airtable base

1. In Airtable, create a new base (suggested name: **Dealality GTM — Owner Targets**).
2. Copy the base ID from the URL: `https://airtable.com/appXXXXXXXXXXXXXX/...`
3. Add to `.env`:

```bash
AIRTABLE_GTM_BASE_ID=appXXXXXXXXXXXXXX
```

Use the same `AIRTABLE_API_KEY` PAT, with access scoped to this base. For schema creation, the PAT also needs `schema.bases:read` and `schema.bases:write`.

### 2. Create tables and fields

```bash
node scripts/ensure-gtm-owner-target-base.mjs
node scripts/ensure-gtm-owner-target-base.mjs --apply
```

Report: `reports/ensure-gtm-owner-target-base.json`

### 3. Place CoStar export

Export from CoStar **Properties** view (CSV or Excel). Minimum columns:

- True Owner
- Building Name
- Submarket (and/or Market)
- RBA/GLA (optional but recommended)
- Property ID (recommended for re-import dedupe)

Copy files to:

```
data/internal/gtm-costar-imports/
```

---

## Import workflow

**Preview (no writes):**

```bash
node scripts/import-gtm-owner-target-costar.mjs
node scripts/import-gtm-owner-target-costar.mjs --file="data/internal/gtm-costar-imports/my-export.csv"
```

**Apply to GTM base:**

```bash
node scripts/import-gtm-owner-target-costar.mjs --apply
node scripts/import-gtm-owner-target-costar.mjs --dir="data/internal/gtm-costar-imports" --apply
```

Reports:

- `reports/gtm-owner-target-import-preview.json`
- `reports/gtm-owner-target-import-preview.csv`

On `--apply`, the script:

1. Creates an **Import Batch** audit row
2. Upserts **GTM Owner Targets** by normalized owner name (refreshes portfolio metrics; does **not** overwrite outreach/pitch status on existing owners)
3. Creates/updates **GTM Owner Target Properties** by `Source Row Key` or `CoStar Property ID`

---

## Airtable tables

### GTM Owner Targets

One row per **True Owner** (rolled up). Track outreach in:

- **Outreach Status** — `not_contacted` → `intro_sent` → `meeting_scheduled` → …
- **Dealality Pitch Status** — `not_pitched`, `pitched`, `interested`, …
- **Priority Tier** — A / B / C (auto-set on import; editable)
- **Pitch Angle**, **Contact Path**, **Primary Contact***, **Next Action**

\* Contact fields are internal GTM only; do not republish.

### GTM Owner Target Properties

One row per CoStar property, linked to an owner target.

### GTM Import Batches

Audit log per import run (file name, counts, preview report path).

---

## Guardrails

- **Internal only:** `Visibility` = `internal_only` on all owner rows
- **No product sync:** Do not link this base to Dealality app code or public routes
- **No census writes:** CoStar fields must not be copied into Hotel Census or Verified Independent Hotel Census
- **Licensed use:** CoStar data stays within your license; do not share exports outside the team
- **Human review:** Validate A-tier targets before outreach; import tiering is heuristic

---

## Data contract snapshot

| Item | Value |
|------|--------|
| Base env | `AIRTABLE_GTM_BASE_ID` |
| Tables | `GTM Owner Targets`, `GTM Owner Target Properties`, `GTM Import Batches` |
| Mapping module | `lib/gtm-owner-target/field-map.js` |
| Required on owner create | Owner Name, Data Source, Data License, Visibility |
| Select sources | `VAL_*` arrays in `field-map.js` |
| Expected import output | Owner rollup rows + linked property rows + batch audit |

---

## Manual QA checklist

- [ ] `AIRTABLE_GTM_BASE_ID` is **not** the MVP or Platform base ID
- [ ] `ensure-gtm-owner-target-base.mjs --apply` created three tables
- [ ] Import dry-run produces preview JSON/CSV with expected owner count
- [ ] Import `--apply` creates properties linked to owners
- [ ] Re-import updates metrics without resetting Outreach Status on existing owners
- [ ] No new routes or UI reference the GTM base

---

## Regression risks

| Risk | Mitigation |
|------|------------|
| Accidental product base write | `assertNotProductBase()` in import + ensure scripts |
| CoStar data in product | Keep GTM base isolated; no API module for GTM |
| Outreach status wiped on re-import | Update path only refreshes portfolio fields |
| Duplicate properties | Dedupe on `Source Row Key` + `CoStar Property ID` |

---

## ICP classification & strike list

Owner Targets are a **rollup index**, not an outreach list. Use ICP classification to segment owners and build a curated strike list.

### Schema (one-time)

```bash
node scripts/ensure-gtm-owner-target-icp-fields.mjs
node scripts/ensure-gtm-owner-target-icp-fields.mjs --apply
```

Adds to **Owner Targets**:

| Field | Purpose |
|-------|---------|
| ICP Segment | `owner_operator`, `franchisor_brand`, `spv_single_asset`, `skip`, etc. |
| Strike List | Checkbox — qualified for curated outreach |
| Deal Trigger | `none_known`, `conversion`, `reflag`, `independent_unbranded`, `brand_renewal_window`, `development_pipeline`, … |

### Branding decision targets (who needs Dealality now)

Rank CALA owners by **brand / operator / development intent** and join **verified contacts** for outreach:

```bash
node scripts/report-gtm-branding-decision-targets.mjs
node scripts/report-gtm-branding-decision-targets.mjs --outreach-ready-only
node scripts/report-gtm-branding-decision-targets.mjs --country=Mexico
```

Reports: `reports/gtm-branding-decision-targets.json` / `.csv` / `.md`

Scoring module: `lib/gtm-owner-target/branding-decision-signals.js` (weights in `MAP_BRANDING_DECISION_CONFIG`).

**Known limits:** CoStar has no franchise contract expiry — `brand_renewal_window` is heuristic. Land purchases need Phase 2 news/advisor signals.
| ICP Classification Notes | Machine-readable reasons |
| CALA Property Count | Properties in CALA geography |

### Classify owners

```bash
node scripts/classify-gtm-owner-target-icp.mjs
node scripts/classify-gtm-owner-target-icp.mjs --apply
node scripts/classify-gtm-owner-target-icp.mjs --min-cala-properties=3 --apply
```

Reports: `reports/gtm-owner-target-icp-classification.json` / `.csv`

**Strike list criteria (default):**

- ICP segment = asset owner / owner-operator / regional / institutional / REIT
- Priority Tier A or B
- CALA Property Count ≥ 3
- Verified contact (owner-exact match + CALA + hospitality) **or** primary contact email on owner row

### Export strike list

```bash
node scripts/export-gtm-owner-strike-list.mjs
node scripts/export-gtm-owner-strike-list.mjs --from-airtable
node scripts/export-gtm-owner-strike-list.mjs --include-needs-contact
```

Reports: `reports/gtm-owner-strike-list.json` / `.csv`

**Contacts** and **Companies** remain lookup tables — do not use them as the top-down outreach list.

---

## Registry contact verification (build-your-own Bystreet layer)

**Purpose:** Resolve **verified owner contacts** from public CALA registries when CoStar contact exports are missing or US-broker skewed. Internal GTM only.

### Identity graph

```
CoStar property → True Owner entity → public registry (legal rep) → email/LinkedIn
```

### One-time schema

```bash
node scripts/ensure-gtm-registry-contact-fields.mjs
node scripts/ensure-gtm-registry-contact-fields.mjs --apply
```

Adds verification provenance fields to **Contacts** (`Verification Tier`, `Registry System`, `Legal Representative Name`, `Verification URL`, etc.).

Mapping: `lib/gtm-owner-target/contact-field-map.js`  
Registry config by country: `lib/gtm-owner-target/registry-contact-config.js`  
Verification rules: `lib/gtm-owner-target/registry-contact-verification.js`

### Verification tiers

| Tier | Source | Strike-list contact? |
|------|--------|----------------------|
| V1 | CoStar contact + owner-exact CALA match | Yes |
| V1R | Named person email on entity domain + proof URL | Yes |
| V2 | Named executive + LinkedIn + proof URL | LinkedIn outreach |
| V3 | Entity/switchboard/role mailbox only (info@, ir@, contact@) — **not** verified person | Research |

### Phone verification tiers

| Tier | Meaning | Outreach value |
|------|---------|----------------|
| VP2 | Person mobile/cell on proof URL (corp site, LinkedIn, registry) | High — call/text |
| VP1 | Person direct office line on proof URL (not entity switchboard) | High — direct dial |
| VP3 | Entity HQ / toll-free switchboard only | Reference only — not person-verified |

Airtable fields: `Business Phone`, `Mobile Phone`, `Phone Verification Tier` on Contacts.  
Run `node scripts/ensure-gtm-registry-contact-fields.mjs --apply` to create them.

Phone rules live in `lib/gtm-owner-target/registry-phone-verification.js`. Enrichment JSON may include `businessPhone`, `mobilePhone`, `phoneType`, and explicit tiers.

### Outreach tracks (brand-decision filtering)

| Track | Meaning | Default outreach? |
|-------|---------|-------------------|
| `asset_owner` | Asset owner / franchisee / SPV | Yes (if contact + intent) |
| `third_party_brand_decision` | Integrated operator but only third-party-flag/unbranded assets matter | Yes |
| `integrated_operator_mixed` | House-brand portfolio + some third-party/unbranded assets | Yes — pitch eligible assets only |
| `integrated_operator_house_brand_only` | Iberostar-style — owns and operates own brand | **No** for brand-decision outreach |

Logic: `lib/gtm-owner-target/branding-owner-context.js`

```bash
node scripts/report-gtm-brand-decision-enrichment-queue.mjs --country=Mexico
node scripts/report-gtm-brand-decision-enrichment-queue.mjs --country=Mexico --needs-enrichment-only
node scripts/report-gtm-brand-decision-enrichment-queue.mjs --country=Mexico --needs-enrichment-only --limit=20 --suffix=p1-sprint
node scripts/report-gtm-branding-decision-targets.mjs --country=Mexico --brand-decision-only --outreach-ready-only
node scripts/ensure-gtm-registry-contact-fields.mjs --apply
```

Reports: `reports/gtm-brand-decision-enrichment-queue-mx-mexico.*` (full queue); P1 sprint: `...-p1-sprint.*`; CALA P1: `gtm-brand-decision-enrichment-queue-p1-sprint.*`

Portfolio audit (run before outreach — confirms CoStar hotel rollups):

```bash
node scripts/report-gtm-owner-portfolio-audit.mjs --p0-only
node scripts/report-gtm-owner-portfolio-audit.mjs --unsafe-only
node scripts/test-gtm-owner-portfolio-audit.mjs
```

Reports: `reports/gtm-owner-portfolio-audit.json` / `.csv` / `.md`

P0 V1R email sprint (upgrade LinkedIn-only P0 to named email):

```bash
node scripts/generate-gtm-p0-v1r-email-enrichments.mjs
node scripts/generate-gtm-p0-v1r-email-enrichments.mjs --import --dry-run
node scripts/generate-gtm-p0-v1r-email-enrichments.mjs --import --apply
node scripts/test-gtm-owner-lead-asset.mjs
node scripts/sync-gtm-owner-target-contacts.mjs --apply
```

Batch 1 imported (2026-07-04): AHG, Posadas, Norte 19, Landstar, Costa del Sol, Collective, Santa Fe (CFO bridge).

V1R sprint — blocked until proof URL (CoStar-only emails do not qualify):

| Owner | CoStar/sync email | Block reason |
|-------|-------------------|--------------|
| GHL Hoteles | Andrés Fajardo (V2, LinkedIn) | Resolved 2026-07-05 — Jorge Londoño CoStar email cleared; CEO primary via registry sync |
| Grupo Marta | amonge@grupomarta.com | Corp site has no named emails |
| Velas / Oasis / Excellence / Brookfield / Brisas | — | V2 LinkedIn only; no entity-domain email proof |

CALA P1 contact research import:

```bash
node scripts/generate-gtm-p1-cala-enrichment-files.mjs --batch=1
node scripts/generate-gtm-p1-cala-enrichment-files.mjs --batch=2 --import --apply
node scripts/sync-gtm-owner-target-contacts.mjs --apply
```

Batch 1 (8 owners): Gaviota, Essendi, Urbanova, Interlink, GHL, Atlantica, Martinon Grumasa, Grace Bay.

Batch 2 (6 V2 imported, 2 V3 research-only): Globalia (Javier Blanco), Real Hotels (Mafalda Alves Dias), A3 Property (Manuel Tamés), JHSF (Augusto Martins), ICH (Alexandre Gehlen), Mohari/Gencom (Mark Scheinberg). Gran Caribe ×2 remain V3 (Jesús Pérez Balsa — press only, role mailboxes on corp site).

DR P1 contact research import:

```bash
node scripts/report-gtm-brand-decision-enrichment-queue.mjs --country="Dominican Republic" --needs-enrichment-only --suffix=dr-review
node scripts/generate-gtm-p1-dr-enrichment-files.mjs --batch=1 --import --apply
node scripts/sync-gtm-owner-target-contacts.mjs --apply
```

DR Batch 1 (8 V2 imported, 1 V3 research-only): Piñero/Bahia Principe (Encarna Piñero), Impressive (Oscar Martinez), Zemi (Frank Rainieri), Hodelpa (Angel Hernandez V2), Central Romana/Casa de Campo (Andrés Pichardo), Majestic (Amil Maleck), Delveccio→Green Earth bridge (Miguel Barletta), VH Hotels (Roberto Casoni). Zafera (Edward González) remains V3 press-only.

ALIS CALA 2026 delegate roster cross-reference:

```bash
node scripts/report-alis-cala-delegate-crossref.mjs
```

Reports: `reports/gtm-alis-cala-2026-delegate-crossref.md` — 569 invitees/registrants; matches to strike list + branding targets; net-new owner leads. Roster archived under `data/internal/gtm-conference-rosters/`.

DR ALIS Batch 1 (net-new leads from roster):

```bash
node scripts/generate-gtm-p1-dr-enrichment-files.mjs --batch=alis --import --apply
node scripts/sync-gtm-owner-target-contacts.mjs --apply
```

Imported to GTM Contacts (CoStar owner rows): **Noval Properties** (Cesar Latrilla CEO), **Grupo Puntacana S.A.** (Simon Suarez). Prospect-only (acquisition list, no CoStar row): Grupo Abrisa, Ocama, FAURCE, GVA — see `data/internal/dealality-user-acquisition-targets/prospect-seeds.json`.

DR Batch 3 (Santa Maria, Mullen, Rizek research):

```bash
node scripts/generate-gtm-p1-dr-enrichment-files.mjs --batch=3 --import --apply
node scripts/sync-gtm-owner-target-contacts.mjs --apply
```

Imported: **Grupo Santa Maria SA** (Georges Santa-Maria), **Mullen Real Estate Capital** (Jeff Mullen CEO + Javier Coll president). **Rizek Group** remains V3 research-only (CoStar owner label unverified for Dreams Dominicus).

GHL primary contact: Andrés Fajardo is current CEO (Nov 2024); Jorge Londoño CoStar email deprioritized in sync scoring and cleared on owner target. Sync now fetches `Verification Tier` + `Legal Representative Name` from Contacts and upgrades primary when registry V1R/V2 beats stale CoStar. Re-import if needed:

```bash
node scripts/import-gtm-registry-contact-enrichments.mjs --apply --file=data/internal/gtm-registry-enrichments/p1-cala-ghl-hoteles-primary.json
node scripts/sync-gtm-owner-target-contacts.mjs --apply
```

Costa Rica P1 contact research:

```bash
node scripts/report-gtm-brand-decision-enrichment-queue.mjs --country="Costa Rica" --needs-enrichment-only --suffix=cr-review
node scripts/report-gtm-branding-decision-targets.mjs --country="Costa Rica" --outreach-ready-only
node scripts/generate-gtm-p1-cr-enrichment-files.mjs --batch=1 --import --apply
node scripts/sync-gtm-owner-target-contacts.mjs --apply
node scripts/report-dealality-user-acquisition-targets.mjs --country="Costa Rica"
```

CR Batch 1: **Caribe Hospitality** (Daniel Campos), **Grupo Leumi** (Stanley Rattner). Maurice Chartier / Grupo Consutur tracked as advisor referral on acquisition list (not asset owner).

CR Batch 2 (Böëna Lodges, Alojica):

```bash
node scripts/generate-gtm-p1-cr-enrichment-files.mjs --batch=2 --import --apply
node scripts/sync-gtm-owner-target-contacts.mjs --apply
node scripts/report-gtm-branding-decision-targets.mjs --country="Costa Rica" --outreach-ready-only
```

Imported: **Böëna Lodges** (Jack Loeb V1R `jack@boena.com`), **Alojica** (Ana Maria Añez VP Asset Management — CR sub-advisory portfolio).

### Dealality customer acquisition targets

Separate from the CoStar owner strike list — tracks **potential Dealality platform users** (owners, developers, capital advisors, ALIS/LinkedIn warm leads).

```bash
node scripts/report-dealality-user-acquisition-targets.mjs
node scripts/report-dealality-user-acquisition-targets.mjs --country="Dominican Republic"
node scripts/report-dealality-user-acquisition-targets.mjs --priority=P1 --outreach-ready-only
node scripts/report-dealality-user-acquisition-targets.mjs --alis-only --country="Dominican Republic"
```

- Builder: `lib/gtm-owner-target/build-dealality-user-acquisition-targets.js`
- Config/scoring: `lib/gtm-owner-target/dealality-user-acquisition-config.js`
- Manual prospects: `data/internal/dealality-user-acquisition-targets/prospect-seeds.json`
- Reports: `reports/dealality-user-acquisition-targets.{json,csv,md}`

Sources merged: strike list, outreach-ready branding targets, ALIS CALA 2026 matches + net-new CSV, registry enrichments, LinkedIn pilot contacts, manual prospect seeds.

P1 contact research import (Mexico):

```bash
node scripts/generate-gtm-p1-mx-enrichment-files.mjs
node scripts/generate-gtm-p1-mx-enrichment-files.mjs --import --dry-run
node scripts/generate-gtm-p1-mx-enrichment-files.mjs --import --apply
node scripts/ensure-gtm-registry-contact-fields.mjs --apply
```

### Registry enrichment queue (weekly ops)

```bash
node scripts/report-gtm-owner-registry-enrichment-queue.mjs --tier-a-eligible --limit=30
node scripts/report-gtm-owner-registry-enrichment-queue.mjs --from-classification --merge-airtable-properties
```

Reports: `reports/gtm-owner-registry-enrichment-queue.json` / `.csv` / `.md`

Human or agent completes registry lookups; save JSON to `data/internal/gtm-registry-enrichments/` (see README there).

### Mexico Wave 1 — corporate web first (no SIGER signup)

```bash
node scripts/report-gtm-wave1-mx-outreach-plan.mjs
node scripts/draft-gtm-mx-registry-enrichments.mjs
node scripts/import-gtm-wave1-mx-enrichments.mjs --dry-run
node scripts/import-gtm-wave1-mx-enrichments.mjs --apply
```

Reads the queue JSON and writes per-owner **corporate-web-first** draft files to `data/internal/gtm-registry-enrichments/drafts/`. Pre-researched Wave 1 contacts import via `import-gtm-wave1-mx-enrichments.mjs`.

Adapters: `lib/gtm-owner-target/adapters/mx-corporate-web-first.js`, `mx-corporate-web-seeds.js`. SIGER/RNT optional fallbacks only.

### Import enrichments

```bash
node scripts/import-gtm-registry-contact-enrichments.mjs --dry-run
node scripts/import-gtm-registry-contact-enrichments.mjs --apply
node scripts/sync-gtm-owner-target-contacts.mjs --apply
node scripts/classify-gtm-owner-target-icp.mjs --apply
```

Example record: `fixtures/gtm-registry-enrichment-example.json`

### Phase 2 (automated fetch adapters)

Country adapters (`planned` in registry config): Mexico SIGER/RNT, Colombia RUES, DR Registro Mercantil, Brazil CNPJ, Chile Registro Empresas. Portals may require captcha/login — queue + import ships first; adapters added per country.

### Tests

```bash
node scripts/test-gtm-registry-contact-verification.mjs
node scripts/test-gtm-owner-icp-classify.mjs
```

---

## Pilot Target List — owner/advisor pilot outreach

**Purpose:** Curated contact-level list for **founder-led pilot outreach** (owners, advisors, operators, referral sources). Separate from CoStar **Owner Targets** rollup and from product **Target List** (brand shortlist per deal).

| Item | Value |
|------|--------|
| **Base** | `AIRTABLE_GTM_BASE_ID` (same GTM base) |
| **Table** | `Pilot Target List` (`tblgsKWuI25MWohAP`) |
| **Field mapping** | `lib/gtm-owner-target/pilot-target-list-field-map.js` |
| **Outreach helpers** | `lib/gtm-owner-target/pilot-target-list-outreach.js` |

### Principles

- **Airtable is SSOT** for outreach tracking — not offline docs alone.
- **First wave = manual send** — no automated email, no CRM, no Gmail/SendGrid yet.
- **CALA-first, not CALA-only**: prioritize CALA for real pilot opportunities; use warm non-CALA contacts for feedback/referrals unless explicitly prioritized.
- **Brand/operator sensitivity**: do not ask brands/operators for confidential owner pipelines; ask for criteria input, operator perspective, or owner-opt-in introductions only.
- **Final Approved Email** is the version used for mail merge export.
- **Ready for Mail Merge** must be checked before export.
- **Do Not Contact** always excludes a record.
- **Outreach Status** is the mail-merge workflow field; legacy **Status** is kept unchanged.

### Pilot Target List Dropdown Options

Standardized dropdown workflow is managed by:

```bash
node scripts/setup-pilot-target-list-dropdown-options.mjs --dry-run
node scripts/setup-pilot-target-list-dropdown-options.mjs --execute
node scripts/setup-pilot-target-list-dropdown-options.mjs --execute --migrate-values
```

Key rules:

- **Pilot Region** is the primary pilot-targeting dropdown.
- **Region** is broader geographic context (multi-select); keep for coverage context, not first-pass targeting.
- Pilot remains **CALA-first, not CALA-only**.
- For real pilot opportunities, prioritize `CALA`, `Mexico`, `Caribbean`, `Central America`, `South America`, `Latin America`.
- Warm non-CALA contacts can be used for feedback/referral/workflow validation.
- Brands/operators should not be asked to share confidential owner pipelines.
- Owner/deal information should come from the owner, advisor, or authorized representative.
- **Approved does not mean sent**; it means human-approved copy only.
- **Ready for Mail Merge** controls export eligibility with Approved status and required fields.
- **Do Not Contact** always excludes records from send/export workflows.
- **No destructive option cleanup by default**; old values are preserved unless explicitly migrated and reviewed.

### Field setup (one-time)

```bash
node scripts/setup-owner-targets-outreach-fields.mjs --dry-run
node scripts/setup-owner-targets-outreach-fields.mjs --execute
```

Report: `reports/setup-owner-targets-outreach-fields.json`

Reuses existing fields where possible:

| Required concept | Existing field | Notes |
|------------------|----------------|-------|
| Message angle (picklist) | Outreach Message Angle | Do not duplicate |
| Message narrative | Why They Matter | Free text |
| Last contacted | Last Contact Date | Do not add Last Contacted Date |
| Priority | Priority | P1/P2/P3 already configured |
| Legacy relationship status | Status | Keep; add Outreach Status for workflow |

### Readiness report

```bash
node scripts/report-owner-targets-outreach-readiness.mjs
```

Report: `reports/owner-targets-outreach-readiness.json`

### Mail-merge export (read-only)

Exports rows where **Outreach Status = Approved**, **Ready for Mail Merge** checked, **Do Not Contact** unchecked, and required email fields present.

```bash
node scripts/export-owner-targets-mail-merge.mjs --dry-run
node scripts/export-owner-targets-mail-merge.mjs --batch "Pilot Wave 1" --output reports/owner-targets-mail-merge.csv
```

CSV columns: `email`, `first_name`, `last_name`, `full_name`, `company`, `role`, `subject`, `message`, `linkedin_url`, `send_channel`, `mail_merge_batch`, `airtable_record_id`.

### Pilot Target List Views

Four grid views support manual/founder-led outreach. **Views do not send email** — they are Airtable UI filters only.

| View | Purpose |
|------|---------|
| **Pilot Outreach Pipeline** | Daily active outreach tracking |
| **Drafting Queue** | Copy drafting and review |
| **Approved for Send / Mail Merge** | Pre-export checklist for manual send or CSV export |
| **Follow-Up Needed** | Scheduled or status-based follow-ups |

Setup:

```bash
node scripts/setup-pilot-target-list-views.mjs --dry-run
node scripts/setup-pilot-target-list-views.mjs --execute
```

- Config: `lib/gtm-owner-target/pilot-target-list-view-config.js`
- Report: `reports/pilot-target-list-views-report.json`
- Manual steps: `reports/pilot-target-list-views-manual.md`

Rules:

- **Approved for Send / Mail Merge** is the view to verify before `export-owner-targets-mail-merge.mjs`.
- **Do Not Contact** excludes a target from sending and export (filters enforce this).
- First wave remains **manual / founder-led** — no automated sending.

Airtable Meta API can **list** views but **cannot create or configure** filters, sorts, or visible fields — create views manually using the manual report.

### Field descriptions

Field descriptions clarify purpose for drafting, manual outreach, and mail-merge prep. They do **not** enable automated sending.

```bash
node scripts/setup-pilot-target-list-field-descriptions.mjs --dry-run
node scripts/setup-pilot-target-list-field-descriptions.mjs --execute
node scripts/setup-pilot-target-list-field-descriptions.mjs --execute --overwrite
```

- Mapping: `lib/gtm-owner-target/pilot-target-list-field-descriptions.js`
- Report: `reports/pilot-target-list-field-descriptions-report.json`
- Manual fallback: `reports/pilot-target-list-field-descriptions-manual.md`

Rules:

- **Final Approved Email** is the mail-merge message body source of truth.
- **Ready for Mail Merge** controls export eligibility (with Approved status and required email fields).
- **Do Not Contact** always excludes a row from outreach and export.
- Existing meaningful descriptions are preserved unless `--overwrite` is passed.
- No automated email sending is implemented in this phase.

### Draft fill (wave 1)

Populate missing outreach planning and draft fields from existing row context (no invented emails/URLs):

```bash
node scripts/fill-pilot-target-list-outreach-drafts.mjs --dry-run
node scripts/fill-pilot-target-list-outreach-drafts.mjs --execute
```

- Logic: `lib/gtm-owner-target/pilot-target-list-draft-fill.js`
- Templates: `lib/gtm-owner-target/pilot-outreach-draft-templates.js`
- Reports: `reports/pilot-target-list-draft-fill-report.json` / `.md`

Rules: fills blank fields only (unless `--overwrite`); never sets Approved or Ready for Mail Merge; never fills Final Approved Email; skips Do Not Contact rows.

### Tests

```bash
node scripts/test-owner-targets-outreach-export.mjs
node scripts/test-pilot-target-list-field-descriptions.mjs
node scripts/test-pilot-target-list-view-config.mjs
```
