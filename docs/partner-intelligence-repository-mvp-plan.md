# Partner Intelligence Repository — MVP Plan

**Status:** Planning & schema (Phase 1–2). No live Explorer wiring yet.  
**Goal:** Source-backed, human-approved brand and operator profiles — no hallucinated data in Brand Explorer or Operator Explorer.

**Reference file root (user-provided):**  
`G:\My Drive\Dealality™\Platform Design & Build\Brand Reference Material\{Parent Company or Operator Name}\`

---

## 1. Repo audit summary

### Stack & patterns

| Layer | Current state |
|-------|----------------|
| **Frontend** | Static HTML/JS in `public/`; app shell in `public/app.js` |
| **API** | Express handlers in `api/`; central field maps in `api/lib/*-map.js` |
| **Auth** | `memberstackAuth` + `requireDealalityUser`; Airtable keys server-only |
| **Airtable** | Primary base (`AIRTABLE_BASE_ID`) — Brand Setup, Operator Setup, Deals, Users |
| **Platform base** | `AIRTABLE_BASE_ID_ALT` — Hotel Census, clause/financial libraries, independent census staging |
| **Batch ETL** | `scripts/apply-*-batch.mjs` with `--dry-run`, schema validation via Meta API |
| **Source policy precedent** | `lib/independent-census/source-registry.js` |

### What exists today (relevant)

| Capability | Location | Gap vs Partner Intelligence |
|------------|----------|------------------------------|
| Brand Explorer UI | `public/brand-explorer-combined.html`, `public/js/brand-explorer-atelier-from-api.js` | No structured source/evidence/approval pipeline |
| Brand data API | `api/brand-library.js` | Reads Brand Setup + presentation slots; no fact-level provenance |
| Footprint trust labels | `lib/brand-explorer-footprint-trust.js` | Verification fields defined but **not wired** on GET |
| Choice FDD extraction | `scripts/extract-choice-fdd-item19.mjs`, `fixtures/choice-fdd-text/` | Batch-only; writes Brand Setup directly (bypasses review) |
| Operator Explorer UI | `public/operator-explorer-gold-mock.html`, section JS modules | Section **DEFAULTS** can look like live data when empty |
| Operator data API | `api/third-party-operator-detail.js`, `api/lib/operator-setup-new-base-read.js` | Setup-sourced; no external document provenance |
| OAS (alignment) | `api/operator-alignment-snapshot.js` | Correctly separate from profile — keep separate |
| Scout mock sources | `api/dealality-scout.js` | Mock `sourceType`, `reviewStatus`, `confidenceScore` — good UI shape reference |
| Partner Directory | `api/partner-directory.js` | **People/companies** — not document sources |
| Independent census staging | `docs/verified-independent-hotel-census-schema.md` | Best architectural precedent for Candidates → Evidence → Approved |

### What does **not** exist

- No `Partner Source Library` table or API
- No extracted-facts staging with human review gates
- No publish workflow from approved facts → Explorer
- No Helena AI material intake table
- No public source discovery engine (only ad-hoc FDD scripts for Choice)

---

## 2. Current Brand Explorer data flow

```
Airtable (primary base)
├── Brand Setup - Brand Basics (+ child tables: Footprint, Fee Structure, …)
└── Brand Setup - Brand Explorer Presentation (slot rows: hero.*, overview.*, …)
         │
         ▼
GET /api/brand-library/brand?brandId=
  • Merges structured tables + brandExplorer.blocks[]
  • Optional censusSummary when BRAND_EXPLORER_CENSUS_METRICS=1
         │
         ▼
brand-explorer-combined.html
  • 10 atelier tabs via slotKey lookups
  • Footprint trust heuristics (hero verification text — fragile)
  • standards.source_confidence / standards.last_reviewed = free-text slots only
```

**Trust today:** `Active` checkbox on presentation rows; `Live` on legacy Modules; footprint heuristics. **Not** source-linked facts with reviewer identity.

**Fit-to-deal** (`POST /api/brand-explorer/fit-to-deal`) returns algorithmic evidence strings — not document-backed facts.

---

## 3. Current Operator Explorer data flow

**Canonical live profile:** `public/operator-explorer-gold-mock.html` opened from `public/operator-explorer.html` (list → popup iframe). Same shell as `operator-dna-profile.html` when loaded with `?operatorId=rec…`.

**Tab count:** **11 tabs** (9 core panels + 2 dynamically mounted extensions). Optional **12th** tab **Alignment Context** when `?dealId=` is present.

```
Airtable (primary base)
├── Operator Setup - Master (+ Profile, Platform, Commercial, Governance)
└── Child tables: Operating Platform, Brand Relationships, Engagement, Explorer Materials, …
         │
         ▼
GET /api/intake/third-party-operators/:recordId
  • buildPrefillObjectFromNewBaseRows + child mappers
  • operatorExplorerMaterials → Operator Materials tab
         │
         ▼
operator-explorer-gold-mock.html + operator-explorer-gold-mock-data.js
  • buildViewModel → buildPanels → mount()
  • mountDnaExtensionTabs() adds tabs 10–11
  • Section modules with DEFAULTS when JSON empty  ⚠️
  • Optional ?dealId= → Alignment Context tab (OAS — separate)
```

### Operator Explorer — 11 tabs (canonical)

| # | Tab | Primary JS module | Data source |
|---|-----|-------------------|-------------|
| 1 | Profile & Positioning | `operator-explorer-new-base-profile.js` | Master, Profile, `explorerProfileJson` |
| 2 | Operating Platform | `operator-operating-platform-sections.js` | Operating Platform child / `op_*_json` |
| 3 | Brand & Relationships | `operator-brand-relationships-sections.js` | Brand Relationships child / `brand_*_json` |
| 4 | Markets & Footprint | `operator-markets-footprint-sections.js`, `operator-market-experience-section.js` | Platform, census footprint |
| 5 | Owner Engagement & Reporting | `operator-engagement-reporting-sections.js` | Engagement child / `ov_*_json` |
| 6 | Infrastructure & Data | `operator-infrastructure-sections.js` | Infrastructure Platform child |
| 7 | Leadership | `operator-leadership-team-sections.js`, `operator-leadership-profile-detail.js` | Leadership Team Members + Leadership Platform |
| 8 | Project Fit & Deal Profile | `operator-best-fit-deal-profile-sections.js` | Commercial `bf_*_json` (preferences, not OAS scores) |
| 9 | Proof & Track Record | `operator-dna-case-studies-be.js` | Case Studies child table |
| 10 | **Operator Materials** | `operator-dna-materials.js` | `Operator Setup - Explorer Materials` (`operatorExplorerMaterials.blocks`) |
| 11 | **Dealality Insights** | `operator-dna-dealality-insights.js` | **Dealality-derived** completeness/gap signals — **not** source-backed factual profile |

Tabs 10–11 are appended at mount time via `operator-dna-profile-mount.js` → `mountDnaExtensionTabs()` (also called from `operator-explorer-gold-mock-data.js`).

**Partner Intelligence scope for Operator Explorer:**

| Tab | In Partner Intelligence publish scope? |
|-----|----------------------------------------|
| 1–9 | Yes — factual profile fields from approved sources |
| 10 Operator Materials | Yes — source documents link to Source Library; gallery slots are evidence artifacts |
| 11 Dealality Insights | **No** — internal Dealality analysis; keep separate from extracted/published facts |
| Alignment Context (optional) | **No** — deal-scoped OAS; already separate |

**Alignment vs profile:** OAS scoring lives in `lib/operator-alignment-scoring-factors.js` and is surfaced only via Alignment Context (`?dealId=`). **Do not merge** alignment signals or Dealality Insights scores into Partner Intelligence published fields.

**Write path risk:** `OPERATOR_SETUP_USE_NEW_BASE_WRITER=0` in `.env.example` — Setup may not write to tables Explorer reads.

---

## 4. Recommended Airtable schema (new tables)

**Base:** Primary (`AIRTABLE_BASE_ID`) — required for links to Brand Basics and Operator Setup - Master.

Detailed field specs:

| Doc | Table |
|-----|-------|
| [partner-source-library-airtable-fields.md](./partner-source-library-airtable-fields.md) | `Partner Intelligence - Source Library` |
| [partner-extracted-facts-airtable-fields.md](./partner-extracted-facts-airtable-fields.md) | `Partner Intelligence - Extracted Facts` |
| [partner-explorer-published-fields-airtable-fields.md](./partner-explorer-published-fields-airtable-fields.md) | `Partner Intelligence - Published Explorer Fields` |
| [partner-helena-intake-airtable-fields.md](./partner-helena-intake-airtable-fields.md) | `Partner Intelligence - Helena Outreach Intake` |

### Design choice: Published fields table vs duplicating full Explorer schemas

**MVP recommendation:** One **`Partner Intelligence - Published Explorer Fields`** table (normalized rows: entity + section + field + approved value + supporting fact links).

**Why not clone 40+ Explorer columns into new tables?**

- Brand Explorer already spans Brand Basics, 10+ child tables, and 100+ presentation slots.
- Operator Explorer spans Master + 5 parent tables + 9 child tables + 22 JSON fields.
- Duplicating would create drift within weeks.

**Publish rule:** Approved extracted facts → upsert rows in Published Explorer Fields → **read merge** in `brand-library.js` / operator detail API overlays approved values (with internal provenance metadata) without overwriting Setup rows until explicitly published.

**Later enhancement:** Materialized views or batch sync into presentation slots for owner-facing copy polish.

---

## 5. API routes needed (server-side only)

All routes: `memberstackAuth` + `requireDealalityUser` + **admin/reviewer role gate** (reuse Users role fields).

| Method | Route | Purpose | Phase |
|--------|-------|---------|-------|
| GET | `/api/partner-intelligence/sources` | List/filter sources | 3–4 |
| GET | `/api/partner-intelligence/sources/:id` | Source detail + linked facts count | 3–4 |
| POST | `/api/partner-intelligence/sources` | Create source record (manual intake) | 4 |
| PATCH | `/api/partner-intelligence/sources/:id` | Update status, quality, approvals | 4 |
| POST | `/api/partner-intelligence/sources/:id/upload` | Multipart → disk + Airtable attachment ref | 4 |
| POST | `/api/partner-intelligence/extraction/run` | Run extraction for approved source(s) | 5 |
| GET | `/api/partner-intelligence/extraction/runs/:runId` | Run status + fact counts | 5 |
| GET | `/api/partner-intelligence/facts` | List facts (filters: profile, status, section) | 5–6 |
| GET | `/api/partner-intelligence/facts/:id` | Fact + evidence + source | 6 |
| PATCH | `/api/partner-intelligence/facts/:id/review` | Approve / edit / reject / visibility | 6 |
| POST | `/api/partner-intelligence/publish` | Promote approved facts → Published Explorer Fields | 7 |
| GET | `/api/partner-intelligence/published` | Read published fields (internal QA) | 7 |
| POST | `/api/partner-intelligence/discovery/capture` | Store URL candidates (status Found/Captured) | 8 |
| GET | `/api/partner-intelligence/discovery/patterns` | Return curated search patterns | 8 |
| GET/POST | `/api/partner-intelligence/helena-intake` | Helena material requests | 9 |
| POST | `/api/partner-intelligence/helena-intake/:id/link-source` | Create Source Library row from intake | 9 |

**Central mapping:** `api/lib/partner-intelligence-field-map.js` (see stub in repo).

**Never:** expose `AIRTABLE_API_KEY` or run extraction in browser.

---

## 6. Backend extraction workflow (Phase 5)

```
Source record (Approved for Extraction = Yes)
    │
    ▼
POST /api/partner-intelligence/extraction/run
    │
    ├─ Resolve file: Airtable attachment OR PARTNER_REFERENCE_ROOT path OR URL fetch (public only)
    ├─ Extract text: PDF → Python subprocess (reuse extract-pdf-text.py) / HTML → cheerio
    ├─ Classify: source type, profile type (brand/operator/parent)
    ├─ Map sections: partner-intelligence-explorer-field-registry.js
    ├─ LLM assist (optional, server-side): suggest facts with evidence quotes ONLY
    │     └─ Every suggestion → Partner Extracted Facts (Pending Review)
    ├─ Attach: evidence text, page/section, confidence, extraction run ID
    └─ Update source status → Extracted / Needs Review
```

**Separation rules:**

| Extraction Type | Human Review Status default | Can auto-publish? |
|-----------------|----------------------------|-------------------|
| Directly Stated | Pending | **No** |
| Inferred | Pending | **No** |
| Needs Confirmation | Pending | **No** |
| Data gap marker | Pending | **No** |

**Missing data copy (standard):** `"Not confirmed in available sources."`

**Conflicts:** create separate fact rows per conflicting claim; never merge silently.

---

## 7. Public source discovery workflow (Phase 8 — capture only)

**Not scraping at scale.** MVP = curated patterns + manual/semi-automated capture.

```
Reviewer selects brand/operator + pattern template
    │
    ▼
GET /api/partner-intelligence/discovery/patterns?profileType=brand|operator
    │
    ▼
Reviewer runs search externally (Google) OR future server-side search API
    │
    ▼
POST /api/partner-intelligence/discovery/capture
  { url, title, profileType, brandId?, operatorId?, suggestedQuality, notes }
    │
    ▼
Partner Source Library row: Status = Found | Captured, Verified Source = No
```

See [partner-source-discovery-patterns.md](./partner-source-discovery-patterns.md).

**Quality hierarchy:** encoded in `Source Quality` + `Source Origin` + reviewer confirmation — not auto-set from third-party PDFs.

---

## 8. Helena AI intake workflow (Phase 9)

```
Helena outreach logged → Partner Intelligence - Helena Outreach Intake
    │
    ▼
Materials received → upload to PARTNER_REFERENCE_ROOT/{company}/
    │
    ▼
POST link-source → Partner Source Library
  Source Origin = Brand Provided | Operator Provided
  Visibility from intake notes
    │
    ▼
Same extraction → review → publish pipeline
```

---

## 9. Admin review workflow (Phase 6)

**UI:** new internal page `public/partner-intelligence-review.html` (admin-only).

| Action | Effect |
|--------|--------|
| Approve | Sets Approved Value; Human Review Status = Approved |
| Edit | Approved Value ≠ Extracted Value; status = Edited |
| Reject | Status = Rejected; no publish |
| Internal-only | Public Visibility = Internal Only |
| Restricted | Public Visibility = Restricted |
| Stale source | PATCH source Status = Stale; flag linked published fields |
| Needs more source | Status = Needs More Source + Follow-up Question |
| Gap export | Filter Data Gap = Yes → CSV for Helena outreach |

### Publishing rule (enforce in API)

Explorer field may publish only if **all** true:

1. ≥1 approved extracted fact supports the field
2. Source quality ≥ Medium (on supporting source(s))
3. Public visibility = Public OR user has internal role
4. Source status ≠ Stale
5. Reviewer set Approved Value
6. `Approved for Explorer Use` on source = Yes (when source-linked)

---

## 10. Frontend / Explorer integration plan (Phase 7+)

### Brand Explorer

| Concern | Approach |
|---------|----------|
| Owner-facing copy | Continue presentation slots + structured tables; overlay **published** values from read merge |
| Internal users | Dev/admin panel: source badge, confidence, evidence drawer |
| Data gaps | Internal-only section or admin overlay; public sees neutral copy or omits field |
| Footprint | Wire `readFootprintVerificationFromFields()` first (quick win, separate ticket) |
| Fit / alignment | Unchanged — not part of Partner Intelligence |

### Operator Explorer (11 tabs)

| Concern | Approach |
|---------|----------|
| Remove false confidence | Replace section DEFAULTS (tabs 1–9) with explicit empty states when no published fact |
| Profile vs OAS | Alignment Context tab (`?dealId=`) stays deal-scoped only |
| Operator Materials (tab 10) | Link gallery items to Source Library; respect visibility on attachments |
| Dealality Insights (tab 11) | **Out of scope** for publish merge — Dealality-derived, not source-backed profile |
| Source-backed badge | Internal role only on tabs 1–10 factual content |

### New internal surfaces

- `partner-intelligence-review.html` — fact review queue
- `partner-intelligence-sources.html` — source library admin
- Optional: embed source count on Brand Setup / Operator Setup (later)

---

## 11. Implementation phases

| Phase | Scope | Deliverables |
|-------|-------|--------------|
| **1** | Audit | This document ✓ |
| **2** | Schema design | Airtable field docs + field map stub + ensure-table script |
| **3** | Source Library | Table + CRUD API + manual upload |
| **4** | Manual intake | Upload to Drive path + Airtable; status workflow |
| **5** | Extraction | Server extraction run → Extracted Facts (Pending) |
| **6** | Review UI | Admin review + approve/reject/edit |
| **7** | Publish connect | Read merge into Brand/Operator Explorer APIs |
| **8** | Discovery capture | Pattern docs + capture API (no auto-scrape) |
| **9** | Helena intake | Outreach table + link-to-source |
| **10** | Hardening | Stale detection, gap reports, confidence rollups |

**MVP scope (Phases 2–7 for one brand + one operator pilot):**

- Choice/Radisson Blu OR Marriott Autograph (brand pilot)
- Hotel Equities OR Arbor Lodging (operator pilot)
- Manual PDF upload + extraction + review + single-field publish proof

**Defer:** full automation, bulk parent-company rollout, Explorer UI badges for owners, LLM extraction without human review.

---

## 12. Files to create or modify (by phase)

### Phase 2 (schema — now)

| File | Action |
|------|--------|
| `docs/partner-*-airtable-fields.md` | Create |
| `api/lib/partner-intelligence-field-map.js` | Create |
| `api/lib/partner-intelligence-explorer-field-registry.js` | Create (section/field catalog) |
| `scripts/ensure-partner-intelligence-tables.mjs` | Create |
| `.env.example` | Add `PARTNER_REFERENCE_ROOT`, table overrides |

### Phase 3–7 (implementation — later)

| File | Action |
|------|--------|
| `api/partner-intelligence-sources.js` | Create |
| `api/partner-intelligence-facts.js` | Create |
| `api/partner-intelligence-extraction.js` | Create |
| `api/partner-intelligence-publish.js` | Create |
| `api/partner-intelligence-discovery.js` | Create |
| `api/partner-intelligence-helena-intake.js` | Create |
| `server.js` | Register routes + auth |
| `api/brand-library.js` | Read merge published brand fields |
| `api/third-party-operator-detail.js` | Read merge published operator fields |
| `public/partner-intelligence-review.html` | Create |
| `public/js/partner-intelligence-review.js` | Create |
| `lib/partner-intelligence-publish-rules.js` | Create |
| `scripts/extract-partner-source-text.mjs` | Create (generalize Choice FDD extract) |

---

## 13. Environment variables

```bash
# Partner Intelligence (add to .env.example in Phase 2)
PARTNER_REFERENCE_ROOT=G:\My Drive\Dealality™\Platform Design & Build\Brand Reference Material
PARTNER_INTELLIGENCE_SOURCE_TABLE=Partner Intelligence - Source Library
PARTNER_INTELLIGENCE_FACTS_TABLE=Partner Intelligence - Extracted Facts
PARTNER_INTELLIGENCE_PUBLISHED_TABLE=Partner Intelligence - Published Explorer Fields
PARTNER_INTELLIGENCE_HELENA_TABLE=Partner Intelligence - Helena Outreach Intake
PARTNER_INTELLIGENCE_EXTRACTION_ENABLED=0          # gate LLM/batch extraction
PARTNER_INTELLIGENCE_PUBLISH_ENABLED=0             # gate writes to published table
PARTNER_INTELLIGENCE_ADMIN_ROLES=Platform Admin,Dealality Admin
```

Existing vars reused: `AIRTABLE_API_KEY`, `AIRTABLE_BASE_ID`, `MEMBERSTACK_*`.

---

## 14. Data quality rules

1. **No fact without evidence text** (min 20 chars quoting source).
2. **No publish without approved fact** linking to published field.
3. **Third-party PDFs:** `Verified Source = No`, max quality Medium until reviewer confirms.
4. **FDD / official brochure:** quality High only when current filing confirmed.
5. **Restricted sources:** never merge into public Explorer responses.
6. **Inferred facts:** never publish without editor rewrite to Directly Stated or Edited approved value.
7. **Stale sources:** auto-flag published fields when source age > configurable threshold (later).
8. **Conflicts:** multiple pending facts allowed; publish requires explicit reviewer choice.
9. **Empty fields:** use standard gap copy — never invent filler.
10. **Overwrite protection:** publish API must not overwrite Setup/presentation without `force` flag + audit log.

---

## 15. MVP scope vs later enhancements

| MVP | Later |
|-----|-------|
| Manual upload + metadata | Automated discovery crawlers |
| PDF/text extraction | OCR for scanned decks |
| Human review queue | Bulk approve with rules |
| Single-table published fields | Sync to presentation slots |
| Admin-only provenance UI | Owner-facing source citations |
| Choice FDD path reuse | All parent companies |
| Capture-only discovery | Scheduled search jobs |
| Helena intake form | Helena API webhook |

---

## 16. Risks and limitations

| Risk | Mitigation |
|------|------------|
| Schema drift vs Explorer | Field registry single source; version column on published rows |
| Dual Brand API (`brand-library` vs `brand-explorer`) | Publish merge only on `brand-library` (combined UI) |
| Operator DEFAULTS mask gaps | Phase 7 requires empty states when no published fact |
| Legacy operator writer | Enable new-base writer before operator publish pilot |
| LLM hallucination | Facts always Pending; evidence required; no auto-publish |
| Drive path not on server | Railway deploy needs synced folder or S3 migration |
| Airtable attachment limits | Large PDFs stored on Drive; attachment optional |
| Record volume | Normalized published table scales better than wide profiles |
| Legal/confidential | Visibility + confidentiality notes enforced in API read filters |

---

## 17. Change impact classification

**High impact** (when implemented): Airtable writes, Explorer read merge, publish workflow.

**Rollback:** Set `PARTNER_INTELLIGENCE_PUBLISH_ENABLED=0`; Explorer reverts to Setup-only reads. Published table rows remain but are ignored.

**Regression retest:** Brand Explorer combined (all tabs), Operator gold-mock (all tabs), OAS panel, Brand/Operator Setup forms, Choice FDD batch scripts (must remain independent until migrated).

---

## 18. Manual QA checklist (post Phase 7)

- [ ] Upload Choice FDD PDF → Source Library row Created
- [ ] Run extraction → Facts created as Pending with evidence quotes
- [ ] Approve one brand fee fact → Publish → Brand Explorer shows value
- [ ] Reject fact → Explorer unchanged
- [ ] Restricted source fact → not visible in public API response
- [ ] Operator pilot: approve portfolio fact → gold-mock tab updates
- [ ] OAS alignment unchanged when profile field publishes
- [ ] No Airtable keys in browser network tab

---

## Data contract snapshot (target)

| Table | Mapping object | Required links |
|-------|----------------|----------------|
| Source Library | `MAP_PARTNER_SOURCE` | Profile Type; ≥1 of Parent Company / Brand / Operator |
| Extracted Facts | `MAP_PARTNER_FACT` | Source Record; Field Name; Extracted Value |
| Published Explorer Fields | `MAP_PARTNER_PUBLISHED` | Brand or Operator link; Field Name; Approved Value |
| Helena Intake | `MAP_PARTNER_HELENA` | Profile Type; Contact; Requested Materials |

**Select options:** defined in schema docs — validate against Meta API before writes (same as `apply-choice-fee-structure-batch.mjs`).
