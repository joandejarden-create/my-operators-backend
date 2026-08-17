# Governance Read Path + Explorer Trust Label Plan

**Date:** 2026-07-06  
**Status:** Planning only — no API changes, no UI labels, no write paths, no scoring changes in this phase.

> **Authority:** [INTELLIGENCE_GOVERNANCE.md](./INTELLIGENCE_GOVERNANCE.md), [brand-operator-validation-fields-plan.md](./brand-operator-validation-fields-plan.md), [NAMING_AND_COPY_GUIDE.md](../ai-build-system/NAMING_AND_COPY_GUIDE.md)

---

## Purpose

This plan defines how Dealality should **read** P1 profile-level governance fields from Airtable and **display** simple trust/source labels in Brand Explorer and Operator Explorer — without overclaiming, without duplicating Partner Intelligence source metadata, and without changing scoring until a later phase.

Goals:

- Normalize governance into a single API object per profile.
- Gate owner-visible labels through `Usage Permission` + `External Display Status`.
- Treat blank governance as **unknown**, not validated.
- Keep source-level provenance in **Partner Intelligence**; Setup roots carry **profile-level trust/status** only.

---

## Current State

### Schema (live)

P1 profile governance columns exist on four tables (14 fields each, except documented aliases):

| Layer | Tables |
|-------|--------|
| **Brand** | `Brand Setup - Brand Basics`, `Brand Setup - Brand Explorer Presentation` |
| **Operator** | `Operator Setup - Master`, `Operator Setup - Explorer Materials` |

Post-apply audit: **64** P1 exact matches, **0** high-priority gaps on P1 tables. Company Profile minimal fields remain out of scope.

### Governance layers

| Layer | Role | Status |
|-------|------|--------|
| **Partner Intelligence** | Source URL/file, capture date, extraction, human review, publish overlay | Schema-ready; not wired to Explorer read path yet |
| **Setup root/profile tables** | Profile-level `Validation Status`, `Usage Permission`, review dates, display gating | **Columns live**; **not read by APIs or UI** |
| **Legacy hero fields** | `Explorer Hero Verification`, `Explorer Hero Data Source` on Brand Basics / Operator Master | **Read today** by Explorer hero UI — pre-P1 ad hoc labels |

### Read paths today

| Product surface | Primary API | Airtable source |
|---------------|-------------|-----------------|
| Brand Explorer (combined, gold, atelier) | `GET /api/brand-library/brand` | Brand Setup child tables + Presentation blocks |
| Brand list / dashboards | `GET /api/brand-library/brands` | Brand Basics |
| Operator Explorer list | `GET /api/third-party-operators` | Operator Setup - Master (+ linked reads) |
| Operator Explorer detail | `GET /api/intake/third-party-operators/:id` | New-base bundle via `operator-setup-new-base-read.js` |
| Operator Explorer (alt route) | `GET /api/operator-explorer/operator` | Delegates to detail/list |

**No `governance` object** is returned today. Trust-like copy comes only from legacy hero fields in `brand-explorer-gold-detail.js` and `operator-explorer-gold-mock-data.js`.

---

## Brand Read Path

### APIs

| API | Use governance? | Notes |
|-----|-----------------|-------|
| `GET /api/brand-library/brand?brandId=` | **Yes — primary** | Detail payload for Explorer pages |
| `GET /api/brand-library/brands` | Optional summary | List cards — defer trust chips to Phase 3+ |
| `GET /api/brand-explorer/brand` | Align later | Secondary brand-explorer route; prefer brand-library SSOT |

### Table roles

| Table | Role |
|-------|------|
| **`Brand Setup - Brand Basics`** | **Canonical profile trust** for brand-level Explorer header |
| **`Brand Setup - Brand Explorer Presentation`** | Slot copy + optional **row-level** governance; defers to Basics for header when Basics is set |
| **Partner Intelligence - Published Explorer Fields** | Future overlay for field-level approved values — not Phase 1 read path |

### Primary vs secondary

**Primary for trust label:** `Brand Setup - Brand Basics`

**Secondary:** Presentation rows — use only when:

- Basics governance is blank, **and**
- Presentation has a single “profile root” row or consistent governance across rows (future convention), **or**
- A specific tab/section needs a **local** label (Phase 4+ — not header).

**Recommended rule:** Header trust label always resolves from **Basics first**. Presentation governance informs **section-level** notes later, not the profile header in Phase 3.

### Conflict handling (Basics vs Presentation)

See [Conflict Rules](#conflict-rules). Summary: **more restrictive wins**; Basics wins for `Validation Status` / `Company Validated` when both are populated.

### Fields returned to UI (owner-facing)

After gating (see Trust Label Rules):

| Field | Expose to UI |
|-------|----------------|
| `validationStatus` | Yes — as `displayLabel` when allowed |
| `sourceRegion` | Yes — region subtitle when allowed |
| `lastReviewedDate` | Yes — formatted subtitle |
| `companyValidated` | Yes — only when checkbox + date support claim |
| `confidenceLevel` | Cautious internal wording only in Phase 3; optional muted chip |
| `displayLabel`, `displaySubtitle` | Derived — preferred for UI |

### Fields internal-only (never owner-facing)

`usagePermission`, `evidenceNotes`, `missingDataFlags`, `internalNotes`, `reviewedBy`, `refreshDueDate`, raw `externalDisplayStatus` (UI uses derived `showTrustLabel` boolean), Partner Intelligence links.

### Implementation hook

Extend `getBrandById` / brand detail builder in `api/brand-library.js` (~`brandDetails` object) to attach `governance` from Basics fields. Read Presentation governance only for conflict merge + future section labels.

**Central field map (future):** `lib/profile-governance/map-brand-profile-governance.js` (or extend `p1-profile-governance-field-specs.js` with read keys).

---

## Operator Read Path

### APIs

| API | Use governance? | Notes |
|-----|-----------------|-------|
| `GET /api/intake/third-party-operators/:id` | **Yes — primary** | Detail via `third-party-operator-detail.js` + `loadNewBaseOperatorBundle` |
| `GET /api/third-party-operators?activeOnly=1` | Optional summary | List — defer chips |
| `GET /api/operator-explorer/operator` | **Yes** | Same detail shape |

### Table roles

| Table | Role |
|-------|------|
| **`Operator Setup - Master`** | **Canonical profile trust** for Operator Explorer header |
| **`Operator Setup - Explorer Materials`** | Materials slot copy + optional row-level governance; secondary for header |
| **Partner Intelligence** | Source/publish layer — not Phase 1 |

### Primary vs secondary

**Primary for trust label:** `Operator Setup - Master`

**Secondary:** `Operator Setup - Explorer Materials` — section/materials context only in later phases.

**Existing partial equivalents on Master (do not duplicate in reads):**

- `Source Type` — already live; map into `governance.sourceType`
- `Data Confidence Level` — **profile governance confidence column on Operator Setup - Master** (P1 spec name `Confidence Level` is not a live column on this table). Read path (`normalize-profile-governance.js`) and pilot writes (`scripts/pilot-profile-governance-values.mjs`) map `Confidence Level` ↔ `Data Confidence Level` for `governance.confidenceLevel`. If both columns ever exist on a row, prefer explicit `Confidence Level` when set, else fall back to `Data Confidence Level`.
- `Last Updated Date` — **not** `lastReviewedDate`; expose separately as staleness hint internal-only
- `Explorer Hero Verification` / `Explorer Hero Data Source` — legacy; keep until UI migrates to `governance.displayLabel`

### Conflict handling (Master vs Materials)

Same as brand: Master wins for profile-level trust; Materials cannot elevate trust above Master.

### Fields returned / internal

Same split as Brand Read Path.

### Implementation hook

Extend `loadNewBaseOperatorBundle` / detail response in `api/third-party-operator-detail.js` to attach `governance` from Master fields. Materials governance merged only for conflict detection.

**Central field map (future):** `lib/profile-governance/map-operator-profile-governance.js`

---

## Trust Label Rules

### Gating order (all profiles)

A trust label may appear owner-facing **only if all** pass:

1. `externalDisplayStatus` ∈ `Show Trust Label` (or unset — see [Blank behavior](#blank--missing-governance-behavior))
2. `usagePermission` ∉ `Internal Only`, `Do Not Use`
3. `validationStatus` ∉ `Do Not Use`, `Needs Review` (external), `Internal Only` (if ever used as validation)
4. `companyValidated` display requires: checkbox **true** AND (`validationStatus` = `Company Validated` OR explicit company validation workflow) AND preferably `companyValidationDate` set

**Default when `externalDisplayStatus` blank:** treat as **Hide Trust Label** externally.

### Validation Status → display

| Validation Status | Owner-visible label? | Notes |
|-------------------|----------------------|-------|
| Company Validated | Yes (gated) | External: **Company-Validated Profile**; requires checkbox + date |
| Company Reviewed | Yes | External: **Company-Reviewed Profile** |
| Company Published | Yes (conservative) | External: **AI-Assisted Profile** + Source Basis: Company Materials — not “Company Published” |
| Source-Informed | Yes | External: **Source-Informed Profile** + Source Basis: Reviewed Sources |
| Owner-Provided | **No** (default) | Deal/opportunity scoped — not mapped to external chip |
| AI-Assisted | Yes (muted) | External: **AI-Assisted Profile** + Source Basis: AI-assisted research |
| Needs Review | **No** external | Internal warning only |
| Stale / Refresh Needed | **No** external (default) | Not mapped to external chip until steward review |
| Do Not Use | **Never** | Suppress all external trust |

### Usage Permission

| Value | Effect |
|-------|--------|
| Internal Only | Suppress external trust label |
| Platform Display Allowed | May show if validation + external display allow |
| Scoring Allowed | No UI effect in Phase 1–3; no scoring change yet |
| External Snapshot Allowed | No snapshot footnotes yet (Phase 4) |
| Company Validated | Reinforces do-not-overwrite; does not alone justify label without checkbox |
| Do Not Use | Suppress everything external |

### External Display Status

| Value | Effect |
|-------|--------|
| Show Trust Label | Allow derived label |
| Hide Trust Label | No external label (default if blank) |
| Internal Only | Admin/internal only |
| Needs Review | No external label; internal “Governance Not Set” or “Needs Review” |
| Do Not Display | Hard suppress |

---

## Recommended UI Labels

Use [NAMING_AND_COPY_GUIDE.md](../ai-build-system/NAMING_AND_COPY_GUIDE.md) — Proper Case, calm, evidence-aware.

**External Explorer trust labels** (`governance.displayLabel` / `displaySubtitle`) are **conservative** and do not mirror raw `Validation Status` verbatim. Confidence is **internal only** — never in `displaySubtitle`.

| Internal Validation Status | External `displayLabel` | External `sourceBasis` (subtitle) |
|----------------------------|-------------------------|-----------------------------------|
| Company Validated (gated) | **Company-Validated Profile** | — |
| Company Reviewed | **Company-Reviewed Profile** | — |
| Company Published | **AI-Assisted Profile** | Company Materials |
| Source-Informed | **Source-Informed Profile** | Reviewed Sources |
| AI-Assisted | **AI-Assisted Profile** | AI-assisted research |
| Needs Review / Do Not Use / blank | *(no chip)* | — |

**Subtitle format** (when label shown): `Last Reviewed: [date] · Source Basis: [basis] · Region: [region]` — omit missing segments; **never** include `Confidence:`.

| Derived subtitle part | When |
|-----------------------|------|
| **Last Reviewed: [Date]** | `lastReviewedDate` set |
| **Source Basis: [basis]** | Mapped from validation status (see table) |
| **Region: CALA-specific** | `sourceRegion` = CALA-Specific |
| **Region: Global Reference** | `sourceRegion` = Global Reference |

**Do not show externally:** raw `Company Published`, `Confidence: High/Medium/Low`, “Validated” alone, “Brand Approved”, “Dealality recommends”, unmapped validation statuses.

### PI publish proposal: source basis drives validation (brand + operator)

**Decision (2026-07-06):** External trust labels must be driven by **source basis** and validation level, not by whether the entity is a brand or operator. Implemented in `lib/partner-intelligence/profile-governance-publish-readiness.js` (`assessPublishScopeSourceBasis`, `inferValidationStatus`).

| Approved publish-scope source basis | Internal `Validation Status` | External chip |
|-------------------------------------|------------------------------|---------------|
| All company-controlled profile source types | **Company Published** | **AI-Assisted Profile** · Source Basis: **Company Materials** |
| Mixed company-controlled + reviewed/public | **Source-Informed** | **Source-Informed Profile** · Source Basis: **Reviewed Sources** |
| All reviewed/public/third-party | **Source-Informed** | **Source-Informed Profile** · Source Basis: **Reviewed Sources** |
| Unknown (default) | **Source-Informed** unless company-origin fallback applies | Per mapping above |

**Company-controlled** profile source types include: Company Website, Company PDF / Brochure, Official Website, Company Materials, Company Published Materials, Company Brochure, Company Fact Sheet, and PI types mapped to those (e.g. `Website Capture` → Company Website, `FDD` → Company PDF / Brochure when company/regulatory materials).

**Reviewed/public** types include: Public Sources + AI Extraction, Third-Party Website, News / Press, Blog, Industry Database, Mixed Sources, Hospitality Media, Unknown, and similar.

**Not implied:** Company Reviewed, Company Validated, or external display of “Company Published”. Operator official website captures with `Public Web` origin still classify as company-controlled when PI `Source Type` maps to Company Website.

**Legacy hero lines:** Phase 3 uses `governance.displayLabel` only; legacy `explorerHeroVerification` / `explorerHeroDataSource` are no longer shown as external trust labels in Explorer headers.

---

## Blank / Missing Governance Behavior

| Context | Behavior |
|---------|----------|
| **Owner-facing Explorer** | **No trust label** (do not show badge) |
| **Internal/admin** | Show **Governance Not Set** warning in review UI |
| **Validation inference** | Blank ≠ validated; blank ≠ AI-Assisted |
| **Scoring (future)** | Blank must not increase confidence; treat as unknown |

**Initial population:** Most profiles will be blank after schema apply. Factory/fixture brands should be bulk-set to `AI-Assisted` + `Needs Review` + `Hide Trust Label` via admin script or Airtable views — separate ops task, not automatic.

---

## Conflict Rules

### Brand Basics vs Brand Explorer Presentation

| Conflict | Resolution |
|----------|------------|
| Basics: Company Published; Presentation: AI-Assisted | **Basics wins** for internal `validationStatus`; external chip uses conservative mapping (Company Published → AI-Assisted Profile) |
| Stricter `usagePermission` on either row | **More restrictive wins** (`Do Not Use` > `Internal Only` > display allowed) |
| `Do Not Use` on either | **Suppress external label** |
| `externalDisplayStatus` Hide vs Show | **Hide wins** |
| Presentation-only governance populated | Use for **internal** section warnings only until section-level UI exists |

### Operator Master vs Explorer Materials

Same pattern; **Master wins** for profile header.

### Usage Permission vs External Display Status

- `Internal Only` permission **always** suppresses external label even if `Show Trust Label`.
- `Do Not Use` permission overrides all display status.

### Company Validated

Display **Company Validated** only when:

- `Company Validated` checkbox is true, **and**
- `companyValidationDate` is set (recommended required), **and**
- `validationStatus` is `Company Validated` or `usagePermission` includes company-validated protection

If checkbox false but status says Company Validated → **internal warning**, no external label.

---

## API Response Shape

Recommend a normalized object on brand/operator detail responses:

```json
{
  "governance": {
    "validationStatus": "Source-Informed",
    "usagePermission": "Platform Display Allowed",
    "sourceType": "Company Website",
    "sourceRegion": "CALA-Specific",
    "confidenceLevel": "Medium",
    "sourceBasis": "Reviewed Sources",
    "lastReviewedDate": "2026-05-01",
    "refreshDueDate": null,
    "companyValidated": false,
    "companyValidationDate": null,
    "externalDisplayStatus": "Show Trust Label",
    "displayLabel": "Source-Informed Profile",
    "displaySubtitle": "Last Reviewed: May 1, 2026 · Source Basis: Reviewed Sources · Region: CALA-specific",
    "showTrustLabel": true,
    "internalWarnings": [],
    "sources": {
      "canonicalTable": "Brand Setup - Brand Basics",
      "mergedFrom": []
    }
  }
}
```

**Internal/admin variant** may add: `evidenceNotes`, `missingDataFlags`, `internalNotes`, `reviewedBy`, `usagePermission`, raw fields, `governanceNotSet: true`.

**List endpoints:** omit or include `governance.displayLabel` only when `showTrustLabel` — defer to Phase 3.

---

## UI Placement

### Phase 1 — API only

Attach `governance` to detail APIs; no visible Explorer change.

### Phase 2 — Internal/admin labels

| Surface | Placement |
|---------|-----------|
| Brand Setup / Operator Setup (admin) | Governance summary card: raw fields + `internalWarnings` |
| Internal profile review (future) | Full governance + Partner Intelligence link |

### Phase 3 — Explorer header labels (first owner-visible)

| Surface | File / area |
|---------|----------------|
| Brand Explorer profile header | `public/js/brand-explorer-gold-detail.js`, `brand-explorer-combined.html` hero |
| Operator Explorer profile header | `public/js/operator-explorer-gold-mock-data.js` hero / `operator-explorer-detail.html` |

Single compact chip + optional subtitle; no label when `showTrustLabel === false`.

### Defer

| Item | Phase |
|------|-------|
| BAS / OAS snapshot footnotes | 4 |
| Scoring / confidence impact | 5 |
| Consideration / shortlist cards | 3+ |
| Company validation self-serve workflow | 5 |
| Partner Intelligence publish overlay in Explorer | After read helpers stable |
| Per-slot Presentation/Materials trust labels | 4+ |

---

## Scoring / Snapshot Impact

**Phase 1–3: no scoring changes.**

Document for future:

| Governance signal | Future scoring rule |
|-------------------|---------------------|
| Company Validated / Company Published / Source-Informed | May support claims when `Scoring Allowed` |
| AI-Assisted | Weak signal only; label clearly |
| Needs Review / Do Not Use | Must not drive scoring |
| Stale / Refresh Needed | Downgrade confidence |
| Blank governance | Unknown — no confidence boost |

OAS `Data Confidence` on deal requests and Master `Data Confidence Level` remain **separate** from profile `governance` until explicit unification.

---

## Implementation Phases

### Phase 1 — API read path only

**Status: implemented (2026-07-06)**

- `lib/profile-governance/profile-governance-fields.js` — Airtable column map
- `lib/profile-governance/normalize-profile-governance.js` — `normalizeProfileGovernance()`
- `GET /api/brand-library/brand` → `brand.governance`
- `GET /api/intake/third-party-operators/:id` → `operator.governance`
- Tests: `npm run test:profile-governance-normalizer`

**Not yet:** snapshot footnotes (Phase 4), scoring impact (Phase 5).

### Phase 3 — Explorer header labels — **implemented (minimal)**

- Shared chip helper: `public/js/profile-governance-trust-chip.js` (`ProfileGovernanceTrustChip.governanceTrustChipHtml`)
- Brand Explorer: `renderPresentationHero` in `public/js/brand-explorer-gold-detail.js` — chip in `brand-hero__verified-line` when `brand.governance.displayLabel` is set; no chip when null
- Operator Explorer: `buildHeroVerificationLineHtml` + `mountProfileChrome` in `public/js/operator-explorer-new-base-profile.js`; `governance` passed via `buildViewModel` in `public/js/operator-explorer-gold-mock-data.js`
- Script tags: `brand-explorer-combined.html`, `brand-explorer-gold-mock.html`, `operator-explorer-gold-mock.html`
- **Rules:** Renders only `displayLabel` + `displaySubtitle`; never `internalWarnings` or raw governance fields; no fallback from `validationStatus` or legacy hero verification lines
- **E2E pilot script:** `npm run pilot-profile-governance-values -- --brand "<name|rec…>" --operator "<name|rec…>"` (dry-run default; `--apply` after founder approval). Reports: `reports/profile-governance-pilot-values.{json,md}`. **Pilot applied (2026-07-06):** Best Western Plus + Viento Sur Gestión Hotelera — end-to-end trust chips validated. Operator confidence writes target **`Data Confidence Level`**; the script resolves this alias automatically.
- **PI → profile governance publish (planned):** [partner-intelligence-profile-governance-publish-plan.md](./partner-intelligence-profile-governance-publish-plan.md) — reviewed PI → Setup profile governance; no automated publish yet.

### Phase 2 — Internal/admin labels

- Setup pages or admin-only panel shows governance + warnings
- “Governance Not Set” for blank profiles

### Phase 3 — Explorer header labels — **implemented (minimal)**

- Replace legacy hero verification lines with governance chip when `displayLabel` is set
- Respect gating via normalizer (`displayLabel` null when not eligible)
- See implementation notes under Phase 1 section above

### Phase 4 — Snapshot source notes

- BAS/OAS printable footnotes from `governance` when `External Snapshot Allowed`

### Phase 5 — Scoring confidence impact

- Downgrade/suppress based on governance; requires product sign-off

---

## Risks / Open Questions

| Question | Recommendation |
|----------|----------------|
| **Canonical brand trust table?** | **Brand Basics** for header; Presentation for future section labels |
| **Canonical operator trust table?** | **Operator Master** for header; Materials secondary |
| **Blank governance externally?** | **Hide label** — yes |
| **Initial value population?** | Manual/admin bulk update; factory brands → `AI-Assisted` + internal review |
| **Company Validated requires checkbox + date?** | **Yes** for external “Company Validated” label |
| **Presentation row-level governance** | Columns exist per row; header must not merge 1:n rows without aggregation rules — default to Basics |
| **Legacy hero fields** | Deprecate gradually after Phase 3; map to governance when blank |
| **`Data Confidence Level` vs `Confidence Level`** | On **Operator Master**, profile governance confidence is stored in **`Data Confidence Level`** (alias); `Confidence Level` is not live on that table. Read helper and pilot script map the alias; prefer `Confidence Level` only if both exist |
| **Partner Intelligence overlay** | Do not block Phase 1; field-level overlay via `publish-overlay.js`. **Profile governance publish plan:** [partner-intelligence-profile-governance-publish-plan.md](./partner-intelligence-profile-governance-publish-plan.md) |

---

## Recommended Next Step

**Create normalized governance read helpers** (Phase 1) — no UI labels yet:

1. Add `lib/profile-governance/normalize-profile-governance.js` with merge + gating logic.
2. Add `lib/profile-governance/map-profile-governance-fields.js` (Airtable column names → API keys).
3. Attach `governance` to `GET /api/brand-library/brand` and `GET /api/intake/third-party-operators/:id`.
4. Add tests: blank, Do Not Use, Company Validated without checkbox, Basics vs Presentation conflict.
5. Run existing route/auth tests; no Explorer HTML/JS changes.

---

## Related

- [brand-operator-validation-fields-plan.md](./brand-operator-validation-fields-plan.md)
- [INTELLIGENCE_GOVERNANCE.md](./INTELLIGENCE_GOVERNANCE.md)
- [DATA_VALIDATION_PROTOCOL.md](./DATA_VALIDATION_PROTOCOL.md)
- [CONTENT_QA_CHECKLIST.md](./CONTENT_QA_CHECKLIST.md)
- `api/brand-library.js`, `api/third-party-operator-detail.js`, `lib/operator-explorer-hero-labels.js`
- `reports/brand-operator-validation-schema-diff.md`
