# Operator Setup — New-Base Writer Extension (Phase B)

**Date:** 2026-05-25  
**Status:** Complete (mapping + validation). **Not** production-enabled (`OPERATOR_SETUP_USE_NEW_BASE_WRITER` remains `0` in `.env.example`).

## Goal

Extend the new-base writer for **high-value** fields consumed by Operator Alignment Snapshot (OAS), Operator Explorer, and Operator Strategy — without mapping all 417 form fields, without schema/scoring/PDF/My Deals UX changes.

## Live Airtable confirmation

Verified via Meta API (`AIRTABLE_BASE_ID` + `AIRTABLE_API_KEY`):

| Table | Column | Type | Mapped |
|-------|--------|------|--------|
| Operator Setup - Master | Data Confidence Level | singleSelect | Yes (writer `adminMap`) |
| Operator Setup - Master | Source Type | multipleSelects | Yes (writer `adminMap`) |
| Operator Setup - Master | Last Updated Date | date | Yes (writer `adminMap`) |
| Operator Setup - Master | company_name | singleLineText | Yes (writer `createOrUpdateOperatorMaster`) |
| Operator Setup - Profile & Positioning | companyDescription, website, headquarters, primaryServiceModel, yearEstablished, yearsInBusiness, company_name | various | Yes (build sheet) |
| Operator Setup - Platform & Markets | chainScale, specificMarkets, totalProperties, totalRooms, Brands Portfolio Detail, company_name | various | Yes (build sheet) |

No Airtable schema changes were made in this phase.

## Fields mapped

### Master (hardcoded writer — not 1:1 build sheet loop)

| Form `name` | Airtable column | Notes |
|-------------|-----------------|-------|
| `companyName` | `company_name` | Required on create/update |
| `dataConfidenceLevel` | Data Confidence Level | OAS inject + admin block |
| `sourceType` | Source Type | OAS inject |
| `lastUpdatedDate` | Last Updated Date | OAS inject (`typecast` via `coerceFieldValue`) |

Manifest: `api/lib/operator-setup-new-base-phase-b-fields.json` → `masterWriterHardcoded`.

### Profile & Positioning (build sheet)

- `companyName` → `company_name`
- `companyDescription`, `website`, `headquarters`, `primaryServiceModel`
- `yearEstablished`, `yearsInBusiness`

### Platform & Markets (build sheet)

- `companyName` → `company_name`
- `chainScale`, `specificMarkets`, `totalProperties`, `totalRooms`
- `regions` → `specificMarkets` (array → CSV text in writer)
- `brandsPortfolioDetail` → `Brands Portfolio Detail` (JSON long text)

### Commercial & Governance

- **`company_name` not mapped** — schema backup lists this column on Commercial/Governance, but **live Airtable does not** (Meta API 2026-05-25). OAS/Strategy use **Master + Profile** `company_name` only. Documented in `blocked` section of phase-b manifest.

**Build sheet:** 103 → **117** rows after Phase B merge (`node scripts/merge-operator-setup-phase-b-build-sheet.mjs`).

## Skipped (documented)

| Item | Decision |
|------|----------|
| `companyLogo` | Multipart file upload not wired to new-base writer; Explorer reads attachment when manually uploaded. **Phase C+.** |
| `dealTermsOptIn` | **Needs Decision** — admin-only vs operator UI vs retire. Not blocking Phase B. |
| `diligenceQaOptIn` | **Needs Decision** — same as above. |
| `overview_*`, most Owner Value / deal-terms scalars | Out of scope (281 Static Form Only remain). |
| `parentCompany` | No column in new-base schema backup; not invented. |

## `regions` / `regionsSupported`

- Form `regions` (multiselect) now coerces to Platform `specificMarkets` on new-base save.
- **Read path:** `regionsSupported` for list/OAS is still **computed** from `geo_*` totals in `operator-setup-new-base-read.js` (unchanged).

## Files modified

| File | Change |
|------|--------|
| `api/lib/operator-setup-new-base-phase-b-fields.json` | **New** — Phase B manifest |
| `api/lib/operator-setup-new-base-build-sheet-rows.json` | +16 rows, `phaseBMergedAt` |
| `api/lib/operator-setup-new-base-writer.js` | `regions`→`specificMarkets`; Master admin `coerceFieldValue` |
| `scripts/merge-operator-setup-phase-b-build-sheet.mjs` | **New** |
| `scripts/validate-operator-setup-new-base-writer-coverage.mjs` | **New** |
| `scripts/test-operator-setup-new-base-save-coverage.mjs` | **New** (dry-run default) |
| `scripts/generate-operator-setup-field-coverage-diff.mjs` | Master Phase B + OAS admin forms |
| `docs/operator-setup-field-coverage-diff.md` | Regenerated |
| `reports/operator-setup-field-coverage-diff.csv` | Regenerated |
| `docs/operator-side-system-comparison.md` | Phase B status |
| `docs/operator-alignment-snapshot-implementation-checklist.md` | Phase B checklist |

**Not modified:** scoring weights, BAS, OCS, OAS PDF layout, My Deals Operator Strategy UX, legacy writer, `.env` production flag.

## Validation results

```bash
node scripts/validate-operator-setup-new-base-writer-coverage.mjs
# → All Phase B coverage checks passed (live meta OK)

node scripts/test-operator-setup-new-base-save-coverage.mjs
# → Dry-run: all Phase B fields would persist from sample payload

node scripts/generate-operator-setup-field-coverage-diff.mjs
```

### Regenerated diff summary (after Phase B)

| Metric | Before Phase B | After Phase B |
|--------|----------------|---------------|
| Build sheet rows | 103 | 117 |
| Fully Covered | 111 | 128 |
| Legacy Only | 38 | 26 |
| OAS-needed, not new-base mapped | 3 | **0** |
| Strategy-needed, not new-base mapped | 10 | **2** (Commercial/Governance `company_name` in backup only — not live columns) |
| Explorer-needed, not new-base mapped | 53 | 51 |

## Remaining high-priority gaps

1. **`companyLogo`** — Explorer list/detail; needs attachment pipeline on new-base save.
2. **`overview_*` Explorer narrative** — many columns exist on Profile/Platform but not in build sheet (Phase C).
3. **26 Legacy Only (Medium risk)** — e.g. `companyTagline`, `missionStatement`, contact fields on Governance — not required for OAS P0.
4. **281 Static Form Only** — intentionally deferred.

## `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1` safety

| Environment | Recommendation |
|-------------|----------------|
| **Staging** | **Safe to test** with flag `1` on a **test operator** Master id: run intake save → `GET /api/third-party-operators?activeOnly=1` → OAS `/companies` → Operator Strategy table. Use `scripts/test-operator-setup-new-base-save-coverage.mjs --apply` only on disposable test data when instructed. |
| **Production** | **Not yet.** Legacy writer remains default. Enable only after staging backfill of Active operators’ P0 fields and QA sign-off. Dual-write shadow (`OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE`) can compare before cutover. |

## Regenerate / merge

```bash
node scripts/merge-operator-setup-phase-b-build-sheet.mjs
node scripts/validate-operator-setup-new-base-writer-coverage.mjs
node scripts/test-operator-setup-new-base-save-coverage.mjs
node scripts/generate-operator-setup-field-coverage-diff.mjs
```
