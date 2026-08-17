# Curio Extraction Context Audit

**Date:** 2026-07-06  
**Status:** Root cause confirmed — **code fix applied** (read-path / extraction preview only; no Airtable writes in this task)  
**Target:** Curio Collection by Hilton — `receQkxgjlezsc1xg`

> **Related:** [curio-clean-reextraction-plan.md](./curio-clean-reextraction-plan.md), [curio-pi-package-integrity-cleanup-plan.md](./curio-pi-package-integrity-cleanup-plan.md)  
> **Reports:** `reports/curio-clean-reextract.{md,json}` (before/after fix)

---

## Summary

| Question | Finding |
|----------|---------|
| Primary issue class | **Bad extraction fallback** + **default Kimpton context leakage** |
| Bad source linkage? | **No** — clean sources are linked to `receQkxgjlezsc1xg` |
| Bad brand registry mapping (Airtable Brand Alias)? | **No** — not used in PI extraction path |
| Bad extraction fallback? | **Yes** — Kimpton/IHG `fixedValue` hints applied to all brands |
| Missing Curio context? | **Yes** — `extractBrandFactsFromText` did not receive target brand context |
| Default/sample Kimpton leakage? | **Yes** — global `BRAND_FIELD_EXTRACTION_HINTS` was Kimpton-only |
| Footprint `"06"` issue? | **Bad pattern** — loose `/Global\s+(\d+)\b/` matched FDD date text |

**Verdict:** The contamination was **not** from Mexico FDD source linkage on the clean US FDD / fact sheet rows. It was from **code-level Kimpton/IHG extraction hints** being applied to every brand extraction, with blind registry fallback when source text did not contain the Kimpton/IHG substring.

---

## Evidence

### 1. Where “Fixed from brand registry” is generated

| Location | Behavior |
|----------|----------|
| `lib/partner-intelligence/brand-extract-rules.js` → `tryExtractField()` | When a field hint has `fixedValue` but the value is **not found** in source text (`idx < 0`), evidence is set to `` `Fixed from brand registry (${anchor})` `` and the fixed value is still returned. |

**Before fix:** No brand context check — any hint with `fixedValue` could blind-fill.

**After fix:** Blind fallback only when `hints.pilotKey === brandContext.pilotKey` (matching target brand). Evidence string includes target brand name.

### 2. Registry / fallback source for Kimpton / IHG values

| File | Content |
|------|---------|
| `lib/partner-intelligence/brand-field-extraction-hints.js` (pre-fix) | Global map with `be.identity.brandName.fixedValue = "Kimpton Hotels"` and `be.identity.parentCompany.fixedValue = "IHG Hotels & Resorts"` |
| `api/lib/partner-intelligence-explorer-field-registry.js` | `PILOT_BRANDS.curioCollection` correctly defines Curio / Hilton Worldwide — **not used** by extraction before fix |

Kimpton values came from **hard-coded extraction hints**, not from Airtable Brand Alias Mapping or Brand Setup records.

### 3. Curio brand record mapping

| Check | Result |
|-------|--------|
| `PILOT_BRANDS.curioCollection.recordId` | `receQkxgjlezsc1xg` ✓ |
| Source `recy2pyEahF9UUsEk.brandId` | `receQkxgjlezsc1xg` ✓ |
| Source `recL1qfHCOAUZr9Rz.brandId` | `receQkxgjlezsc1xg` ✓ |

Curio was **not** mapped to the wrong brand record in PI sources.

### 4. Brand Alias Mapping

Brand Alias Mapping (`AIRTABLE_BASE_ID_ALT`) is used for **hotel census affiliation resolution**, not Partner Intelligence extraction. **Not involved** in this bug.

### 5. Source linkage

Clean sources are correctly linked to Curio. Mexico FDD (`recIH5lyY8MASnfrp`) is excluded from clean re-extract allowlist. **Not the cause** of Kimpton/IHG on US FDD / fact sheet previews.

### 6. Default sample brand context

`extractBrandFactsFromText()` accepted only `sourceTitle`, `sourceRole`, `localFilePath` — **no `brandId` or pilot context**. Hints were resolved from a **single global Kimpton-oriented map**.

### 7. Missing Curio context → Kimpton fallback

When Curio FDD/fact sheet text did not contain “Kimpton” / “IHG” substrings, `tryExtractField` still returned Kimpton/IHG via `fixedValue` + “Fixed from brand registry”.

### 8. Why `be.footprint.globalHotels` became `"06"`

Pattern in hints: `/Global\s+(\d+)\b/i` matched **“Global 06 January 2025”** in US Curio FDD exhibit header text — not a hotel count.

**Fix:** Stricter pattern `/Global\s+(\d{2,})\s+([\d,]+)\s+(\d+)/i` (table row format). When no match → gap fact (`Not confirmed in available sources.`).

---

## Root Cause

Partner Intelligence brand extraction was built for the **Kimpton pilot** with a **single global hint table** (`BRAND_FIELD_EXTRACTION_HINTS`). The extraction pipeline never passed **target brand context** from `source.brandId` into rule resolution. Identity fields with `fixedValue` were **always** applied when text match failed, leaking Kimpton/IHG onto Curio (and any non-Kimpton brand).

Secondary issue: footprint regex was too permissive for FDD date fragments.

---

## Fix Plan (implemented)

| Change | File |
|--------|------|
| Resolve pilot brand context from `brandId` | `lib/partner-intelligence/brand-extraction-context.js` (new) |
| Scope hints per `pilotKey`; identity hints from `PILOT_BRANDS` metadata | `lib/partner-intelligence/brand-field-extraction-hints.js` |
| Guard blind `fixedValue` fallback; block cross-brand identity leak | `lib/partner-intelligence/brand-extract-rules.js` |
| Pass `brandContext` from `source.brandId` | `lib/partner-intelligence/run-extraction.js` |
| Unit tests | `scripts/test-partner-intelligence-extraction-context.mjs` |

### Guardrails

1. **Identity hints** built dynamically from `PILOT_BRANDS` (`brandName`, `parentCompany`).
2. **Blind registry fallback** only when `hints.pilotKey === brandContext.pilotKey`.
3. **No Kimpton hints** for Curio or unresolved context.
4. **Wrong-brand leak detector** blocks Kimpton/IHG identity on non-Kimpton targets.
5. **Missing context** → identity fields become gap / manual review, not Kimpton fallback.

---

## Post-Fix Dry-Run Results

`npm run curio-clean-reextract -- --dry-run` (2026-07-06 after fix):

| Field | Before | After |
|-------|--------|-------|
| `be.identity.brandName` | Kimpton Hotels | **Curio Collection by Hilton** (source text evidence) |
| `be.identity.parentCompany` | IHG Hotels & Resorts | **Hilton Worldwide** (source text or Curio-scoped registry fallback) |
| `be.footprint.globalHotels` | `06` | **Gap** — Not confirmed |
| Contamination warnings | 4 blocked | **0** |
| Would write on apply | 1 (bad footprint) | **5** (identity rows — still require human review) |

---

## Apply Safety

| Status | Note |
|--------|------|
| Kimpton/IHG fallback | **Removed** from Curio preview |
| Extraction apply | **Still requires founder review** — identity values now look correct but all facts remain Pending; stewardship + manual QA before approval |
| Do not bulk-approve | Review evidence quotes per fact; reject any residual wrong-brand rows from legacy Mexico batch |

**Recommended sequence after founder approval:**

```bash
npm run curio-clean-reextract -- --apply --approve-curio-clean-reextract
npm run steward-partner-intelligence -- --entity-type brand --target-rec-id receQkxgjlezsc1xg --dry-run
```

---

## Regression Checklist

- [x] `npm run test:partner-intelligence-extraction-context`
- [x] `npm run curio-clean-reextract -- --dry-run` — no Kimpton/IHG
- [x] `npm run test:partner-intelligence-stewardship-package`
- [x] `npm run audit-partner-intelligence-publish-readiness`
- [ ] Kimpton pipeline re-run (optional) — confirm Kimpton identity still extracts

---

## Change Impact

| Tier | Classification |
|------|----------------|
| Extraction context fix | **Medium** — brand read/extract path; Kimpton + Curio pilots |
| Airtable | **None** in this task |

**Rollback:** Revert `brand-extraction-context.js`, `brand-field-extraction-hints.js`, `brand-extract-rules.js`, `run-extraction.js` changes.
