# Curio Clean Re-Extraction Plan

**Date:** 2026-07-06  
**Status:** Planning — **do not publish profile governance** until clean facts are approved from narrow company-material sources.  
**Target:** Curio Collection by Hilton — Brand Setup `receQkxgjlezsc1xg`

> **Authority:** [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md), [DATA_VALIDATION_PROTOCOL.md](./DATA_VALIDATION_PROTOCOL.md), [CONTENT_QA_CHECKLIST.md](./CONTENT_QA_CHECKLIST.md)  
> **Related:** [curio-pi-package-integrity-cleanup-plan.md](./curio-pi-package-integrity-cleanup-plan.md) (integrity investigation + quarantine), [partner-intelligence-stewardship-fix-plan.md](./partner-intelligence-stewardship-fix-plan.md)  
> **Reports (read-only inputs):** `reports/partner-intelligence-stewardship-package.{md,json}`, `reports/partner-intelligence-publish-readiness.{md,json}`, `reports/curio-pi-contaminated-facts-quarantine.{md,json}`

---

## Purpose

Curio Collection by Hilton must **not** be published from the current mixed, contaminated Partner Intelligence package. The package contains wrong-brand extraction values (Kimpton / IHG), a broad source rollup that triggers **mixed company/public source origins**, and **zero approved facts**.

This plan defines a **narrow, company-material re-extraction path** from two Curio-specific sources only. The goal is to establish a **clean approved identity and positioning baseline** before any Explorer Use approvals, fact approvals, or profile-governance publish dry-run.

**This document is planning only.** It does not approve sources, approve facts, write to Airtable, or run extraction in this task.

---

## Current State

| Metric | Value | Source |
|--------|-------|--------|
| Brand Basics record | `receQkxgjlezsc1xg` — Curio Collection by Hilton | Stewardship report 2026-07-06 |
| Sources in linked package | **15** | Stewardship report |
| Total facts | **144** | Stewardship report |
| Approved facts | **0** | Stewardship report |
| Pending candidate facts | **120** | Stewardship report (after exclusion logic) |
| Rejected / quarantined facts | **24** | Quarantine apply + stewardship exclusion |
| Recommended facts (stewardship) | **8** (footprint-only from US FDD; no identity) | Stewardship report |
| All sources `Approved for Explorer Use` | **No** (all 15) | Publish readiness audit |
| Publish eligible | **false** | Publish readiness audit |
| Origin conflict | **yes** — `mixed_company_and_public_source_origins` | Publish readiness audit |
| Clean approved identity facts | **none** — no `be.identity.*` with Curio/Hilton values approved | Integrity cleanup plan |

### Quarantine status (completed)

- **24 contaminated facts** from Mexico FDD (`recIH5lyY8MASnfrp`) marked `Human Review Status = Rejected` with quarantine reviewer notes.
- **78 facts** from the same Mexico FDD source remain **Pending** (no Kimpton/IHG text match) — require manual batch review or rejection; **do not bulk-approve**.
- Stewardship assistant **no longer recommends** rejected/quarantined facts (fixed 2026-07-06).

### Why publish is still blocked

1. **No approved facts** — governance publish requires ≥1 `Approved` or `Edited` fact.
2. **No Explorer-approved sources** — all 15 sources have `Approved for Explorer Use = No`.
3. **Mixed origins** — package rolls up Brand Provided + Public Web + Other sources (Honors pages, blogs, press).
4. **No trustworthy identity baseline** — existing identity rows are rejected wrong-brand values or absent; pending package lacks Curio/Hilton identity facts ready for approval.

---

## Clean Source Set

**First publish path = company-material Curio sources only (2 sources).**

| Record ID | Source title | Why included | Source category | Expected use | Risks / manual review notes |
|-----------|--------------|--------------|-----------------|--------------|----------------------------|
| `recy2pyEahF9UUsEk` | 2025 US Curio FDD | Curio-specific regulatory FDD; `Brand Provided`, `FDD`, High quality; company-materials path for governance | Company / regulatory (FDD) | Identity, development model, footprint, owner obligations — **re-extract** governance-priority fields | Current 6 facts are duplicate nonsense `be.footprint.globalHotels = "06"` — **do not approve existing rows**; verify PDF is Curio US FDD not cross-brand template |
| `recL1qfHCOAUZr9Rz` | Curio Collection Fact Sheet May 2026 | Official brand summary PDF; `Brand Provided`, High quality; primary positioning source | Company materials (fact sheet) | `be.positioning.*`, `be.identity.*`, `be.overview.typicalUseCase`, guest promise / tagline | **0 facts linked today** — extraction needed; confirm canonical row (duplicate `recMuN9bR1doJ3gjN` exists — use this ID only); spot-check Hilton parent attribution |

**Publish package scope after cleanup:** approve Explorer Use on **these two sources only** (not all 15). This resolves the mixed-origin blocker for the governance publish rollup.

---

## Excluded Source Set

**Exclude from first clean publish path.** Sources may remain in Source Library for internal research; they must not be in the governance publish rollup or Explorer-approved set until separately reviewed.

### Primary exclusion — contaminated extraction batch

| Record ID | Title | Why excluded |
|-----------|-------|--------------|
| `recIH5lyY8MASnfrp` | 2025 Mexico Curio FDD | **102 facts**; 24 Kimpton/IHG-contaminated (now Rejected); 78 pending unverified; wrong-brand extraction run suspected; unsafe for first publish |

### Hilton Honors / loyalty (not Curio-specific)

| Record ID | Title |
|-----------|-------|
| `rec9NH6nBLsHUlBqr` | Hilton Honors Program Overview |
| `recm0BO5Y7tyR732T` | Hilton Honors Program Overview (duplicate Website Capture) |
| `recslDSN2UNtENJTy` | Hilton Honors Points and Miles |

**Why excluded:** Hilton corporate loyalty content, not Curio brand identity/positioning; adds noise and public-origin mix.

### Press / corporate announcements

| Record ID | Title |
|-----------|-------|
| `recFqF8CETGBerPXC` | Hilton Honors 2026 Program Changes Press Release |
| `recnzI8rOX2xjpchb` | Hilton Honors 2026 Program Changes Press Release (duplicate) |
| `rectS4eaOgRfxgsKE` | Hilton Honors 2026 Press Release full capture |

**Why excluded:** Press/news; `Public Web` or duplicate Brand Provided captures; not Curio governance evidence.

### Third-party / points guides / competitor comparisons

| Record ID | Title |
|-----------|-------|
| `rec6opP76pLDVjDuP` | Best Hilton Hotels Points Value Guide 2026 |
| `recwCTH346cX1FRWu` | Prince of Travel Hilton Honors 2026 Analysis |
| `recOlxkapaiqtBe5R` | MT Luxury Marriott vs Hilton vs Hyatt Comparison |

**Why excluded:** Third-party analysis (`Source Origin = Other`); 36 low-value “Not confirmed” facts on points guide; wrong link suspected; not company-material.

### Hilton corporate development (non-Curio identity)

| Record ID | Title |
|-----------|-------|
| `recR0bSGVPmzuVSnl` | Hilton Develop APAC Brochure |
| `recstXfpXJmYNIrnz` | Hilton Develop APAC Brochure (duplicate, Public Web) |

**Why excluded:** Hilton parent development context; duplicate rows; APAC brochure may support development model later but not first identity publish; Public Web duplicate worsens origin conflict.

### Duplicates (defer to canonical clean sources)

| Record ID | Title | Note |
|-----------|-------|------|
| `recMuN9bR1doJ3gjN` | Curio Collection Fact Sheet May 2026 | Duplicate of `recL1qfHCOAUZr9Rz` — do not double-count in publish package |
| Unlinked `recA7VOMWhmTtiklj` | Curio Collection Fact Sheet May 2026 | Missing brand link — link or merge before use |
| Unlinked `recW4sJlpnCdZAZJ0` | 2025 Mexico Curio FDD | Missing brand link — **do not link** until Mexico batch is clean |

---

## Target Facts to Re-Extract

Re-extract from **clean source set only**. Prioritize fields in `BRAND_GOVERNANCE_FIELD_KEYS` and governance publish proposal.

### Identity

| Field key | Priority | Expected source |
|-----------|----------|-----------------|
| `be.identity.brandName` | **P0** | US FDD + fact sheet |
| `be.identity.parentCompany` | **P0** | US FDD + fact sheet (Hilton / Hilton Worldwide) |

### Positioning

| Field key | Priority | Expected source |
|-----------|----------|-----------------|
| `be.positioning.summary` | **P0** | Fact sheet (primary) |
| `be.positioning.tagline` | **P1** | Fact sheet |
| `be.positioning.guestPromise` | **P1** | Fact sheet |

### Overview / development

| Field key | Priority | Expected source |
|-----------|----------|-----------------|
| `be.overview.typicalUseCase` | **P1** | Fact sheet / US FDD |
| `be.development.conversionRelevance` | **P2** | US FDD (if clearly stated) |
| `be.development.ownerConsiderations` | **P2** | US FDD (if clearly stated) |

### Footprint / region

| Field key | Priority | Expected source |
|-----------|----------|-----------------|
| `be.footprint.globalHotels` | **P2** | US FDD (only if numeric/text is meaningful — reject `"06"` dupes) |
| `be.footprint.regionalPresence` | **P2** | US FDD / fact sheet **if supported** in brand registry |

### Standards / owner considerations

- Extract **only if clearly source-backed** with evidence quotes.
- Defer loyalty, fees, and commercial-engine fields until identity + positioning are approved.
- Do **not** re-extract `be.loyalty.*` from Hilton Honors sources in this path.

---

## Manual Review Rules

Before approving any fact from the clean re-extraction run:

1. **Reject wrong-brand values** — do not approve facts mentioning **Kimpton**, **IHG**, **IHG One Rewards**, or parent companies other than Hilton.
2. **Reject non-answers** — do not approve `"Not confirmed in available sources."` or equivalent gap copy.
3. **Avoid duplicate approvals** — do not approve multiple rows for the same field key unless they strengthen evidence; prefer one best row per field.
4. **Prioritize identity + positioning** over footprint-only facts.
5. **Approve only 3–8 facts** after human evidence review — not bulk approve.
6. **Do not set Company Validated** or **Company Validation Date** on Brand Basics.
7. **Do not set External Display Status / Show Trust Label** directly on Setup — profile governance publish script only after re-audit passes.
8. **Verify evidence text** — every approved fact must quote the US FDD or fact sheet, not Kimpton boilerplate.
9. **Mexico FDD pending facts** — treat remaining 78 pending rows from `recIH5lyY8MASnfrp` as **out of scope** for first publish; reject or hold.

---

## Proposed Stewardship Sequence

Execute in order. **Dry-run by default** at every automation step.

| Step | Action | Command / artifact |
|------|--------|-------------------|
| 1 | **Re-extract** Curio facts from clean source set only | `npm run curio-clean-reextract -- --dry-run` (preview); apply only with founder approval |
| 2 | **Review** extracted facts for Curio/Hilton correctness | Airtable Extracted Facts + `CONTENT_QA_CHECKLIST.md` |
| 3 | **Approve** only selected clean facts (3–8) | Manual `Human Review Status` → Approved/Edited |
| 4 | **Approve Explorer Use** on clean sources only (`recy2pyEahF9UUsEk`, `recL1qfHCOAUZr9Rz`) | Manual or future explicit stewardship `--approve-source-ids` after human sign-off |
| 5 | **Re-run publish readiness audit** | `npm run audit-partner-intelligence-publish-readiness` |
| 6 | **Stewardship dry-run** (narrow source IDs when applying later) | `npm run steward-partner-intelligence -- --entity-type brand --target-rec-id receQkxgjlezsc1xg --dry-run` |
| 7 | **Profile governance publish dry-run only** (if eligible) | `npm run publish-partner-intelligence-profile-governance -- --entity-type brand --target-rec-id receQkxgjlezsc1xg --dry-run` |

**Never run** bulk stewardship apply across all 15 sources. When using the stewardship assistant for apply (future), pass explicit narrow lists:

```bash
# Example — only after human review; NOT for this planning task
npm run steward-partner-intelligence -- --apply --approve-stewardship \
  --entity-type brand --target-rec-id receQkxgjlezsc1xg \
  --approve-source-ids "recy2pyEahF9UUsEk,recL1qfHCOAUZr9Rz" \
  --approve-fact-ids "rec...,rec..."
```

---

## Expected Governance Outcome

**If** the clean company-material path succeeds and re-audit shows `eligible: true`:

### Internal (Brand Basics profile governance fields)

| Field | Expected value |
|-------|----------------|
| Validation Status | **Company Published** (internal — official company FDD/fact sheet after review) |
| Usage Permission | Platform Display Allowed |
| Source Region | Global Reference (or steward-selected region) |
| Confidence Level | Medium or High (internal `Data Confidence Level` only) |

### External Explorer trust chip

| Element | Expected display |
|---------|------------------|
| Chip label | **AI-Assisted Profile** |
| Source Basis | **Company Materials** |
| Region | Global Reference (or relevant region) |
| Subtitle | Last Reviewed · Source Basis · Region — **no confidence in subtitle** |

### Never (Curio first publish)

- **Company-Reviewed Profile**
- **Company-Validated Profile**
- Raw “Company Published” as external chip text

---

## Clean Re-Extraction Script

**Implemented:** `npm run curio-clean-reextract` → `scripts/curio-clean-reextract.mjs`

| Mode | Command |
|------|---------|
| Dry-run (default) | `npm run curio-clean-reextract -- --dry-run` |
| Apply (founder approval required) | `npm run curio-clean-reextract -- --apply --approve-curio-clean-reextract` |

Reports: `reports/curio-clean-reextract.{md,json}`

- Hard-coded source allowlist: `recy2pyEahF9UUsEk`, `recL1qfHCOAUZr9Rz` only
- Dry-run uses read-only `extractFromBrandSourceDocument` preview — **no Airtable writes**
- Apply writes Pending facts + source extraction run notes only — does not approve sources/facts or touch Setup governance

## Open Implementation Question (resolved)

### Does the repo have a safe PI re-extraction tool for narrow Curio sources?

**Yes — `npm run curio-clean-reextract` (dry-run default).**

| Tool | Path | Dry-run safe? | Narrow sources? | Notes |
|------|------|---------------|-----------------|-------|
| **Curio clean re-extract** | `npm run curio-clean-reextract` | **Yes** — previews extraction without writes | **Yes** — hard-coded 2-source allowlist | Reports: `reports/curio-clean-reextract.{md,json}` |
| Curio brand pipeline | `node scripts/curio-brand-source-pipeline.mjs` | **No** | **No** — all brand sources | Do not use for clean path |
| PI extract smoke | `npm run partner-intelligence:extract-smoke` | Yes (no-op) | Single source (Arbor default) | Not Curio-configured |
| Per-source extraction API | `POST /api/partner-intelligence/extraction/run` | No | Yes | Writes to Airtable |
| Core library | `lib/partner-intelligence/run-extraction.js` | Preview via exported `extractFromBrandSourceDocument` | Per-source when called from clean script | `createFactsFromMerged` writes — only on apply path |

### Safest workflow today

1. `npm run curio-clean-reextract -- --dry-run` — review preview + contamination warnings.
2. Founder approves apply.
3. `npm run curio-clean-reextract -- --apply --approve-curio-clean-reextract`
4. `npm run steward-partner-intelligence -- --entity-type brand --target-rec-id receQkxgjlezsc1xg --dry-run`
5. Manually review + approve clean facts only.

**Do not run** `curio-brand-source-pipeline.mjs` for the clean publish path.

**Extraction context fix (2026-07-06):** Kimpton/IHG blind registry fallback removed — see [curio-extraction-context-audit.md](./curio-extraction-context-audit.md). Re-run `npm run curio-clean-reextract -- --dry-run` before apply.

---

## Related Commands

```bash
# Clean re-extract preview (dry-run default — safe)
npm run curio-clean-reextract -- --dry-run

# Readiness + stewardship (dry-run — safe)
npm run audit-partner-intelligence-publish-readiness
npm run steward-partner-intelligence -- --entity-type brand --target-rec-id receQkxgjlezsc1xg --dry-run

# Quarantine (completed for 24 facts; dry-run for verification)
npm run quarantine-curio-pi-facts -- --dry-run

# Do NOT run broad pipeline for clean path
# node scripts/curio-brand-source-pipeline.mjs
```

---

## Change Impact

| Tier | Classification |
|------|----------------|
| This document | **Low** — documentation only |
| Future re-extraction script | **High** — PI fact creation, source status patches |
| Future fact/source approvals | **High** — governance publish prerequisites |

**Rollback:** N/A for this document. Future extraction apply: reject new facts by `Extraction Run ID`; revert source `Extraction Run ID` if needed.

---

## Regression Checklist (when execution begins)

- [ ] Re-extracted facts show Curio/Hilton identity — not Kimpton/IHG
- [ ] Only 2 sources approved for Explorer Use in first publish path
- [ ] Mixed-origin blocker cleared in re-audit
- [ ] ≥1 approved fact; ideally 3–8 governance fields
- [ ] Stewardship dry-run recommends no rejected/quarantined facts
- [ ] Profile governance publish dry-run shows AI-Assisted Profile · Company Materials
- [ ] Company Validated remains false
