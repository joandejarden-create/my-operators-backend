# ADP Monthly Review artifact recovery — 2026-10-01

**READ-ONLY generation / no regeneration.** Artifacts restored from filesystem backup only.  
No Airtable writes, Surfe, paid providers, deploy, merge, or git clean/reset.

## Trace: `review_artifact_missing`

**Source:** `lib/ai-demand-positioning/monthly-review/admin/published-review-coverage-v1.js` → `classifyCoverage()`

| Condition | `coverageStatus` | `coverageReason` |
|-----------|------------------|------------------|
| Not eligible | `BLOCKED` | eligibility blockers |
| Eligible + no archive row for property | `NEEDS_REBUILD` | **`review_artifact_missing`** |
| Archive exists but `pdfStatus` ≠ `READY` | `NEEDS_REBUILD` | `pdf_missing_or_pending` |
| Archive + PDF READY | `READY` | null |

Admin UI (`public/js/admin-ai-demand-reviews.js`) treats `NEEDS_REBUILD` / `PENDING_PDF` as “Generating” / Needs Rebuild.

### Dependency chain

```
Admin Reviews tab
  → GET admin coverage (buildPublishedAdpReviewCoverageV1)
    → listPublishedPropertyIds()  [published ADP universe]
    → listArchiveReviews()        [reports/.../monthly-review/archive/index.json]
    → latestArchiveByProperty()   [prefer newest PDF-READY review]
    → loadReviewPayload()         [review.json → actionAgenda count]
    → reviewArtifactPaths()       [review.json, metadata.json, meeting.json, report.pdf, fingerprints, generation-log, review-render.json]
```

### Expected artifact package (per review)

| Piece | Path pattern |
|-------|----------------|
| Index entry | `reports/ai-demand-positioning/monthly-review/archive/index.json` |
| Review JSON | `.../archive/{propertyId}/{yyyy-mm}/{reviewId}/review.json` |
| Metadata | `.../metadata.json` (status, fingerprints, qualityGates) |
| Meeting pack | `.../meeting.json` |
| PDF | `.../report.pdf` |
| Render meta | `.../review-render.json` |
| Fingerprints | `.../fingerprints.json` |
| Generation log | `.../generation-log.json` |
| Golden fixtures (Cambridge/NOHO only) | `fixtures/ai-demand-positioning/monthly-review/*.json` |
| Golden PDF mirrors | `reports/.../monthly-review/pdf/*.pdf` |
| Cover/page PNGs (parity QA) | `reports/.../monthly-review/pdf-pages/**/cover.png` |

Actions come from `review.actionAgenda` inside `review.json` (not a separate action-register file for archived reviews). Empty fixture `action-register-empty-v1.json` is a golden helper only.

---

## Pre-recovery inventory

| Metric | Value |
|--------|-------|
| Published universe | **19** |
| READY | **2** (Cambridge beaches v1 golden-register, NOW NOW NOHO v1) |
| NEEDS_REBUILD (`review_artifact_missing`) | **17** |
| BLOCKED | **0** |
| Current archive index reviews | **2** |
| Current `report.pdf` count | **2** |

---

## Pre-crash source found

**Primary source:** `C:\Dev\Backup-Staging\2026-10-01_0200\deal-capture-proxy\reports\ai-demand-positioning\monthly-review\`

| Backup metric | Value |
|---------------|-------|
| Index reviews | **136** (`updatedAt` 2026-09-11) |
| Properties with READY PDF | **15** |
| `report.pdf` files | **131** |

Also checked (incomplete / post-loss): `dealality-backups\LATEST`, pre-crash recovery freezes — monthly-review archive largely absent there.

---

## Restore action (no regeneration)

1. Safety snap of then-current tiny archive → `C:\Dev\Backup-Scripts\dealality-snapshots\pre-restore-monthly-review-archive-*`
2. `robocopy` full tree:
   - Source: `Backup-Staging\2026-10-01_0200\...\monthly-review\`
   - Dest: `deal-capture-proxy\reports\ai-demand-positioning\monthly-review\`
3. Result: **1107 files / ~68.7 MB** copied (robocopy exit 1 = success with copies)

Statuses/IDs/timestamps taken from restored `metadata.json` + `index.json` — not fabricated.

---

## Post-recovery coverage

| Metric | Value |
|--------|-------|
| Published | **19** |
| READY | **15** |
| NEEDS_REBUILD | **4** |
| BLOCKED | **0** |
| Index reviews | **136** |
| Archive PDFs | **131** |

### Restored READY (15) — PDF + review.json hash-identical to Backup-Staging

All **15/15** current `report.pdf` and `review.json` SHA-256 match backup byte-for-byte.

| Property | Review ID | Actions | Status |
|----------|-----------|---------|--------|
| Bethesda Marriott | `..._v13` | 4 | READY_FOR_REVIEW / SUCCESS / READY |
| Cambridge Beaches | `..._v14` | 3 | READY_FOR_REVIEW / SUCCESS / READY |
| Casas del XVI | `..._v8` | 5 | READY_FOR_REVIEW / SUCCESS / READY |
| Faranda Collection Bogotá | `..._v8` | 5 | READY_FOR_REVIEW / SUCCESS / READY |
| Hotel Caribe Faranda Grand | `..._v9` | 5 | READY_FOR_REVIEW / SUCCESS / READY |
| Hotel Phillips KC | `..._v8` | 5 | READY_FOR_REVIEW / SUCCESS / READY |
| JW Marriott Monterrey Valle | `..._v8` | 2 | READY_FOR_REVIEW / SUCCESS / READY |
| JW Marriott Santo Domingo | `..._v8` | 1 | READY_FOR_REVIEW / SUCCESS / READY |
| NOW NOW NOHO | `..._v11` | 5 | READY_FOR_REVIEW / SUCCESS / READY |
| Radisson Santo Domingo | `..._v8` | 5 | READY_FOR_REVIEW / SUCCESS / READY |
| Renaissance Times Square | `..._v8` | 5 | READY_FOR_REVIEW / SUCCESS / READY |
| St. Regis Cap Cana | `..._v9` | 2 | READY_FOR_REVIEW / SUCCESS / READY |
| St. Regis Mexico City | `..._v8` | 1 | READY_FOR_REVIEW / SUCCESS / READY |
| Waterstone Boca Raton | `..._v8` | 4 | READY_FOR_REVIEW / SUCCESS / READY |
| Westin Monterrey Valle | `..._v8` | 3 | READY_FOR_REVIEW / SUCCESS / READY |

### Genuine rebuild required (4) — class G: never present in pre-crash archive

| Property | Classification | Evidence |
|----------|----------------|----------|
| `adp_ac_hotel_a_coruna` | G — never generated (archive) | No `archive/adp_ac_hotel_a_coruna` in Backup-Staging; published snapshot exists |
| `adp_hilton_times_square` | G — never generated (archive) | No archive folder in Backup-Staging |
| `adp_spice_island_beach_resort` | G — never generated (archive) | No archive folder in Backup-Staging |
| `adp_w_rome` | G — never generated (archive) | No archive folder in Backup-Staging |

These still correctly show `NEEDS_REBUILD` / `review_artifact_missing` until a real generate pass (out of scope — do not regenerate in this recovery).

---

## Cover / PDF visual assets

Restored under `reports/ai-demand-positioning/monthly-review/pdf-pages/`:
- `cambridge-template-parity-20260911/cover.png` (+ executive/actions/…)
- `bethesda-template-parity-20260911/cover.png` (+ …)
- Template-parity PDF pack under `pdf/template-parity-20260911/`
- Per-version PDF copies under `pdf/`

Hash gate for admin-served latest archive PDFs: **15/15 IDENTICAL** to Backup-Staging.

---

## Git vs filesystem backup classification (permanent rule)

| Class | What | Track in Git? | Protect how |
|-------|------|---------------|-------------|
| **A. PRODUCT FIXTURES** | Golden Cambridge + NOHO review JSON under `fixtures/ai-demand-positioning/monthly-review/`; empty action-register helper | **YES** (already tracked) | Git |
| **B. GENERATED CUSTOMER ARTIFACTS** | Full `reports/ai-demand-positioning/monthly-review/archive/**` (review.json, report.pdf, metadata, index), bulk `pdf/`, `pdf-pages/` | **NO** (large, customer/runtime) | **Filesystem + cloud backup must include this tree** — `Backup-Dealality.ps1` already copies working tree excluding only `node_modules`/build caches (archive is included when present) |
| **C. LOCAL CACHE** | Transient render temp, Playwright dumps | NO | Disposable |

**Root cause of disappearance:** archive lived only on disk; working tree lost it during crash recovery; LATEST backup was taken after loss; Staging `2026-10-01_0200` still held the complete package.

**Permanent rule:** completed Monthly Review archive under `reports/ai-demand-positioning/monthly-review/archive/` is **filesystem-backup-critical**. Never treat missing archive as “needs rebuild” until Staging/LATEST/OneDrive archives are searched. Prefer restore over regenerate.

---

## Git commit

No source/fixture code changes required for this restore.  
HEAD remains: `caad4a37d1cd42d44ca0338f756d9cce0e77b30c`  
Restored files are **untracked/generated reports** (class B).

Machine JSON: `reports/adp-monthly-review-artifact-recovery-20261001.json`

---

## QA checklist (human)

1. Hard-refresh `http://localhost:8080/app/#/admin/ai-demand?tab=reviews`
2. Confirm 15 rows: READY_FOR_REVIEW / SUCCESS / READY / View PDF / non-zero Actions (where agenda > 0)
3. Confirm 4 rows still Needs Rebuild: AC A Coruña, Hilton Times Square, Spice Island, W Rome
4. Open 2–3 View PDF links; confirm cover branding + property name + September 2026

---

READY FOR CHATGPT QA — ADP MONTHLY REVIEW ARTIFACTS RECOVERED
