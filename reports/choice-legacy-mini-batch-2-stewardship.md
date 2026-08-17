# Choice Legacy Batch Source Stewardship v1

Generated: 2026-07-06T23:13:43.593Z
Mode: **apply**
Airtable modified: **yes**

> stewardship-package.js buildSafeSourcePatch does not set Approved for Extraction?; this batch workflow adds it for eligible local PDFs and official readable Choice consumer/press captures.

## Executive summary

| Metric | Count |
|--------|------:|
| Brands | 4 |
| Sources found | 11 |
| Eligible for batch approval | 11 |
| Skipped | 0 |

### Blockers (aggregate)

- none

### Batch apply command

```bash
npm run choice-legacy-batch-source-stewardship -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-stewardship
```

### Per-brand fallback

```bash
npm run choice-legacy-batch-source-stewardship -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-stewardship --brand country-inn-suites-choice
```

```bash
npm run choice-legacy-batch-source-stewardship -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-stewardship --brand radisson-choice
```

```bash
npm run choice-legacy-batch-source-stewardship -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-stewardship --brand radisson-individuals-choice
```

```bash
npm run choice-legacy-batch-source-stewardship -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-stewardship --brand radisson-red-choice
```

### Next after batch approval

- **mini-batch-2**: Approve official Choice consumer/press sources (Extraction Yes when readable). Do not extract facts until founder approves per brand.
  ```bash
npm run choice-legacy-batch-source-stewardship -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-stewardship
  ```

## Brands

### Country Inn & Suites by Choice

- Record: `recaayt9u7YYg8h7Y`
- Sources found: **2**
- Eligible: **2** · Skipped: **0**
- Expected primary PDF: `Choice Hotels International/Country Inn & Suites/Country Inn & Suites Prototype Brochure.pdf`

| Source ID | Title | Type | Status | Explorer Use | Extraction | Role | Eligible | Recommendation |
|-----------|-------|------|--------|--------------|------------|------|----------|----------------|
| `recOx0YuUUOfaLPBe` | Country Inn & Suites by Choice — Choice consumer brand page | Brand Page | Captured | No | No | consumer_page | yes | approve_explorer_use_status_and_extraction |
| `recPqQVEe01xl3aQ6` | Country Inn & Suites by Choice — prototype brochure (local) | Development Brochure | Captured | No | No | mini_batch_primary_pdf | yes | approve_explorer_use_status_and_extraction |

### Radisson by Choice

- Record: `recywbx1YQSTCPqW1`
- Sources found: **3**
- Eligible: **3** · Skipped: **0**
- Expected primary PDF: `Choice Hotels International/Radisson/1. Brand Book - RD.pdf`

| Source ID | Title | Type | Status | Explorer Use | Extraction | Role | Eligible | Recommendation |
|-----------|-------|------|--------|--------------|------------|------|----------|----------------|
| `recLsN4M2G1z0rJBa` | Radisson by Choice — brand book (local) | Development Brochure | Captured | No | No | mini_batch_primary_pdf | yes | approve_explorer_use_status_and_extraction |
| `recdOL9QhOIrAxYRP` | Radisson by Choice — Choice press kit / media center | Press Release | Captured | No | No | press_kit | yes | approve_explorer_use_status_and_extraction |
| `recsnDjbEjI5yCxmm` | Radisson by Choice — Choice consumer brand page | Brand Page | Captured | No | No | consumer_page | yes | approve_explorer_use_status_and_extraction |

### Radisson Individuals by Choice

- Record: `recRyvM8OmLlDj9G7`
- Sources found: **3**
- Eligible: **3** · Skipped: **0**
- Expected primary PDF: `Choice Hotels International/Radisson Individuals/RADIN_PitchDeck_PPT_New_Final.pdf`

| Source ID | Title | Type | Status | Explorer Use | Extraction | Role | Eligible | Recommendation |
|-----------|-------|------|--------|--------------|------------|------|----------|----------------|
| `reccfAdMmZI5XmRJK` | Radisson Individuals by Choice — Choice press kit / media center | Press Release | Captured | No | No | press_kit | yes | approve_explorer_use_status_and_extraction |
| `recgDFeovQZZuiXZ8` | Radisson Individuals by Choice — Choice consumer brand page | Brand Page | Captured | No | No | consumer_page | yes | approve_explorer_use_status_and_extraction |
| `recin2kwFrIlQNKmp` | Radisson Individuals by Choice — pitch deck (local) | Development Brochure | Captured | No | No | mini_batch_primary_pdf | yes | approve_explorer_use_status_and_extraction |

### Radisson RED by Choice

- Record: `recmKqo7M7mLZgRqQ`
- Sources found: **3**
- Eligible: **3** · Skipped: **0**
- Expected primary PDF: `Choice Hotels International/Radisson RED/Upscale by Choice brand overview guide.pdf`

| Source ID | Title | Type | Status | Explorer Use | Extraction | Role | Eligible | Recommendation |
|-----------|-------|------|--------|--------------|------------|------|----------|----------------|
| `recPrdF1bltJtq4JS` | Radisson RED by Choice — Choice consumer brand page | Brand Page | Captured | No | No | consumer_page | yes | approve_explorer_use_status_and_extraction |
| `rechXybBKQsqTIsCz` | Radisson RED by Choice — Choice press kit / media center | Press Release | Captured | No | No | press_kit | yes | approve_explorer_use_status_and_extraction |
| `recz8fmzzxvsP6V6J` | Radisson RED by Choice — Upscale by Choice brand overview (local) | Development Brochure | Captured | No | No | mini_batch_primary_pdf | yes | approve_explorer_use_status_and_extraction |

## Apply result

- Applied: **11**
- Skipped: **0**
- Errors: **0**

## Does not do

- Rebuild Explorer content or overwrite Brand Setup fields
- Approve or extract facts
- Publish governance or set Company Validated
- Approve development-page sources (JS-shell provenance only)
- Approve RHG/global or uncertain third-party sources
