# Choice Legacy Batch Source Stewardship v1

Generated: 2026-07-06T22:12:12.178Z
Mode: **dry_run**
Airtable modified: **no**

> stewardship-package.js buildSafeSourcePatch does not set Approved for Extraction?; this batch workflow adds it for eligible local PDFs and official readable Choice consumer/press captures.

## Executive summary

| Metric | Count |
|--------|------:|
| Brands | 3 |
| Sources found | 9 |
| Eligible for batch approval | 0 |
| Skipped | 9 |

### Blockers (aggregate)

- already_fully_approved

### Batch apply command

```bash
npm run choice-legacy-batch-source-stewardship -- --apply --approve-choice-legacy-batch-stewardship
```

### Per-brand fallback

```bash
npm run choice-legacy-batch-source-stewardship -- --apply --approve-choice-legacy-batch-stewardship --brand comfort-inn-suites
```

```bash
npm run choice-legacy-batch-source-stewardship -- --apply --approve-choice-legacy-batch-stewardship --brand everhome-suites
```

```bash
npm run choice-legacy-batch-source-stewardship -- --apply --approve-choice-legacy-batch-stewardship --brand quality-inn
```

### Next after batch approval

- **mini-batch-1**: Approve six official Choice consumer/press sources (Extraction Yes when readable). Do not extract facts until founder approves per brand.
  ```bash
npm run choice-legacy-batch-source-stewardship -- --apply --approve-choice-legacy-batch-stewardship
  ```

## Brands

### Comfort Inn & Suites

- Record: `recOzH5iAE1xEjyD0`
- Sources found: **3**
- Eligible: **0** · Skipped: **3**
- Expected primary PDF: `Choice Hotels International/Comfort Inn/brochure--comfort-inn.pdf`

| Source ID | Title | Type | Status | Explorer Use | Extraction | Role | Eligible | Recommendation |
|-----------|-------|------|--------|--------------|------------|------|----------|----------------|
| `recRbi8CjS8BVt4Z3` | Comfort Inn & Suites — Choice press kit / media center | Press Release | Approved | Yes | Yes | press_kit | no | no_op_already_approved |
| | blockers: already_fully_approved | | | | | | | |
| `recZFPfGRo5C9FF2Q` | Comfort Inn & Suites — Choice development brochure (local) | Development Brochure | Approved | Yes | Yes | mini_batch_primary_pdf | no | no_op_already_approved |
| | blockers: already_fully_approved | | | | | | | |
| `recxm2Jxqvi2n2I8K` | Comfort Inn & Suites — Choice consumer brand page | Brand Page | Approved | Yes | Yes | consumer_page | no | no_op_already_approved |
| | blockers: already_fully_approved | | | | | | | |

### Everhome Suites

- Record: `recqkkrsevi4r9ibj`
- Sources found: **3**
- Eligible: **0** · Skipped: **3**
- Expected primary PDF: `Choice Hotels International/Everhome Suites/Everhome Suites_Franchise Development Presentation.pdf`

| Source ID | Title | Type | Status | Explorer Use | Extraction | Role | Eligible | Recommendation |
|-----------|-------|------|--------|--------------|------------|------|----------|----------------|
| `rec28KQ9ubpynVfTq` | Everhome Suites — Choice consumer brand page | Brand Page | Approved | Yes | Yes | consumer_page | no | no_op_already_approved |
| | blockers: already_fully_approved | | | | | | | |
| `rechRqlbx7DF4YCCV` | Everhome Suites — franchise development presentation (local) | Development Brochure | Approved | Yes | Yes | mini_batch_primary_pdf | no | no_op_already_approved |
| | blockers: already_fully_approved | | | | | | | |
| `rechbWISi8BQwTqGb` | Everhome Suites — Choice press kit / media center | Press Release | Approved | Yes | Yes | press_kit | no | no_op_already_approved |
| | blockers: already_fully_approved | | | | | | | |

### Quality Inn

- Record: `recd8o4k1JddhkRWW`
- Sources found: **3**
- Eligible: **0** · Skipped: **3**
- Expected primary PDF: `Choice Hotels International/Quality Inn/brochure--quality-inn.pdf`

| Source ID | Title | Type | Status | Explorer Use | Extraction | Role | Eligible | Recommendation |
|-----------|-------|------|--------|--------------|------------|------|----------|----------------|
| `recfh3rpBaKo0U0H1` | Quality Inn — Choice press kit / media center | Press Release | Approved | Yes | Yes | press_kit | no | no_op_already_approved |
| | blockers: already_fully_approved | | | | | | | |
| `recmEnl9wcLfSA4Mk` | Quality Inn — Choice development brochure (local) | Development Brochure | Approved | Yes | Yes | mini_batch_primary_pdf | no | no_op_already_approved |
| | blockers: already_fully_approved | | | | | | | |
| `recpsFcGtpvib16s0` | Quality Inn — Choice consumer brand page | Brand Page | Approved | Yes | Yes | consumer_page | no | no_op_already_approved |
| | blockers: already_fully_approved | | | | | | | |

## Does not do

- Rebuild Explorer content or overwrite Brand Setup fields
- Approve or extract facts
- Publish governance or set Company Validated
- Approve development-page sources (JS-shell provenance only)
- Approve RHG/global or uncertain third-party sources
