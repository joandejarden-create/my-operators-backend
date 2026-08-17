# Choice Legacy Brand Mini-Batch 1

**Date:** 2026-07-06  
**Status:** Dry-run workflow implemented  
**Brands:** Comfort Inn & Suites, Everhome Suites, Quality Inn

> **Authority:** [choice-legacy-brand-source-package-v1.md](./choice-legacy-brand-source-package-v1.md), [active-brand-governance-upgrade-v1.md](./active-brand-governance-upgrade-v1.md)

---

## Purpose

First controlled governance-upgrade mini-batch for three Explorer-active Choice brands with local PDFs on disk. Registers/captures **P0 source packages only** — no extraction, no stewardship apply, no governance publish.

---

## Brands

| Brand | Record ID | Primary local PDF |
|-------|-----------|-------------------|
| Comfort Inn & Suites | `recOzH5iAE1xEjyD0` | `Comfort Inn/brochure--comfort-inn.pdf` |
| Everhome Suites | `recqkkrsevi4r9ibj` | `Everhome Suites/Everhome Suites_Franchise Development Presentation.pdf` |
| Quality Inn | `recd8o4k1JddhkRWW` | `Quality Inn/brochure--quality-inn.pdf` |

---

## P0 package per brand

1. **Local development PDF** — register on apply (primary extraction evidence)
2. **Consumer brand page** — capture via `partner-reference:download`
3. **Press kit** — capture via `partner-reference:download`
4. **Development page** — provenance only when JS-shell risk; PDF replaces for extract

---

## Commands

```bash
# Dry-run (default)
npm run choice-legacy-brand-source-package-batch -- --dry-run

# Single brand
npm run choice-legacy-brand-source-package-batch -- --dry-run --brand comfort-inn-suites

# Apply local PDFs only (after founder review)
npm run choice-legacy-brand-source-package-batch -- --apply --approve-choice-legacy-batch-source-register
```

Reports: `reports/choice-legacy-brand-mini-batch-1.{md,json}`

---

## Order of operations

1. Run dry-run batch report
2. Apply local PDF registration (explicit approval)
3. Capture consumer + press URLs (`--apply --register` per brand after dry-run)
4. Optional development URL capture (provenance; not primary extract)
5. `steward-partner-intelligence --dry-run` per brand
6. **Do not** extract or publish governance until sources stewarded

---

## Does not do

- Rebuild Explorer content / overwrite Setup fields
- Extract facts / approve sources / publish governance
- Register uncertain URLs without capture
- Set Company Validated
