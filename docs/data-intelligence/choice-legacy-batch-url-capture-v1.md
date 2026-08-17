# Choice Legacy Mini-Batch URL Capture v1

**Date:** 2026-07-06  
**Status:** Dry-run default; batch apply with explicit approval  
**Brands:** Comfort Inn & Suites, Everhome Suites, Quality Inn (mini-batch 1)

> **Authority:** [choice-legacy-brand-mini-batch-1.md](./choice-legacy-brand-mini-batch-1.md), [choice-legacy-batch-source-stewardship-v1.md](./choice-legacy-batch-source-stewardship-v1.md)

---

## Purpose

After local PDF registration and batch PDF stewardship, capture and register **official Choice consumer and press/media pages** for all three mini-batch brands in one controlled run — instead of six separate `partner-reference:download` commands.

Registers **Source Library rows only** — not facts, not governance, not Explorer content.

---

## URLs included (v1)

| Brand | Consumer | Press / media |
|-------|----------|---------------|
| Comfort Inn & Suites | `https://www.choicehotels.com/comfort-hotels` | `https://media.choicehotels.com/comfort-press-kit` |
| Everhome Suites | `https://www.choicehotels.com/everhome-suites` | `https://media.choicehotels.com/everhome-suites` |
| Quality Inn | `https://www.choicehotels.com/quality-inn` | `https://media.choicehotels.com/quality-press-kit` |

**Excluded from v1:** development URLs (`choicehotelsdevelopment.com`) — JS-shell provenance only; capture later if needed.

---

## Registration defaults

| Field | Value |
|-------|-------|
| Status | Captured |
| Approved for Explorer Use? | No |
| Approved for Extraction? | No |

Duplicate check runs against existing Source Library rows (URL + title) before register.

---

## Commands

```bash
# Dry-run (default) — probes URLs, checks duplicates, no disk/Airtable writes
npm run choice-legacy-batch-url-capture -- --dry-run

# Single brand
npm run choice-legacy-batch-url-capture -- --dry-run --brand comfort-inn-suites

# Batch apply (all six URLs)
npm run choice-legacy-batch-url-capture -- --apply --approve-choice-legacy-batch-url-capture
```

Reports: `reports/choice-legacy-batch-url-capture.{md,json}`

---

## Order of operations

1. Local PDFs registered (`choice-legacy-brand-source-package-batch --apply`)
2. PDF batch stewardship applied (`choice-legacy-batch-source-stewardship --apply`)
3. **This workflow** — batch URL capture dry-run → review → apply
4. Re-run batch stewardship for URL sources (Explorer Yes, Extraction No)
5. **Do not** extract facts until founder approves per-brand extraction

---

## Next after capture

```bash
npm run choice-legacy-batch-source-stewardship -- --dry-run
npm run choice-legacy-batch-source-stewardship -- --apply --approve-choice-legacy-batch-stewardship
```

---

## Does not do

- Rebuild Explorer content / overwrite Brand Setup fields
- Capture development URLs in v1
- Extract facts / approve facts / publish governance
- Set Company Validated or Company Validation Date
- Auto-approve URL sources
- Change UI, scoring, BAS, OAS, OCS, Deal Readiness, or schema
