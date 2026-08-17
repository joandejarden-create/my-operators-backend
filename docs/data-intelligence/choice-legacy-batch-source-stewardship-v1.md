# Choice Legacy Batch Source Stewardship v1

**Date:** 2026-07-06  
**Status:** Dry-run default; batch apply with explicit approval  
**Brands:** Comfort Inn & Suites, Everhome Suites, Quality Inn (mini-batch 1); Country Inn & Suites by Choice, Radisson by Choice, Radisson Individuals by Choice, Radisson RED by Choice (mini-batch 2)

**Batch flag:** `--batch mini-batch-1` (default) or `--batch mini-batch-2`. Manifest: `lib/partner-intelligence/choice-legacy-batch-config.js`.

> **Authority:** [choice-legacy-brand-mini-batch-1.md](./choice-legacy-brand-mini-batch-1.md), [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md)

---

## Purpose

Batch stewardship for three Choice legacy brands sharing the same source pattern. Avoids brand-by-brand manual dry-run review when local PDFs are identical risk profile.

Approves **Source Library rows only** — not facts, not governance.

---

## Eligible sources (batch auto-approve)

| Role | Approval |
|------|----------|
| **mini_batch_primary_pdf** | Status → Approved · Explorer Use → Yes · **Extraction → Yes** |
| **consumer_page** (official Choice brand page) | Status → Approved · Explorer Use → Yes · **Extraction → Yes** when local HTML capture is readable (≥200 chars) |
| **press_kit** (official `media.choicehotels.com`) | Status → Approved · Explorer Use → Yes · **Extraction → Yes** when readable and URL matches brand press kit |
| development_provenance | **Skipped** — JS-shell; provenance only; Extraction No |
| third-party / uncertain | **Manual review** — not batch-approved |

Blockers: wrong brand link, stale/rejected, RHG/global URLs, URL mismatch vs expected consumer/press URL, unreadable PDF, already fully approved.

---

## Extraction field note

`stewardship-package.js` `buildSafeSourcePatch` does **not** write `Approved for Extraction?`. This batch workflow adds it for **eligible local PDFs** and **official readable Choice consumer/press captures** using `MAP_PARTNER_SOURCE.approvedForExtraction`.

---

## Commands

```bash
# Dry-run (default)
npm run choice-legacy-batch-source-stewardship -- --batch mini-batch-2 --dry-run

# Batch apply (all brands in batch)
npm run choice-legacy-batch-source-stewardship -- --batch mini-batch-1 --apply --approve-choice-legacy-batch-stewardship

# Per-brand fallback
npm run choice-legacy-batch-source-stewardship -- --batch mini-batch-1 --apply --approve-choice-legacy-batch-stewardship --brand comfort-inn-suites
```

Reports: `reports/choice-legacy-batch-source-stewardship.{md,json}` (batch 1) or `reports/choice-legacy-mini-batch-2-stewardship.{md,json}` (batch 2)

---

## After batch approval

1. Re-run batch stewardship dry-run after URL capture registers six official sources
2. Apply batch stewardship for URL sources (Explorer Yes; Extraction Yes when readable)
3. **Do not** extract facts until founder approves extraction path per brand

---

## Does not do

- Rebuild Explorer / overwrite Setup
- Approve facts / extract / publish governance
- Set Company Validated
- Approve development-page sources for extraction
