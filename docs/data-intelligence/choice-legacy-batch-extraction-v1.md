# Choice Legacy Mini-Batch Extraction v1

**Date:** 2026-07-07  
**Status:** Dry-run default; batch apply with explicit approval  
**Brands:** mini-batch 1 — Comfort, Everhome, Quality; mini-batch 2 — Country Inn, Radisson, Radisson Individuals, Radisson RED (RHG global sources disallowed as primary)

**Batch flag:** `--batch mini-batch-1` (default) or `--batch mini-batch-2`. Manifest: `lib/partner-intelligence/choice-legacy-batch-config.js`.

> **Authority:** [choice-legacy-batch-source-stewardship-v1.md](./choice-legacy-batch-source-stewardship-v1.md), [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md)

---

## Purpose

Batch fact extraction dry-run for three Explorer-active Choice brands with approved PI source packages. Avoids brand-by-brand extraction commands when risk profile is shared.

Creates **Pending** Extracted Facts only — no auto-approval, no governance publish.

---

## Source allowlists (per brand)

| Brand | PDF | Consumer | Press |
|-------|-----|----------|-------|
| Comfort | `recZFPfGRo5C9FF2Q` | `recxm2Jxqvi2n2I8K` | `recRbi8CjS8BVt4Z3` |
| Everhome | `rechRqlbx7DF4YCCV` | `rec28KQ9ubpynVfTq` | `rechbWISi8BQwTqGb` |
| Quality | `recmEnl9wcLfSA4Mk` | `recpsFcGtpvib16s0` | `recfh3rpBaKo0U0H1` |

**Excluded:** development JS-shell pages, RHG/global URLs, sources not approved for extraction.

---

## Target fact keys (registry-supported)

| Priority | Field key |
|----------|-----------|
| P0 | `be.identity.brandName`, `be.identity.parentCompany`, `be.positioning.summary`, `be.positioning.tagline`, `be.positioning.guestPromise` |
| P1 | `be.overview.developmentModel`, `be.overview.whyValue` |
| P2 | `be.overview.typicalUseCase`, `be.footprint.geoIntro`, `be.loyalty.programName` |

`be.positioning.segment` and `be.positioning.chainScale` are **not** registry keys — capture in `be.positioning.summary` when source-backed.

---

## Extraction rules

- **Local PDF** — development model, owner value, typical use case, positioning
- **Consumer page** — guest promise, loyalty, guest-facing language
- **Press kit** — official description, footprint/geo intro when stated
- Primary PDF wins cross-source dedupe for development/positioning keys
- No gap facts; skip duplicates against existing Airtable facts
- Conservative language; no Company Validated inference

---

## Commands

```bash
# Dry-run (default)
npm run choice-legacy-batch-extract -- --batch mini-batch-2 --dry-run

# Single brand fallback
npm run choice-legacy-batch-extract -- --batch mini-batch-1 --dry-run --brand comfort-inn-suites

# Batch apply (founder approval required)
npm run choice-legacy-batch-extract -- --batch mini-batch-1 --apply --approve-choice-legacy-batch-extract
```

Reports: `reports/choice-legacy-batch-extract.{md,json}` (batch 1) or `reports/choice-legacy-mini-batch-2-extract.{md,json}` (batch 2)

---

## Post-extract workflow

1. `npm run steward-partner-intelligence -- --entity-type brand --target-rec-id <rec…> --dry-run --recompute`
2. Human review Pending facts → approve subset
3. `npm run audit-partner-intelligence-publish-readiness`
4. Governance publish dry-run (per brand) — **not** from this script

---

## Does not do

- Rebuild Explorer / overwrite Brand Setup
- Approve facts / publish governance / Company Validated
- Extract development JS-shell or third-party sources
- Create gap facts
