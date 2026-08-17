# Choice Legacy Mini-Batch Fact Stewardship v1

**Date:** 2026-07-07  
**Status:** Dry-run default; batch apply with explicit approval  
**Brands:** mini-batch 1 — Comfort, Everhome, Quality; mini-batch 2 — Country Inn, Radisson, Radisson Individuals, Radisson RED

**Batch flag:** `--batch mini-batch-1` (default) or `--batch mini-batch-2`. Run prefix: `pi-choice-legacy-batch-` (batch 1) / `pi-choice-legacy-batch-2-` (batch 2).

> **Authority:** [choice-legacy-batch-extraction-v1.md](./choice-legacy-batch-extraction-v1.md), [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md)

---

## Purpose

Batch review of **Pending** facts from `pi-choice-legacy-batch-*` extraction across Comfort, Everhome, and Quality. Recommends approve / hold / reject without brand-by-brand stewardship commands.

Does **not** publish governance or write Brand Setup fields.

---

## Scope

- Pending facts linked to mini-batch allowlisted sources
- Extraction run ID prefix: `pi-choice-legacy-batch-`
- Unsupported keys (`be.positioning.segment`, `be.positioning.chainScale`) → reject recommendation
- Gap facts → reject recommendation

---

## Recommendation rules

| Outcome | When |
|---------|------|
| **Approve** | Approved Choice-controlled source, supported key, substantive value, clear evidence, no overclaim |
| **Hold** | Fragmentary value, weak evidence, noisy PDF summary, careful-review keys (`typicalUseCase`, short `whyValue`), short `guestPromise`, thin `geoIntro` |
| **Reject** | Wrong brand, duplicate, unsupported key, gap fact, blocked URL, implies Company Validated |

**Apply** writes `Human Review Status = Approved` (+ `Approved Value`) for **approve** recommendations only. Hold/reject stay Pending.

---

## Priority approve keys

- `be.identity.brandName`
- `be.identity.parentCompany`
- `be.positioning.summary` (unless PDF layout noise)
- `be.positioning.guestPromise` (unless fragment)
- `be.overview.developmentModel`
- `be.footprint.geoIntro` (unless fragment)
- `be.loyalty.programName`

---

## Commands

```bash
# Dry-run (default)
npm run choice-legacy-batch-fact-stewardship -- --batch mini-batch-2 --dry-run

# Batch apply
npm run choice-legacy-batch-fact-stewardship -- --batch mini-batch-1 --apply --approve-choice-legacy-batch-fact-stewardship

# Per-brand fallback
npm run choice-legacy-batch-fact-stewardship -- --batch mini-batch-1 --dry-run --brand comfort-inn-suites
```

Reports: `reports/choice-legacy-batch-fact-stewardship.{md,json}` (batch 1) or `reports/choice-legacy-mini-batch-2-fact-stewardship.{md,json}` (batch 2)

---

## After batch fact approval

1. `npm run audit-partner-intelligence-publish-readiness`
2. `npm run publish-partner-intelligence-profile-governance -- --entity-type brand --target-rec-id <rec…> --dry-run`
3. Founder review governance dry-run before any publish apply

---

## Does not do

- Rebuild Explorer / overwrite Brand Setup
- Publish governance / Company Validated
- Auto-approve held facts
- Change UI, scoring, or schema
