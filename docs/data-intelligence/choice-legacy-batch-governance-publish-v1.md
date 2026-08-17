# Choice Legacy Mini-Batch Governance Publish v1

**Date:** 2026-07-07  
**Status:** Dry-run default; batch apply with explicit approval  
**Brands:** mini-batch 1 — Comfort, Everhome, Quality (Platform Ready); mini-batch 2 — Country Inn, Radisson, Radisson Individuals, Radisson RED

**Batch flag:** `--batch mini-batch-1` (default) or `--batch mini-batch-2`.

> **Authority:** [choice-legacy-batch-fact-stewardship-v1.md](./choice-legacy-batch-fact-stewardship-v1.md), [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md)

---

## Purpose

Batch **profile governance publish** for Choice mini-batch 1 after source stewardship, extraction, and fact stewardship. Publishes trust labels only on **Brand Setup - Brand Basics** — no Explorer rebuild, no Brand Setup content overwrites, no additional fact approval.

---

## Scope

| Brand | Record ID |
|-------|-----------|
| Comfort Inn & Suites | `recOzH5iAE1xEjyD0` |
| Everhome Suites | `recqkkrsevi4r9ibj` |
| Quality Inn | `recd8o4k1JddhkRWW` |

Each brand must have:

- 3 approved company-controlled sources (PDF + consumer + press kit)
- ≥1 approved governance-priority fact in publish scope
- Held facts remain Pending (do not block publish)

---

## Expected governance posture

| Field | Value |
|-------|-------|
| Validation Status | **Company Published** |
| Usage Permission | **Platform Display Allowed** |
| External Display Status | **Show Trust Label** |
| Explorer chip | **AI-Assisted Profile** |
| Source Basis | **Company Materials** |
| Company Validated | **unchanged** (false) |
| Company Validation Date | **unchanged** |

---

## Batch guards (apply blocked when)

- Readiness not eligible on publish scope
- Any approved source not company-controlled (misclassified Choice press/consumer)
- Proposed **Source-Informed** validation or **Reviewed Sources** chip basis
- Proposed **Company Validated** or **Company Validation Date** write
- **Do Not Display** or **Do Not Use**
- Would **downgrade** stronger live governance
- Standard protection rules (Company Validated target, HOLD notes, newer Last Reviewed)

---

## Commands

```bash
# Dry-run (default)
npm run choice-legacy-batch-governance-publish -- --batch mini-batch-2 --dry-run

# Batch apply (founder approval required)
npm run choice-legacy-batch-governance-publish -- --batch mini-batch-1 --apply --approve-choice-legacy-batch-governance-publish

# Per-brand fallback
npm run choice-legacy-batch-governance-publish -- --batch mini-batch-1 --dry-run --brand comfort-inn-suites
```

Reports: `reports/choice-legacy-batch-governance-publish.{md,json}` (batch 1) or `reports/choice-legacy-mini-batch-2-governance-publish.{md,json}` (batch 2)

---

## Post-apply verification

```bash
npm run audit-partner-intelligence-publish-readiness
npm run active-brand-governance-upgrade -- --dry-run
npm run test:partner-intelligence-publish-readiness
npm run test:partner-intelligence-profile-governance-publish
```

Per-brand publish dry-run (optional):

```bash
npm run publish-partner-intelligence-profile-governance -- --entity-type brand --target-rec-id recOzH5iAE1xEjyD0 --dry-run
```

---

## Does not do

- Rebuild Explorer content
- Overwrite Brand Setup content fields
- Approve more facts
- Set Company Validated or Company Validation Date
- Change UI, scoring, BAS, OAS, OCS, Deal Readiness, or schema

---

## Mini-Batch 1 completion

After successful batch governance apply + post-apply verification (`no_op` or `equivalent_stable_live_governance` readiness, trust chips visible), mini-batch 1 is **Platform Ready** for governance/trust labels. Held facts and content enrichment remain optional follow-ups.

**Audit equivalence (2026-07-07):** Publish-readiness uses `equivalent_stable_live_governance` when live Company Published governance is same or stronger than the PI proposal (e.g. live `Company PDF / Brochure` + `High` confidence vs proposed `Company Materials` + `Medium`). Cosmetic diffs do not trigger re-apply or block Platform Ready in `active-brand-governance-upgrade`.
