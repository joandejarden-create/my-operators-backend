# Active Brand Profile Governance Upgrade v1

**Date:** 2026-07-06  
**Status:** Dry-run audit workflow implemented  
**Scope:** Existing Explorer-active Brand profiles — add PI evidence and governance **without** rebuilding Explorer content.

> **Authority:** [dealality-intelligence-production-workflow-v1.md](./dealality-intelligence-production-workflow-v1.md), [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md)

---

## Purpose

Dealality has many **already-active** Brand Explorer profiles built via the prior manual/AI-assisted method. The Partner Intelligence pipeline should **not** rebuild these profiles from scratch. Instead, add:

- Source evidence (Source Library)
- Approved facts (Extracted Facts)
- Profile governance (trust label / validation status)
- Optional enrichment recommendations

Treat active legacy brands as:

**Explorer Active — Pending Governance Upgrade**

---

## What this workflow does

1. Resolves Brand Setup - Brand Basics `rec…` for a fixed v1 batch (11 Choice/Hilton active brands).
2. Checks Explorer active status (`Brand Status` Active/Live or `Active` checkbox).
3. Estimates **existing profile completeness** from Brand Setup fields (not PI fact count).
4. Inventories linked PI sources and facts.
5. Reads live governance + readiness report for expected trust chip.
6. Classifies profile status and recommended next action.
7. Reports missing evidence package items (no download/register in v1).

---

## What it does not do

- Rebuild Brand Explorer presentation content
- Overwrite populated Brand Setup fields
- Download or register sources
- Run extraction apply
- Approve facts
- Publish governance
- Set Company Validated / Company Validation Date
- Downgrade Company Reviewed / Company Validated profiles

---

## v1 brand batch

| Brand | Known `rec…` (hint) |
|-------|----------------------|
| Ascend Hotel Collection | resolve from Airtable |
| Comfort Inn & Suites | resolve from Airtable |
| Country Inn & Suites by Choice | resolve from Airtable |
| Curio Collection by Hilton | `receQkxgjlezsc1xg` |
| Everhome Suites | resolve from Airtable |
| Kimpton Hotels | `recCKuXCmGvxHPfb3` |
| Quality Inn | resolve from Airtable |
| Radisson Blu by Choice | `recWPEvxBQxVVzSq3` |
| Radisson by Choice | resolve from Airtable |
| Radisson Individuals by Choice | resolve from Airtable |
| Radisson RED by Choice | resolve from Airtable |

Record IDs are resolved by **exact name match** on Brand Basics. `knownRecId` is verified when present; ambiguous or missing rows are reported as unresolved.

---

## Profile completeness (conservative)

| Category | Heuristic |
|----------|-----------|
| **Strong Existing Profile** | ≥8 substantive Setup signals + logo/hero signals |
| **Adequate Existing Profile** | ≥5 signals |
| **Thin Existing Profile** | Active but &lt;5 signals |
| **Missing / Not Active** | Not Active/Live in Setup |
| **Unable To Determine** | Row not loaded |

PI absence alone does **not** mark a profile incomplete.

---

## Status taxonomy

| Status | Meaning |
|--------|---------|
| Platform Ready | Stage 8 + governance `no_op` or `equivalent_stable_live_governance` (or downgrade-protected) |
| Active Legacy Profile | Live governance + stable |
| Active — Governance Upgrade Needed | Eligible PI package; governance publish pending |
| Active — Evidence Package Needed | Active Explorer profile; PI sources missing |
| Fact Approval Needed | Sources exist; pending facts |
| Extraction Needed | Approved sources; no facts |
| Active — Light Enrichment Needed | Active but thin profile or optional assets |
| Level 2+ Candidate | Not Explorer-active but has some Setup content |
| New / Full Production Needed | Not active / too thin for legacy upgrade path |
| Unresolved Record | Could not resolve Brand Basics row |

**Governance equivalence (2026-07-07):** When live Setup governance already produces the same Explorer trust chip as the PI proposal, non-critical proposal drift (Source Type `Company PDF / Brochure` vs `Company Materials`, live `High` vs proposed `Medium` confidence, live `CALA-Specific` vs proposed null region, Internal Notes text) classifies as `equivalent_stable_live_governance` — not `conflict`. Real mismatches (trust label, usage permission, Do Not Display, Company Validated writes) remain blocked.

---

## Commands

```bash
# Dry-run (default)
npm run active-brand-governance-upgrade -- --dry-run

# Refresh readiness + queue after individual brand work
npm run audit-partner-intelligence-publish-readiness
npm run intelligence-production-queue -- --plan
```

Reports: `reports/active-brand-governance-upgrade.{md,json}`

---

## Scaling to 30–40 brands

1. Add brands to `ACTIVE_BRAND_BATCH` in `lib/partner-intelligence/active-brand-governance-upgrade.js`.
2. Run dry-run batch audit → sort by priority (P1 governance / source package first).
3. Run per-brand PI pipeline (capture → steward → extract → fact approve → governance publish dry-run → apply).
4. Re-run batch audit to move brands from **Governance Upgrade Needed** → **Platform Ready**.
5. Use **Light Enrichment** list for optional second-pass assets (images, PDFs, PR links) without blocking governance.

---

## Change impact

| Tier | **Medium** — read-only audit; no Airtable writes |
| Modules | `lib/partner-intelligence/active-brand-governance-upgrade.js`, `scripts/active-brand-governance-upgrade.mjs` |
