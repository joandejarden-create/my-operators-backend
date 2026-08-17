# Choice Legacy Brand Source Package v1

**Date:** 2026-07-06  
**Status:** Dry-run planning workflow implemented  
**Scope:** 8 Explorer-active Choice legacy brands — register official source evidence **without** rebuilding Explorer content.

**Batch manifests:** `lib/partner-intelligence/choice-legacy-batch-config.js` — `--batch mini-batch-1` (Comfort, Everhome, Quality) or `--batch mini-batch-2` (Country Inn, Radisson, Radisson Individuals, Radisson RED).

> **Authority:** [active-brand-governance-upgrade-v1.md](./active-brand-governance-upgrade-v1.md), [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md), [radisson-blu-pi-production-plan.md](./radisson-blu-pi-production-plan.md)

---

## Purpose

Active Brand Governance Upgrade v1 identified **8 Choice-family brands** with:

- Explorer active: **Yes**
- Profile completeness: **Strong Existing Profile**
- Status: **Active — Evidence Package Needed**
- Rebuild: **No**

This workflow plans the **P0/P1 Partner Intelligence source package** per brand so each can later move through:

`source registration → stewardship → extraction → fact approval → governance publish`

**Control excluded:** Radisson Blu by Choice (`recWPEvxBQxVVzSq3`) — already platform-ready.

---

## v1 brand batch

| # | Brand | Record ID |
|---|-------|-----------|
| 1 | Ascend Hotel Collection | `reclkgOzvAcBheUSo` |
| 2 | Comfort Inn & Suites | `recOzH5iAE1xEjyD0` |
| 3 | Country Inn & Suites by Choice | `recaayt9u7YYg8h7Y` |
| 4 | Everhome Suites | `recqkkrsevi4r9ibj` |
| 5 | Quality Inn | `recd8o4k1JddhkRWW` |
| 6 | Radisson by Choice | `recywbx1YQSTCPqW1` |
| 7 | Radisson Individuals by Choice | `recRyvM8OmLlDj9G7` |
| 8 | Radisson RED by Choice | `recmKqo7M7mLZgRqQ` |

---

## P0 source package (per brand)

| Slot | Source type | Origin |
|------|-------------|--------|
| Consumer brand page | Brand Page | `choicehotels.com/{slug}` |
| Development page | Development Page | `choicehotelsdevelopment.com/our-brands/…` |
| Development PDF | Development Brochure | Local `Brand Reference Material/Choice Hotels International/{brand}/` |
| Press kit | Press Release | `media.choicehotels.com/…` |

**JS-shell risk:** Choice development pages may render Salesforce/LWC shells (see Radisson Blu `recC9utJdNaKWR56k`). Prefer DAM PDF / one-pager when extract preview is thin.

**Radisson family caveat:** Use **Choice Americas** sources only on Choice Brand Basics rows. RHG global materials are separate reference — do not mix ownership facts.

---

## P1 optional enrichment

- Recent openings / PR (`media.choicehotels.com` announcements)
- Logo / image downloads
- FDD (`Choice Hotels International/FDDs/`)
- Extra local brochures / pitch decks

---

## Commands

```bash
# Dry-run (default) — all 8 brands
npm run choice-legacy-brand-source-package -- --dry-run

# Single brand
npm run choice-legacy-brand-source-package -- --dry-run --brand quality-inn

# Refresh governance context first (optional)
npm run active-brand-governance-upgrade -- --dry-run
```

Reports: `reports/choice-legacy-brand-source-package.{md,json}`

### Apply (local PDFs only — not run in v1 batch audit)

```bash
npm run choice-legacy-brand-source-package -- --apply --approve-choice-legacy-source-register --brand country-inn-suites-choice
```

Does **not** auto-register uncertain URLs. Does **not** auto-approve sources.

### Capture verified URLs (after dry-run review)

```bash
npm run partner-reference:download -- --url "https://www.choicehotels.com/quality-inn" \
  --company "Choice Hotels International" --brand "Quality Inn" --type website-capture \
  --title "Quality Inn consumer page" --brand-id recd8o4k1JddhkRWW --dry-run
```

---

## What it does not do

- Rebuild Brand Explorer content
- Overwrite Brand Setup fields
- Extract facts
- Auto-approve sources or facts
- Publish governance
- Set Company Validated / Date
- Auto-download uncertain URLs
- Register RHG global sources on Choice Americas rows

---

## Change impact

| Tier | **Medium** — read-only planning; optional local-file register on explicit `--apply` |
| Modules | `lib/partner-intelligence/choice-legacy-brand-source-package.js`, `scripts/choice-legacy-brand-source-package.mjs` |
