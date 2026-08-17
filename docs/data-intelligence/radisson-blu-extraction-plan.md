# Radisson Blu by Choice — Narrow Extraction Plan

**Date:** 2026-07-06  
**Status:** Dry-run workflow implemented — **apply not run**  
**Brand:** Radisson Blu by Choice — `recWPEvxBQxVVzSq3`

> **Authority:** [radisson-blu-pi-production-plan.md](./radisson-blu-pi-production-plan.md), [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md)

---

## Purpose

Controlled brand fact extraction from **three allowlisted Choice/Americas web captures** only. Default is dry-run; apply creates **Pending** Extracted Facts — no auto-approval, no governance publish.

---

## Source Allowlist

| Source ID | Title |
|-----------|-------|
| `recC9utJdNaKWR56k` | Radisson Blu Choice development brand page *(JS shell — not extractable; URL provenance)* |
| `recH1ZepKU6zJp7M2` | Radisson Blu Choice consumer brand page |
| `recWGLvwnDn0v5rmL` | Radisson Blu Choice press kit Americas |
| `reczafLghta09o2sB` | Radisson Blu Choice development one-pager *(primary for development/owner facts)* |

`ONE_PAGER_SOURCE_ID` wins cross-source dedupe for `be.overview.developmentModel`, `be.overview.whyValue`, `be.overview.typicalUseCase`, and positioning keys when both one-pager and web sources match.

---

## Commands

```bash
# Dry-run (default)
npm run radisson-blu-extract -- --dry-run

# Apply (founder approval required)
npm run radisson-blu-extract -- --apply --approve-radisson-blu-extract
```

Reports: `reports/radisson-blu-extract.{md,json}`

---

## Target Fact Keys (registry-supported)

| Priority | Field key | Display label |
|----------|-----------|---------------|
| P0 | `be.identity.brandName` | Brand Name |
| P0 | `be.identity.parentCompany` | Parent Company |
| P0 | `be.positioning.summary` | Brand Positioning |
| P0 | `be.positioning.tagline` | Brand Tagline |
| P0 | `be.positioning.guestPromise` | Brand Customer Promise |
| P1 | `be.overview.developmentModel` | Development Model |
| P1 | `be.overview.whyValue` | Why Owners Choose Brand |
| P1 | `be.footprint.americasHotels` | Americas Hotel Count |
| P2 | `be.positioning.history` | Brand History |
| P2 | `be.overview.typicalUseCase` | Typical Use Case |
| P2 | `be.footprint.geoIntro` | Footprint Summary (ownership caveat) |
| P2 | `be.loyalty.programName` | Loyalty Program |

### User-requested keys not in registry

| Requested | Use instead |
|-----------|-------------|
| `be.snapshot.brandName` | `be.identity.brandName` |
| `be.snapshot.parentCompany` | `be.identity.parentCompany` |
| `be.positioning.segment` / `chainScale` | Capture in `be.positioning.summary` |
| `be.markets.regionsSupported` | `be.footprint.americasHotels` / `geoIntro` |
| `be.brandFamily.context` | `be.footprint.geoIntro` (ownership disclaimer) |
| `be.ownerConsiderations.developmentPositioning` | `be.overview.whyValue` |
| `be.standards.conversionConsiderations` | `be.overview.developmentModel` |
| `be.development.model` | `be.overview.developmentModel` |

---

## Region / Ownership Rules

1. **Americas package only** — linked to `recWPEvxBQxVVzSq3`.
2. **Parent company** must be Choice Hotels International when sourced from Choice materials — not Radisson Hotel Group.
3. **Americas hotel counts** preferred from press kit (`recWGLvwnDn0v5rmL`).
4. **Reject** RHG global portfolio claims (e.g. 390+ hotels globally, Europe-leading) unless evidence includes Americas scope from Choice source.
5. **Do not** link RHG-global Brand Basics row sources.

---

## Quality Filters

Skipped candidate reasons include: `gap_copy`, `booking_boilerplate`, `rhg_global_or_wrong_region`, `weak_evidence`, `duplicate_*`.

---

## Post-Extract Workflow

1. `npm run steward-partner-intelligence -- --entity-type brand --target-rec-id recWPEvxBQxVVzSq3 --dry-run --recompute`
2. Human review Pending facts → approve subset via stewardship `--approve-fact-ids`
3. `npm run audit-partner-intelligence-publish-readiness`
4. `npm run publish-partner-intelligence-profile-governance -- --entity-type brand --target-rec-id recWPEvxBQxVVzSq3 --dry-run`

---

## Does Not Write

- Human Review Status = Approved
- Brand Setup governance fields
- Company Validated / Company Validation Date
- Platform / Explorer published fields
- Governance publish

---

## Change Impact

| Tier | **Medium** — new extract script; apply path writes Pending facts only |
| Rollback | Delete Pending facts from narrow run; revert source Status if needed |
| Modules | `lib/partner-intelligence/radisson-blu-extract.js`, `scripts/radisson-blu-extract.mjs`
