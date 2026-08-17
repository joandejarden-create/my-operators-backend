# GHL Hoteles — Extraction Plan

**Date:** 2026-07-06  
**Status:** Narrow extract script added — **dry-run first**  
**Operator:** GHL Hoteles (GHL Holding) — `reciI2tYQBfMoMK9G`

> **Authority:** [ghl-hotels-pi-production-plan.md](./ghl-hotels-pi-production-plan.md), [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md)

---

## Executive Summary

| Question | Answer |
|----------|--------|
| **Sources in scope** | 5 approved English official website HTML rows (hard allowlist) |
| **Excluded** | `recFqJpw4wJbMmVSF` — Spanish home (not in allowlist) |
| **Safe apply path** | `npm run ghl-hoteles-extract` dry-run default; apply requires `--apply --approve-ghl-hoteles-extract` |
| **Recommended next step** | Review `reports/ghl-hoteles-extract.md` → founder approval → apply → steward facts |

---

## 1. Source Records in Scope

| Priority | Source ID | Title | URL |
|----------|-----------|-------|-----|
| P0 | `recvjfaDa9AnCJkNx` | GHL Hoteles home EN | https://www.ghlhoteles.com/en/ |
| P0 | `reckrUB2WmnSm02g3` | GHL Hoteles destinations | https://www.ghlhoteles.com/en/destinations/ |
| P0 | `recy337fP8zhpvePy` | GHL Hoteles hotels portfolio | https://www.ghlhoteles.com/en/hotels/ |
| P1 | `recqLGiIQAEP1I1Hv` | GHL Hoteles brand GHL | https://www.ghlhoteles.com/en/brands/ghl/ |
| P1 | `recoOcRjSD3VZb3qt` | GHL Hoteles events | https://www.ghlhoteles.com/en/events/ |

**Excluded:** `recFqJpw4wJbMmVSF` — GHL Hoteles home (ES redirect). Kept Captured but not approved for Explorer use in current steward package.

---

## 2. Target Fact Keys

| Field key | Priority | Registry | Notes |
|-----------|----------|----------|-------|
| `op.snapshot.companyName` | P0 | Supported | GHL Hoteles / GHL Hotels |
| `op.snapshot.companyDescription` | P0 | Supported | Scale sentence from EN home when present |
| `op.snapshot.primaryServiceModel` | P0 | Supported | Hotel operations only — no management-structure overclaim |
| `op.markets.regionsSupported` | P0 | Supported | Latin America + Colombia, Peru, Chile, Guatemala |
| `op.brand.familiesOperated` | P1 | Supported | Geotel, GHL, GHL Collection, GHL Relax, GHL Style, Irotama Resort, Latam Hotel Corporation |
| `op.platform.offeredServices` | P0 | Supported | Events/MICE + guest services where evidenced |
| `op.capabilities.managementServices` | — | **Unsupported** | Use `op.platform.offeredServices` proxy |
| `op.portfolio.scale` | — | **Unsupported** | Scale embedded in `op.snapshot.companyDescription` |
| `op.events.miceCapability` | — | **Unsupported** | Use `op.platform.offeredServices` for events page |

---

## 3. Extraction Quality Rules

- Prefer exact company-source statements (EN official web).
- Avoid booking-engine copy, offers/promotions, property-level booking pages.
- Avoid gap facts / Not confirmed placeholders.
- Avoid unsupported registry keys.
- Do not infer third-party management, ownership, or franchise relationships unless explicitly stated.
- Do not use Spanish home source in this workflow.

---

## 4. Commands

### Dry-run (default)

```bash
npm run ghl-hoteles-extract -- --dry-run
```

Reports: `reports/ghl-hoteles-extract.{md,json}`

### Apply (founder approval only)

```bash
npm run ghl-hoteles-extract -- --apply --approve-ghl-hoteles-extract
```

Creates **Pending** Extracted Facts only. Does not approve facts or sources. Does not publish governance.

### Post-apply stewardship

```bash
npm run steward-partner-intelligence -- --entity-type operator --target-rec-id reciI2tYQBfMoMK9G --dry-run --recompute
```

---

## 5. Governance Guardrails

| Action | Allowed by narrow script? |
|--------|---------------------------|
| Create Pending facts | Yes (apply only) |
| Approve facts | **No** — manual steward |
| Approve sources | **No** |
| Publish governance | **No** |
| Company Validated | **No** |
| Operator Setup governance fields | **No** |

Official website captures → target **Company Published** / **AI-Assisted Profile** at governance publish time (separate script, after fact approval).

---

## Change Impact

| Tier | **Medium** — new narrow extract path; no apply in this task |
| Rollback | Do not run apply; delete Pending facts manually if needed |
