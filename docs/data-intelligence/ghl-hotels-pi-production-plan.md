# GHL Hotels — Partner Intelligence Production Plan

**Date:** 2026-07-06  
**Status:** Discovery complete — **source capture required before PI stewardship**  
**Target:** GHL Hoteles (GHL Holding) — Operator Setup - Master  
**Record ID:** `reciI2tYQBfMoMK9G`

> **Authority:** [partner-intelligence-priority-profile-production-tracker.md](./partner-intelligence-priority-profile-production-tracker.md), [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md), [hotel-equities-pi-production-plan.md](./hotel-equities-pi-production-plan.md)

---

## Executive Summary

| Question | Answer |
|----------|--------|
| **Operator Setup record exists?** | **Yes** — `reciI2tYQBfMoMK9G` · **GHL Hoteles (GHL Holding)** |
| **PI sources linked?** | **No** — 0 Source Library rows |
| **Extracted facts?** | **No** — 0 Extracted Facts rows |
| **Approved Explorer-use sources?** | **0** |
| **Approved/Edited facts?** | **0** |
| **Local reference folder?** | **Yes** (Operator Reference Material) — **empty** (no captured files) |
| **In publish readiness audit?** | **No** — no PI package assembled |
| **Stewardship dry-run runnable?** | **Yes** (ran 2026-07-06) — empty package |
| **Profile governance publish ready?** | **No** — blocked at source capture |
| **Next step** | Official website capture + Source Library registration (dry-run first) |

**Naming note:** Priority tracker lists **GHL Hotels**. Live Operator Master display name is **GHL Hoteles (GHL Holding)**. Use `reciI2tYQBfMoMK9G` for all PI commands. GTM Owner Target row `recVmFmZiPf1hbjKk` is a **different base/table** — not the Operator Setup PI target.

---

## 1. Operator Setup Record

| Field | Value |
|-------|-------|
| **Record ID** | `reciI2tYQBfMoMK9G` |
| **Table** | Operator Setup - Master |
| **Display name** | GHL Hoteles (GHL Holding) |
| **Explorer** | `/operator-explorer-gold-mock.html?id=reciI2tYQBfMoMK9G` |
| **Operator Setup** | `/third-party-operator-setup-new-two.html?recordId=reciI2tYQBfMoMK9G` |

### Current profile / governance (Setup root — read-only 2026-07-06)

| Field | Live value |
|-------|------------|
| Validation Status | — (blank) |
| Usage Permission | — |
| Source Type | — |
| Data Confidence Level | — |
| Last Reviewed Date | — |
| Company Validated | — |
| External Display Status | — |
| Explorer Hero Verification | Demo — Not Operator-Verified |
| Explorer Hero Data Source | Mock Data for Presentation |

P1 profile governance is **not applied**. Explorer child tables are linked (Materials, Governance/Delivery) but are **not** PI evidence until Source Library + reviewed facts exist.

---

## 2. Partner Intelligence Package Status

### Sources

| Metric | Count |
|--------|-------|
| Source Library rows linked to `reciI2tYQBfMoMK9G` | **0** |
| Unlinked title match “GHL” (first API page) | **0** |
| Approved for Explorer Use = Yes | **0** |

### Facts

| Metric | Count |
|--------|-------|
| Extracted Facts linked to operator | **0** |
| Human Review Status = Approved / Edited | **0** |

### Publish readiness audit

GHL does **not** appear in `reports/partner-intelligence-publish-readiness.{md,json}` (2026-07-06).

### Stewardship dry-run (2026-07-06)

```bash
npm run steward-partner-intelligence -- --entity-type operator --target-rec-id reciI2tYQBfMoMK9G --dry-run --recompute
```

| Result | Value |
|--------|-------|
| Sources in package | 0 |
| Facts in package | 0 |
| Eligible | **false** |
| Blockers | `no_linked_sources`, `no_approved_explorer_sources`, `no_approved_facts` |

Report: `reports/partner-intelligence-stewardship-package.md` (overwritten by latest run — GHL section in JSON when re-run with `--recompute` after capture).

---

## 3. Local Reference Material

| Root | Path | Status |
|------|------|--------|
| **Operator Reference Material** | `GHL Hoteles (GHL Holding)/` | Folder **exists**; **no files** found on disk scan (2026-07-06) |
| **Brand Reference Material** | — | **No** GHL-named company folder |

**Registered in Source Library?** **No** — nothing to register until files are captured.

**Dual-root read path:** Once files exist under Operator Reference Material, `readLocalSourceText()` resolves them (Brand root first, then Operator root). Relative `Local File Path` should use folder name **`GHL Hoteles (GHL Holding)/…`**.

---

## 4. Current Blockers

| Blocker | Severity |
|---------|----------|
| No PI Source Library rows linked to operator | **P0** |
| No local captured files in reference folder | **P0** |
| No extracted / approved facts | **P0** |
| No `operator-reference-registry.js` harvest profile for GHL | **P1** — use `partner-reference:download` first |
| Not in `brand-reference-material-companies.json` | **P1** — add when capture starts |
| P1 governance fields blank on Master | **Expected** until publish |
| Legacy Explorer hero = mock/demo labels | **Low** — replaced after governance publish |

---

## 5. Recommended Source Records / URLs to Review

**Official domain (GTM enrichment reference only — verify live before capture):** `https://ghlhoteles.com`

Prioritize **company-controlled** pages (not individual hotel property microsites):

| Priority | Page type | Purpose |
|----------|-----------|---------|
| P0 | Corporate home | `op.snapshot.companyName`, `op.snapshot.companyDescription` |
| P0 | About / company / who we are | Description, LATAM footprint |
| P0 | Services / management / solutions | `op.snapshot.primaryServiceModel`, `op.platform.offeredServices` |
| P1 | Brands / portfolio / flags operated | `op.brand.familiesOperated` |
| P1 | LATAM / international presence | `op.markets.regionsSupported` |
| P2 | Press / news (official only) | Supporting context — not governance lead |

**Do not use as PI primary evidence without review:** CoStar/GTM owner portfolio stats, LinkedIn bios, third-party press unless explicitly scoped as Source-Informed.

**Existing repo context (not PI):**

- GTM enrichment: `lib/gtm-owner-target/company-profile-enrichments.js` (`ghl-hoteles`) — Advent International, ~61 hotels, franchise brands named
- CALA delegate crossref: Andrés Fajardo CEO — Colombia/LATAM operator

---

## 6. Recommended First Source Capture / Register Path

Mirror **Hotel Equities** operator path (not Curio recovery):

### Step 1 — Init folder (dry-run)

```bash
npm run partner-reference:init-folder -- --company "GHL Hoteles (GHL Holding)" --dry-run
```

### Step 2 — Capture P0 website HTML (dry-run each URL)

```bash
npm run partner-reference:download -- \
  --url "https://ghlhoteles.com/" \
  --company "GHL Hoteles (GHL Holding)" \
  --type website-capture \
  --title "GHL Hoteles home" \
  --operator-id reciI2tYQBfMoMK9G \
  --profile-type Operator \
  --dry-run
```

Repeat for about/services/brands pages after verifying paths on the live site.

### Step 3 — Apply + register (founder approval)

```bash
npm run partner-reference:download -- ... --apply --register
```

Field defaults: Status **Captured**, Approved for Explorer Use **No**, Approved for Extraction **No**.

### Step 4 — Optional harvest profile (later)

Add GHL to `lib/partner-intelligence/operator-reference-registry.js` if multi-page browser harvest is desired (Arbor/Brittain pattern).

### Step 5 — Narrow extraction (2026-07-06)

`npm run ghl-hoteles-extract -- --dry-run` — see [ghl-hoteles-extraction-plan.md](./ghl-hoteles-extraction-plan.md). Allowlists 5 approved EN sources; excludes Spanish home `recFqJpw4wJbMmVSF`.

---

## 7. Target Facts (extraction / stewardship)

| Field key | Priority | Registry | Notes |
|-----------|----------|----------|-------|
| `op.snapshot.companyName` | P0 | Supported | Expect “GHL Hoteles” / holding naming |
| `op.snapshot.companyDescription` | P0 | Supported | LATAM operator + sub-brands |
| `op.snapshot.primaryServiceModel` | P0 | Supported | Hotel management / operation |
| `op.platform.offeredServices` | P0 | Supported | Management, F&B, etc. from services pages |
| `op.markets.regionsSupported` | P0 | Supported | Latin America corridors — verify against Dealality geography labels |
| `op.brand.familiesOperated` | P1 | Supported | Franchise partners + GHL Relax/Style/Collection if stated |
| `op.ownerValueProposition` | — | **Unsupported** in explorer registry | Do not extract/write |
| `op.operatingModel` | — | **Unsupported** | Do not extract/write |

---

## 8. Governance Recommendation (if clean package)

| Source basis | Internal Validation Status | External chip |
|--------------|---------------------------|---------------|
| Official `ghlhoteles.com` captures (company-controlled) | **Company Published** | **AI-Assisted Profile** · Source Basis: Company Materials |
| Mixed official + third-party press | **Source-Informed** | **Source-Informed Profile** · Source Basis: Reviewed Sources |
| Third-party / database only | **Source-Informed** | **Source-Informed Profile** |

**Never** set **Company Validated** unless GHL directly confirms.  
**Sparse fact cap:** expect **Medium** confidence until ≥3 substantive approved facts.

---

## 9. Exact Next Command

```bash
npm run ghl-hoteles-extract -- --dry-run
```

After founder review of `reports/ghl-hoteles-extract.md`:

```bash
npm run ghl-hoteles-extract -- --apply --approve-ghl-hoteles-extract
```

Then re-steward:

```bash
npm run steward-partner-intelligence -- --entity-type operator --target-rec-id reciI2tYQBfMoMK9G --dry-run --recompute
```

---

## 10. External Source Capture Needed?

**Yes.** No PI sources and no local files. Official website capture is required before extraction or governance publish.

---

## Change Impact

| Tier | **Low** — planning only; no Airtable writes |
| Rollback | N/A |

## Regression Checklist

- Do not confuse GTM owner `recVmFmZiPf1hbjKk` with Operator Master `reciI2tYQBfMoMK9G`
- Do not approve sources/facts or publish governance until capture + review
- Do not set Company Validated from public web extraction
