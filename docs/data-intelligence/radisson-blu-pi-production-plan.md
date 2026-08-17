# Radisson Blu (Choice) — Partner Intelligence Production Plan

**Date:** 2026-07-06  
**Status:** **Stage 4 — narrow extraction dry-run ready** (3 sources approved; 0 facts)  
**Target:** Radisson Blu by Choice — Brand Setup - Brand Basics  
**Record ID:** `recWPEvxBQxVVzSq3`

> **Authority:** [partner-intelligence-priority-profile-production-tracker.md](./partner-intelligence-priority-profile-production-tracker.md), [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md), [dealality-intelligence-production-workflow-v1.md](./dealality-intelligence-production-workflow-v1.md), [radisson-blu-choice-reference.md](../radisson-blu-choice-reference.md)

---

## Executive Summary

| Question | Answer |
|----------|--------|
| **Brand Setup record exists?** | **Yes** — `recWPEvxBQxVVzSq3` · display name **Radisson Blu by Choice** |
| **PI sources linked?** | **Yes** — 3 Source Library rows (web captures) |
| **Extracted facts?** | **No** — 0 Extracted Facts rows |
| **Approved Explorer-use sources?** | **3** |
| **Brand Explorer presentation?** | **L2 complete** (217+ presentation rows; CALA fixtures) — **not** PI evidence |
| **Local Choice reference folder?** | **Likely yes** on shared drive — FDD + CALA one-pager/deck per repo audits; **not registered in PI** |
| **In publish readiness audit?** | **No** — no PI package assembled |
| **Workflow stage (2026-07-06)** | **Stage 1** — `source_discovery` · blocker `no_linked_sources` |
| **Next step** | `npm run radisson-blu-extract -- --dry-run` → review → apply → steward fact approval |

**Naming note:** Airtable display name is **Radisson Blu by Choice**. Fixtures and tracker often use **Radisson Blu (Choice)**. Explorer gold-standard docs use both. Always pass `--target-rec-id recWPEvxBQxVVzSq3` for PI commands.

**Separate brand row:** **Radisson Blu** (Radisson Hotel Group parent) is a **different** Brand Basics record for global/RHG markets. This PI package targets **Choice Americas affiliation only** — do not link RHG-global sources without explicit region tagging and publish-scope review.

---

## 1. Brand Record

| Field | Value |
|-------|-------|
| **Record ID** | `recWPEvxBQxVVzSq3` |
| **Table** | Brand Setup - Brand Basics |
| **Display name** | Radisson Blu by Choice |
| **Fixture / docs name** | Radisson Blu (Choice) |
| **Parent (fixture target)** | Alpha Brand Studios / Choice Hotels International (Americas) |
| **Explorer** | `/brand-explorer-combined.html?id=recWPEvxBQxVVzSq3` |

### Current PI status (read-only 2026-07-06)

| Metric | Count |
|--------|-------|
| Source Library rows linked | **0** |
| Extracted Facts linked | **0** |
| Approved for Explorer Use | **0** |
| Approved / Edited facts | **0** |
| Publish readiness package | **Not present** |
| Completeness tier | **empty** |

Workflow report: `reports/intelligence-profile-workflow.json` (entity `recWPEvxBQxVVzSq3`).

---

## 2. Current Governance Status (Setup root)

| Field | Live value |
|-------|------------|
| Validation Status | — (blank) |
| Usage Permission | — |
| Source Type | Public Sources + AI Extraction *(legacy default only)* |
| Data Confidence Level | — |
| Last Reviewed Date | — |
| Company Validated | **false** |
| External Display Status | — |
| Explorer Hero Verification | Source-Backed Draft *(legacy)* |
| Normalized display label | — *(Governance Not Set)* |

P1 profile governance is **not applied**. Rich Brand Explorer presentation is **presentation-layer** content until PI sources + reviewed facts exist.

**Governance pilot:** `pilot-profile-governance-values` may have touched related Choice brands; **this row has no live governance chip**.

---

## 3. Region / Ownership Caveats

| Topic | Guidance |
|-------|----------|
| **Americas vs global** | Choice Hotels International owns/franchises Radisson Blu (and Radisson, RED, Individuals, etc.) in the **Americas**. Outside the Americas, Radisson Blu is owned by **Radisson Hotel Group** (Belgium HQ) — unaffiliated with Choice. |
| **Press kit disclaimer** | Choice media center states this split explicitly ([Radisson Blu press kit](https://media.choicehotels.com/Radisson-blu-press-kit)). |
| **Global footprint claims** | RHG development pages cite **390+ hotels globally** — **includes EMEA/APAC not under Choice**. For Dealality PI, prefer **Americas-only** counts from Choice materials (e.g. 10 hotels / 2,433 rooms Americas as of Sep 2024 in press kit). |
| **Parent company field** | Fixture uses **Alpha Brand Studios** (Choice brand studio). FDD lists **Choice Hotels International, Inc.** as franchisor. Extract both contexts; do not conflate with **Radisson Hotel Group** parent on the separate RHG brand row. |
| **CALA** | Repo CALA materials (`RADBLU_OnePager_New_Final.pdf`, `RB_PitchDeck_Final.pdf`) are **Choice CALA regionalization** — valid for CALA corridor facts when source-backed; tag **Source Region** accordingly at stewardship. |
| **RHG PDFs on media.radissonhotels.net** | May be useful **reference** for brand DNA (positioning, design pillars) but treat as **global/RHG** unless the PDF is explicitly Americas/Choice-scoped. Do not use RHG global hotel counts as Americas footprint without Choice source confirmation. |
| **Third-party FDD sites** | e.g. fddexchange.com — **do not capture**; use official FDD PDF from Choice reference folder (`35781-202604-09`, FY2025 per [choice-fdd-inventory.md](../choice-fdd-inventory.md)). |
| **Booking / OTA pages** | **Exclude** from v1 PI capture. |

---

## 4. Source Discovery Findings

### 4.1 Priority — capture for Americas PI package

| # | URL | Owner / domain | Region | Company-controlled? | Facts supported | Capture now? | Suggested Source Library title |
|---|-----|----------------|--------|---------------------|-----------------|--------------|-------------------------------|
| 1 | https://www.choicehotelsdevelopment.com/our-brands/upper-upscale/radisson-blu | Choice Hotels International | **Americas** | **Yes** | Positioning, upper-upscale segment, development types (new build/conversion), recent openings, owner value prop, contact path | **Yes — P0** | Radisson Blu — Choice development brand page |
| 2 | https://www.choicehotels.com/radisson-blu | Choice Hotels International | **Americas** | **Yes** | Guest-facing brand story, tagline, experience pillars | **Yes — P0** | Radisson Blu — Choice consumer brand page |
| 3 | https://media.choicehotels.com/Radisson-blu-press-kit | Choice Hotels International | **Americas** | **Yes** | Official positioning copy, Americas hotel count, ownership disclaimer, brand highlights | **Yes — P0** | Radisson Blu — Choice press kit (Americas) |
| 4 | Local: `Choice Hotels International/fdd/` → `35781-202604-09` (2025 Radisson Blu FDD) | Choice Hotels International | **U.S. / Americas** | **Yes** | Franchisor identity, investment range, Item 19 performance (verify year), agreement structure themes | **Yes — P0** (copy/register, not third-party download) | Radisson Blu FDD 2025 (MN/state filing filename) |
| 5 | Local: `RADBLU_OnePager_New_Final.pdf` | Choice CALA / brand studio | **CALA / Americas** | **Yes** (company materials) | Tagline, guest psychographic, development types, Americas presence snapshot, pillars | **Yes — P1** | Radisson Blu CALA one-pager (Choice) |
| 6 | Local: `RB_PitchDeck_Final.pdf` | Choice CALA | **CALA / Americas** | **Yes** | Positioning, KPIs, heritage narrative, Choice platform support — **verify footnotes** (global vs Americas) | **Yes — P1** | Radisson Blu CALA development pitch deck |
| 7 | Local: `brochure--blu.pdf` (per brand-basics audit) | Choice / brand studio | **Americas** | **Yes** | Think in Blu pillars, rich positioning | **Yes — P1** | Radisson Blu development brochure (Choice) |

**Local folder (expected):** `Brand Reference Material/Choice Hotels International/` — see [choice-fdd-inventory.md](../choice-fdd-inventory.md). Files may also live on shared drive `G:\My Drive\Dealality™\…\Choice Hotels International`.

### 4.2 Secondary — reference only (do not lead publish scope)

| # | URL | Owner | Region | Company-controlled? | Capture now? | Notes |
|---|-----|-------|--------|---------------------|--------------|-------|
| 8 | https://www.radissonhotels.com/en-us/brand/radisson-blu | Radisson Hotel Group | **Global / EMEA-APAC** | Yes (RHG) | **No** for v1 package | Guest brand page; global narrative |
| 9 | https://www.radissonhotels.com/en-us/corporate/development-opportunities/our-brands/radisson-blu | Radisson Hotel Group | **EMEA / APAC dev** | Yes (RHG) | **No** for v1 package | Global footprint, EMEA/APAC factsheets — **conflicts with Choice Americas scope** |
| 10 | https://investor.choicehotels.com/…/2025/…Radisson…Visual-Identities… | Choice Hotels International | **Americas** | Yes | **Optional P2** | Rebrand / repositioning context (Jan 2025) |
| 11 | https://www.choicehotelsdevelopment.com/international/cala | Choice Hotels International | **CALA** | Yes | **Optional P2** | CALA development hub; news links |

### 4.3 Exclude

| Source type | Reason |
|-------------|--------|
| Booking.com, Expedia, TripAdvisor, etc. | Third-party — not company-controlled |
| fddexchange.com, franchisegator, etc. | Third-party FDD aggregators |
| Wikipedia / travel blogs | Source-Informed at best; not v1 primary |
| RHG global press releases for EMEA signings | Wrong region for Choice Americas PI package |

---

## 5. Recommended First Capture Set

**Scope:** Americas / Choice-controlled only — minimum viable PI package (mirror Kimpton / Curio clean-path pattern).

| Order | Material | Type | Rationale |
|-------|----------|------|-----------|
| 1 | Choice development brand page | Website capture | Official owner-facing positioning + openings |
| 2 | Choice consumer brand page | Website capture | Tagline + guest promise cross-check |
| 3 | Choice press kit | Website / media-kit capture | Americas counts + explicit ownership disclaimer |
| 4 | FDD `35781-202604-09` | FDD (local copy) | High-trust franchisor + economics |
| 5 | CALA one-pager PDF | Development brochure (local copy) | Dense company materials; CALA relevance |
| 6 | CALA pitch deck PDF | Development brochure (local copy) | Owner value + platform support — review global footnotes before approving facts |

**Folder:** `Choice Hotels International` (not `Radisson Blu by Choice`) per [partner-development-portal-registry.js](../../api/lib/partner-development-portal-registry.js).

**Stewardship defaults after register:** Status **Captured** → human review → **Approved for Explorer Use = Yes** only on reviewed sources; Source Origin **Brand Provided** for company PDFs (override default Public Web where appropriate).

---

## 6. Recommended Extraction Targets

Prioritize publish-scope fields from `brand-explorer-registry-catalog.js`:

| Field key | Priority | Notes |
|-----------|----------|-------|
| `be.identity.brandName` | P0 | Expect "Radisson Blu" |
| `be.identity.parentCompany` | P0 | Choice Hotels International / Alpha Brand Studios — **not** RHG for this package |
| `be.positioning.tagline` | P0 | "Think in Black & White Blu" (verify live copy) |
| `be.positioning.summary` | P0 | Upper-upscale positioning |
| `be.positioning.guestPromise` | P0 | Experience / design / service pillars |
| `be.positioning.history` | P1 | 1960 design-hotel heritage — cite source |
| `be.overview.typicalUseCase` | P1 | Inspired professional / guest psychographic |
| `be.overview.developmentModel` | P1 | New build, conversion, adaptive reuse |
| `be.overview.whyValue` | P1 | Owner value from development page / deck |
| `be.footprint.*` | P2 | **Americas counts only** from Choice press kit / one-pager |
| `be.markets.regionsSupported` | P2 | Tag **Americas** / **CALA** — not global RHG footprint |
| Standards / owner requirements | P2 | Only if official development PDF states requirements |

**Do not extract** global RHG hotel totals into this package without parallel Americas source.

**Narrow extract:** After ≥3 sources captured and approved, run stewardship dry-run then brand extract dry-run (pattern: `curio-clean-reextract` / Kimpton narrow allowlist).

---

## 7. Risks / Blockers

| Risk | Severity | Mitigation |
|------|----------|------------|
| Region mix (Choice vs RHG) | **High** | Americas-only source set; tag Source Region at registration; quarantine RHG-only facts |
| Global vs Americas footprint in pitch deck footnotes | **High** | Human review before approving footprint facts |
| Two Brand Basics rows named "Radisson Blu*" | **Medium** | Always link sources to `recWPEvxBQxVVzSq3` |
| Explorer presentation ≠ PI evidence | **Medium** | Do not auto-approve facts from fixtures; re-extract from captured sources |
| Legacy `Source Type` on row | **Low** | Overwritten only via approved governance publish later |
| Showpad / gated Choice PDFs | **Medium** | Prefer local CALA copies; test URL access before download |
| Sparse approved facts after first extract | **Expected** | Medium confidence until ≥3 substantive approved facts |

---

## 8. Exact Dry-Run Capture Commands

**Do not add `--apply` or `--register` until founder approval.**

### Step 0 — Workflow plan (read-only)

```bash
npm run intelligence-profile-workflow -- --entity-type brand --target-rec-id recWPEvxBQxVVzSq3 --plan
```

### Step 1 — Init company folder

```bash
npm run partner-reference:init-folder -- --company "Choice Hotels International" --dry-run
```

### Step 2 — Website captures (P0)

```bash
npm run partner-reference:download -- \
  --url "https://www.choicehotelsdevelopment.com/our-brands/upper-upscale/radisson-blu" \
  --company "Choice Hotels International" \
  --brand "Radisson Blu by Choice" \
  --type website-capture \
  --title "Radisson Blu Choice development brand page" \
  --brand-id recWPEvxBQxVVzSq3
```

```bash
npm run partner-reference:download -- \
  --url "https://www.choicehotels.com/radisson-blu" \
  --company "Choice Hotels International" \
  --brand "Radisson Blu by Choice" \
  --type website-capture \
  --title "Radisson Blu Choice consumer brand page" \
  --brand-id recWPEvxBQxVVzSq3
```

```bash
npm run partner-reference:download -- \
  --url "https://media.choicehotels.com/Radisson-blu-press-kit" \
  --company "Choice Hotels International" \
  --brand "Radisson Blu by Choice" \
  --type media-kit \
  --title "Radisson Blu Choice press kit Americas" \
  --brand-id recWPEvxBQxVVzSq3
```

### Step 3 — FDD (local file — register after copy to reference tree)

If FDD already exists under `Choice Hotels International/fdd/`, **copy** (do not re-download from third party) then register via Source Library manual row or future register script. Reference path per [choice-fdd-inventory.md](../choice-fdd-inventory.md): `35781-202604-09` (2025).

Optional future dedicated script (pattern: `save-radisson-red-choice-development-pdfs.mjs`):

```bash
# TODO when approved — local copy + optional RHG reference PDFs with region notes
node scripts/save-radisson-blu-choice-development-pdfs.mjs
```

### Step 4 — CALA PDFs (local copy — when files confirmed on disk)

```bash
# Example pattern once public URL or local path confirmed:
npm run partner-reference:download -- \
  --url "https://…" \
  --company "Choice Hotels International" \
  --brand "Radisson Blu by Choice" \
  --type development-brochure \
  --title "Radisson Blu CALA one-pager" \
  --brand-id recWPEvxBQxVVzSq3
```

For files already on disk (`RADBLU_OnePager_New_Final.pdf`, `RB_PitchDeck_Final.pdf`, `brochure--blu.pdf`), register via PDF source workflow — **dry-run first**:

```bash
npm run register-radisson-blu-pdf-sources -- --dry-run
npm run register-radisson-blu-pdf-sources -- --apply --approve-radisson-blu-pdf-register
```

Reports: `reports/radisson-blu-pdf-register.{md,json}`

v1 registers only `RADBLU_OnePager_New_Final.pdf`. Pitch deck, brochure, and FDD are future candidates (listed in dry-run inventory, not registered).

### Step 5 — After capture (later, not now)

```bash
npm run steward-partner-intelligence -- --entity-type brand --target-rec-id recWPEvxBQxVVzSq3 --dry-run --recompute
npm run audit-partner-intelligence-publish-readiness
```

---

## 9. External Web Capture Needed?

| Capture type | Needed? |
|--------------|---------|
| **HTML website capture** | **Yes** — development page, consumer page, press kit (3 P0 URLs) |
| **PDF download from public URLs** | **Partial** — CALA PDFs likely **local copy** from Choice folder; FDD from existing FDD library |
| **RHG global web/PDF** | **No** for v1 Americas package |
| **Browser harvest script** | **Optional later** — Choice portal registry exists; no Blu-specific harvest script yet |

---

## 10. Recommended Governance Outcome (if clean package)

| Scenario | Internal Validation Status | External chip | Source Basis |
|----------|---------------------------|---------------|--------------|
| **Clean company-controlled Choice materials** (expected) | **Company Published** | **AI-Assisted Profile** | **Company Materials** |
| Mixed Choice official + third-party press only | **Source-Informed** | **Source-Informed Profile** | **Reviewed Sources** |
| Company Validated | **Never** without direct company attestation | — | — |

**Confidence:** expect **High** if ≥3 substantive approved facts from High-quality company sources (4+ sources, FDD + brochures — similar to Kimpton path).

**Company Validated / Company Validation Date:** **Do not write** from PI pipeline.

---

## 11. Related Repo Assets (not PI)

| Asset | Path |
|-------|------|
| Explorer fixtures | `fixtures/brand-explorer-presentation-radisson-blu*.json` |
| Brand basics fixture | `fixtures/brand-basics-radisson-blu-choice.json` |
| Reference doc | [radisson-blu-choice-reference.md](../radisson-blu-choice-reference.md) |
| Choice factory gold standard | [choice-brand-explorer-completion-runbook.md](../choice-brand-explorer-completion-runbook.md) |

---

## 12. Manual QA Checklist (post-capture, future)

- [ ] 3+ Source Library rows linked to `recWPEvxBQxVVzSq3`
- [ ] Each source has Source Region = Americas or CALA (not Global unless justified)
- [ ] No RHG-global footprint facts approved without Americas corroboration
- [ ] Stewardship dry-run shows approved Explorer sources
- [ ] Narrow extract dry-run — no wrong-brand contamination (Curio lesson)
- [ ] Publish readiness `changeClass` = `new` (not `downgrade`)
- [ ] Governance publish dry-run → AI-Assisted Profile / Company Materials

---

## Change Impact

| Tier | **Low** — documentation + read-only workflow plan only |
| Rollback | Delete or archive this plan doc; no Airtable impact |
| Airtable fields touched | **None** |
