# Choice Brand Explorer — completion runbook (Radisson Blu parity)

**Goal:** Every Choice Hotels International brand in **Brand Setup → Brand Explorer** is populated with realistic, source-backed content at the same depth as **Radisson Blu (Choice)** — not just slot-complete generator copy.

**Gold standard:** `Radisson Blu (Choice)` (`recWPEvxBQxVVzSq3`) — 217 presentation rows, split fixtures, CALA case studies, FDD economics, footprint momentum, gallery shells.

**Reference materials root:** `PARTNER_REFERENCE_ROOT` → `G:\My Drive\Dealality™\Platform Design & Build\Brand Reference Material\Choice Hotels International\`

---

## Two completion levels

| Level | What it means | How you know |
|-------|----------------|--------------|
| **L1 — Slot complete** | All 161 expected slot keys present in Airtable | `npm run audit-choice-explorer-presentation-gaps` → 0 missing |
| **L2 — Blu parity** | Brand-specific copy, CALA property examples, economics from FDD, case studies, momentum timeline, materials | Manual QA in Brand Explorer UI; row count often 190–220+ |

All 22 CHI brands are **L1 complete** today. This runbook targets **L2** for each brand.

---

## Repeatable pipeline (one brand)

```bash
# See all brands, record IDs, parity status
npm run choice-brand-explorer:manifest

# Dry-run full pipeline (no Airtable writes)
npm run choice-brand-explorer:pipeline -- --brand "Comfort Inn & Suites"

# Apply presentation + CALA footprint for one brand
npm run choice-brand-explorer:pipeline -- --brand "Comfort Inn & Suites" --apply

# Single phase
npm run choice-brand-explorer:pipeline -- --brand "Radisson Blu (Choice)" --phase generate --apply
```

**Always use `--brand-record-id` on Windows** when applying presentation fixtures directly — npm can truncate names like `Radisson Blu (Choice)` to `Radisson`.

---

## Phase-by-phase workflow

### 1. Sources — network + online research

Follow `docs/partner-reference-material-collection-guide.md`.

Per brand folder: `Choice Hotels International/brands/{Brand Name}/`

| Subfolder | Content |
|-----------|---------|
| `development/` | Brochure, one-pager, pitch deck |
| `fdd/` | U.S. FDD PDF |
| `regional/` | CALA / Mexico materials |
| `press/` | Media center press kit |

**Repo extractions** (already partially crawled):

- `fixtures/choice-dev-site-text/` — choicehotels.com/development brand pages
- `fixtures/choice-media-center-text/` — press kits
- `fixtures/choice-fdd-text/` — FDD plain text (`docs/choice-fdd-inventory.md`)

```bash
npm run partner-reference:init-folder -- --company "Choice Hotels International" --brand "Cambria Hotels"
npm run partner-reference:download -- --url "https://…" --company "Choice Hotels International" --brand "Cambria Hotels" --type development-brochure --apply --register
```

### 2. Generate — fixture files (separate files per tab)

Two paths:

#### Path A — Tier 1 generator (16 brands)

Brands in `scripts/lib/choice-tier1-explorer-profiles.mjs`:

```bash
npm run generate-choice-tier1-explorer-full -- --brand "Comfort Inn & Suites"
# → fixtures/brand-explorer-presentation-comfort-inn-suites-full.json
```

Enrich **before** generate by editing the profile in `choice-tier1-explorer-profiles.mjs` (tagline, positioning, footprint openings, case study overrides).

#### Path B — Premium split fixtures (Radisson Blu pattern)

For upscale / Radisson-family brands, maintain **separate JSON files per tab** (easier to review and re-apply by prefix):

| Fixture suffix | Slots |
|----------------|-------|
| `.example.json` | hero, overview, operations, valueOwners, loyalty, insight |
| `-standards-*.json` | standards.* |
| `-economics-*.json` | economics.* |
| `-materials.json` | materials.file |
| `-case-studies.json` | materials.caseStudy |
| `-gallery.json` | materials.gallery.* |
| `-footprint-openings.json` | footprint.openings |
| `-footprint-momentum.json` | footprint.momentum |
| `-footprint-geo-growth.json` | footprint.geo, region.*, growth, editorial |
| `-footprint.json` | footprint.geo.summary, footprint.growth.narrative |
| `-portfolio-compliance-similar.json` | portfolio_mix, compliance, insight.similar |

**Reference implementation:** `docs/radisson-blu-choice-fixtures.md`, `docs/radisson-blu-choice-reference.md`

```bash
node scripts/build-radisson-blu-tab-fixtures.mjs
node scripts/apply-radisson-blu-choice-all-fixtures.mjs
```

**To add a new premium brand:** copy Blu fixture naming pattern (`brand-explorer-presentation-{slug}-*.json`), add build + apply scripts, register in `scripts/lib/choice-brand-explorer-manifest.mjs` → `PREMIUM_SPLIT_BRANDS`.

#### Path C — Tier 1 L2 enrichment (Ascend pattern)

For Tier 1 brands with `brand-explorer-presentation-{slug}-full.json` already slot-complete:

```bash
# 1. Add split overlays (optional per tab):
#    -{case-studies,footprint-momentum,materials,gallery}.json
# 2. Enrich CURATED_BY_PROFILE in choice-cala-openings-from-census.mjs (CALA openings)
# 3. Register momentum fixture in choice-chi-footprint-momentum-curated.mjs if needed

npm run restore-choice-tier1-brand-explorer -- --brand "Ascend Hotel Collection"
npm run restore-choice-tier1-brand-explorer -- --brand "Ascend Hotel Collection" --with-images
```

Registers in `TIER1_ENRICHED_BRANDS` in `choice-brand-explorer-manifest.mjs` when L2 is complete.

### 3. Brand Basics + commercial tables

| Layer | Batch script |
|-------|----------------|
| Brand Basics narrative | `npm run apply-choice-brand-basics-batch` |
| Fee structure / deal terms | `apply-choice-fee-structure-batch.mjs`, `apply-choice-deal-terms-batch.mjs` |
| Loyalty / commercial | `apply-choice-loyalty-commercial-batch.mjs` |
| Project fit form | `generate-choice-project-fit-fixtures.mjs` → `apply-choice-project-fit-batch.mjs` |

Source mapping: `docs/choice-brand-basics-reference-audit.md`

### 4. Apply presentation

**Tier 1 (full JSON):**

```bash
node scripts/apply-brand-explorer-presentation-fixture.mjs \
  --brand-record-id {recXXX} \
  --fixture fixtures/brand-explorer-presentation-{slug}-full.json \
  --only-missing
```

**Premium split (prefix replace):** see `apply-radisson-blu-choice-all-fixtures.mjs` step list.

### 5. CALA footprint enrichment

Real CALA property cards (not generic placeholders):

```bash
npm run apply-choice-cala-footprint-openings-batch -- --brand "Comfort Inn & Suites"
npm run apply-choice-footprint-momentum-from-openings-batch -- --brand "Comfort Inn & Suites"
npm run patch-choice-case-study-from-openings -- --brand "Comfort Inn & Suites"
```

Images: `attach-choice-footprint-opening-images.mjs`, `attach-choice-case-study-images.mjs` — or manual Airtable Image upload on gallery / opening rows.

### 6. Audit + QA

```bash
npm run audit-choice-explorer-presentation-gaps -- --brand "Comfort Inn & Suites"
```

Open: `/brand-explorer-combined.html?id={Brand%20Name}`

Checklist per tab: Overview, Operations, Economics, Footprint (openings + momentum), Materials (case studies + gallery), Loyalty, Insight.

---

## Brand priority queue (L1 → L2)

| Priority | Brands | Why |
|----------|--------|-----|
| **Done (L2)** | Radisson Blu (Choice), Radisson (Choice), Ascend Hotel Collection, Radisson RED (Choice) | Gold standard + owner-voice CALA content |
| **Done (non-CHI L2)** | Kimpton (IHG), Curio Collection by Hilton | Pilot soft-brand / lifestyle references outside CHI |
| **P1** | Radisson Collection, Radisson Individual, Park Plaza, Park Inn, Country Inn | Radisson family — shared CALA materials; stub or L1 today |
| **P2** | Cambria, Clarion, Quality, Sleep, Comfort | High owner interest; Tier 1 profiles exist — enrich from FDD + press kits |
| **P3** | Remaining Tier 1 + WoodSpring, Radisson Inn, Everhome, Suburban, Rodeway, Econo, MainStay, Clarion Pointe | Generator baseline; upgrade copy + CALA examples |

Run `npm run choice-brand-explorer:manifest` for current record IDs and fixture status.

---

## File layout convention (per brand)

```
fixtures/
  brand-explorer-presentation-{slug}-full.json          # Tier 1 merged
  brand-explorer-presentation-{slug}-case-studies.json    # Premium split (optional)
  brand-explorer-presentation-{slug}-footprint-openings.json
  brand-basics-from-choice-materials/{slug}.json
  choice-dev-site-text/...
  choice-media-center-text/...
docs/
  {slug}-choice-reference.md                            # Source notes + load sequence (optional)
```

---

## What “realistic data” means (Blu bar)

- **Positioning** from official brochure / dev site — not generic “Choice portfolio brand” stub text
- **Economics** from FDD Item 19 / fee tables — with “verify in agreement” disclaimers where needed
- **Footprint openings** — real CALA `choicehotels.com` property URLs or labeled tier-family comps with disclaimers
- **Case studies** — named hotels with owner objective / brand relevance / interpretation fields
- **No cross-brand leaks** — run `audit-brand-explorer-presentation-formats.mjs` after bulk applies

---

## Related docs

- `docs/brand-explorer-presentation-slots.md` — slot key contract
- `docs/choice-explorer-presentation-gap-audit.md` — L1 slot audit
- `docs/choice-fdd-inventory.md` — FDD file inventory
- `docs/partner-reference-material-collection-guide.md` — network folder workflow
