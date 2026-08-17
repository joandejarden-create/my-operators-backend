# Brand Explorer presentation layer (Airtable + API)

## Purpose

Optional **1:n** copy keyed by **slot** so the Brand Explorer combined UX can be filled without borrowing unrelated Brand Setup fields. `GET /api/brand-library/brand` merges rows into `brand.brandExplorer`:

```json
"brandExplorer": {
  "version": 1,
  "blocks": [
    { "recordId": "rec…", "slotKey": "hero.benefit_zones", "title": "", "body": "…", "sort": 0 }
  ]
}
```

Inactive rows (`Active` = false / no) are omitted.

**First brand to load (example):** Radisson — copy-ready slot rows are in [`fixtures/brand-explorer-presentation-radisson.example.json`](../fixtures/brand-explorer-presentation-radisson.example.json). **Source PDFs:** Choice CALA one-pager → [radisson-choice-cala-one-pager-reference.md](./radisson-choice-cala-one-pager-reference.md); Radisson brochure (32 pp., mostly visual) → [radisson-brochure-reference.md](./radisson-brochure-reference.md). To push rows: `npm run apply-brand-explorer-presentation -- --dry-run --brand-name Radisson` then without `--dry-run`.

- **`--only-missing`** — create only fixture rows whose **Slot Key** is not already present for that brand (no deletes).
- **`--slot-keys k1,k2`** — limit the fixture to those slot keys (use with `--only-missing` for a small patch).
- **`--replace`** — delete **all** presentation rows for the brand, then create the **entire** fixture (full reload; not a partial patch).
- **`--prune-except-slot-keys k1,k2`** — delete existing rows for the brand whose slot key is **not** in the list (cleanup only).

---

## Airtable: create the table

1. **Table name (exact):** `Brand Setup - Brand Explorer Presentation`
2. **Link field:** `Brand` → links to **Brand Setup - Brand Basics** (same pattern as Footprint, Standards, etc.).
3. **Optional:** `Brand Name` (single line) for text fallback if link field names differ in your base.
4. **Columns:**

| Column (recommended name) | Type | Notes |
|---------------------------|------|--------|
| **Slot Key** | Single line | Stable id from table below (required per row). |
| **Title** | Single line | Optional; combined with Body as `Title: Body` when both set. Use **title case** (major words capitalized) for headings shown in Brand Explorer. |
| **Body** | Long text | Main copy. |
| **Case Summary Overview** | Long text (optional) | Modal section **Property overview**. If empty, the **Situation** paragraph from **Body** is used (or text after `---` in Body; see `materials.caseStudy`). |
| **Case Summary Owner Objective** | Long text (optional) | Modal **Owner objective**. If empty, **Location · Asset** lines from **Body** are combined. |
| **Case Summary Brand Relevance** | Long text (optional) | Modal **Brand relevance**. If empty, the **Why the brand was relevant** paragraph from **Body** is used. |
| **Case Summary Interpretation** | Long text (optional) | Modal **Dealality interpretation**. If empty, the owner **takeaway** paragraph from **Body** is used. |
| **Case Summary Tags** | Long text (optional) | Comma-separated tags for modal **Related tags**. If empty, the card chip list (first **Body** paragraph) is reused. |
| **Image** | Attachment (multiple) | Optional. **First** attachment’s URL is exposed as `imageUrl` on that block in `brand.brandExplorer.blocks`. Used today for **Overview → Where This Brand Creates the Most Value** scenario cards when **Slot Key** is `overview.scenario.1` … `overview.scenario.3` (same row as title/body). Also accepts field names `Images`, `Scenario Image`, or `Attachments` if your base uses those labels. |
| **Sort Order** | Number | Lower first within the same slot. |
| **Active** | Checkbox | Uncheck to hide a row without deleting. |

5. Add one or more rows per brand; link each row to the brand’s Basics record.

**Add Case Summary columns via Metadata API (optional):** from the repo, with `AIRTABLE_API_KEY` and `AIRTABLE_BASE_ID` set, run `npm run ensure-brand-explorer-presentation-case-summary-fields` (use `--dry-run` on the script to preview). Creates the five **Case Summary …** long-text fields only if missing.

If the table or columns are missing, the API still succeeds; `brandExplorer.blocks` is empty and `loadWarnings` may include `"Brand Explorer Presentation"` when the fetch throws.

---

## Slot keys wired in code

### Combined hero (`brand-explorer-gold-detail.js`)

| Slot key | Where it appears | Behavior |
|----------|------------------|----------|
| `hero.benefit_zones` | Hero meta “Typical Benefit Zones” | If any row exists, **Body** (multiple rows joined with `, `) replaces the heuristic from differentiators / footprint. |
| `hero.operator_compat` | Hero meta “Operator Compatibility” | If set, replaces specializations / testimonials / profile first line. |

### Atelier Overview (`renderAtelierOverview`)

| Slot key | Where it appears | Behavior |
|----------|------------------|----------|
| `overview.typical_use_case` | Overview snapshot → **Typical Use Case** | Merged **Body** (or **Title** if body empty). Replaces Footprint **Specific Markets/Cities** proxy. Example: *Gateway cities, regional hubs, leisure destinations with independent character.* |
| `overview.portfolio_context` | Overview → **Portfolio Context** ladder + snapshot **Relative Positioning** | **One row per brand.** **Title** = ladder index `0`–`3`. **Body** = relative positioning copy (portfolio spectrum—not **Brand Positioning** on Basics). Inactive ladder steps list other **Choice Hotels International** brands in that tier (from brand list + tier map); active step shows this brand’s name. Legacy keys `overview.portfolio_ladder_tier` / `overview.relative_positioning` still read if present. GET `/api/brand-library/brand` returns **`portfolioLadderTier`**; list endpoint includes it for CHI brands. Batch: `npm run apply-choice-portfolio-context-batch`. |
| `overview.relative_positioning` | Overview snapshot → **Relative Positioning** | *(Legacy—prefer `overview.portfolio_context` Body.)* Merged **Body** when consolidated row absent. |
| `overview.portfolio_ladder_tier` | Overview → **Portfolio Context** tier only | *(Legacy—prefer `overview.portfolio_context` Title.)* |
| `overview.development_model` | Overview snapshot → **Development Model** | Merged **Body** (or **Title** if body empty). Conversion vs new-build story—not **Brand Development Stage** + **Brand Model Format** join. Example: *Conversion & repositioning (primary); new-build where market-appropriate.* |
| `operations.flexibility.design` | Operations & Standards → **Flexibility Indicators** → Design Flexibility | **Body** = level label (bar width follows). See **Flexibility indicator Body values** below. |
| `operations.flexibility.conversion` | → Conversion Friendliness | Same. |
| `operations.flexibility.localization` | → Localization Flexibility | Same. |
| `operations.flexibility.operational_rigidity` | → Operational Rigidity | Same (higher rigidity = longer bar). |
| `operations.flexibility.pip` | → PIP Sensitivity | Same. |
| `operations.flexibility.prototype` | → Prototype Dependence | Same. |
| `operations.model.primary_model` | **Operating Model** → Structure & ownership → **Primary Model** | **Body** only (no Brand Basics / Footprint fallback). |
| `operations.model.management_option` | → **Management Option** | **Body** only—not Footprint managed/franchised % mix. |
| `operations.model.typical_ownership` | → **Typical Ownership Structure** | **Body** only—not Operational Support owner-involvement fields. |
| `operations.model.brand_involvement` | Brand involvement & systems → **Brand Involvement** | **Body** only. |
| `operations.model.systems_integration` | → **Systems Integration** | **Body** only. |
| `operations.model.pre_opening` | → **Pre-opening Discipline** | **Body** only. |
| `operations.model.staffing_intensity` | Operations & complexity → **Staffing Intensity** | **Body** only. |
| `operations.model.fb_complexity` | → **F&B Complexity** | **Body** only. |
| `operations.model.training` | → **Training Requirements** | **Body** only. |
| `operations.model.reporting_discipline` | Governance & technology → **Reporting Discipline** | **Body** only. |
| `operations.model.qa_rhythm` | → **QA Rhythm** | **Body** only. |
| `operations.model.technology` | → **Technology Expectations** | **Body** only. |
| `operations.standards_philosophy` | Operations & Standards → **Standards Philosophy** → **Philosophy** | Merged **Body** only (no Brand Setup **Brand Standards** fallback). |
| `operations.operator_compat.summary` | Operations & Standards → **Third-Party Operator Compatibility** → **Summary** | Merged **Body** only (no Brand Setup fallback). |
| `operations.operator_compat.tags` | → yellow tag chips | **Body**: one tag per line, or comma / `;` / `•` separated. Optional: multiple rows with the same slot key (each row’s **Body** or **Title** = one tag). |
| `operations.operator_compat.fit` | → **Fit** | Merged **Body** only (no Brand Setup fallback). |

### Flexibility indicator Body values

Paste **one** phrase in **Body** (Title optional). Matching is case-insensitive. The tag on screen is exactly what you typed. **Each canonical label has its own bar width** (pairs like `High` vs `Very high` are never the same length).

| Body value | Bar width (approx.) |
|------------|---------------------|
| *(blank)* | 0% — empty |
| `Minimal` | 14% |
| `Low` | 28% |
| `Moderate` | 42% |
| `Medium` | 58% |
| `High` | 76% |
| `Very high` | 94% |

**Synonyms** (map to the same bar as the row above): `Very low` → Minimal · `Mid`, `Average`, `Fair`, … → Moderate · `Strong`, `Significant`, … → High · `Exceptional`, `Maximum`, `Extensive` → Very high · `N/A`, `TBD`, … → empty.

**Numeric shortcut (`1`–`6` or `4/6`):** `1` Minimal · `2` Low · `3` Moderate · `4` Medium · `5` High · `6` Very high.

**Radisson example set:** Design `High` · Conversion `Very high` · Localization `High` · Operational rigidity `Moderate` · PIP `Moderate` · Prototype `Low`.

**Enforcement (required):** Every brand must use the **same answer shape** per slot key. Do **not** mix canonical levels with narrative paragraphs or compound phrases (`Moderate to High`) on `operations.flexibility.*`—the UI bar label is the Body text exactly. Put prose in `operations.standards_philosophy`, `overview.development_model`, or other editorial slots.

**Write-path gate (automatic):** Applies and Tab Factory builds sanitize Flexibility Bodies before Airtable write via `sanitizeFlexibilityPresentationBody` in `lib/brand-explorer-flexibility-levels.mjs`:
- Leading level is kept (`Very high\nprose…` → `Very high`).
- Empty / pure narrative falls back to a **segment-appropriate** default (`inferFlexSegmentForBrand` → `flexLevelForSlot`).
- Wired in `scripts/apply-brand-explorer-presentation-fixture.mjs` and `lib/partner-intelligence/brand-explorer-full-tab-factory-build.js`. UI also displays level-only as a safety net.

**Normalize / audit:**
- `node scripts/generate-choice-tier1-explorer-full.mjs` — regenerates CHI `*-full.json` fixtures with canonical flex levels.
- `node scripts/normalize-flexibility-presentation.mjs --fixtures` — patch existing JSON fixtures.
- `node scripts/normalize-flexibility-presentation.mjs --airtable` — patch live Airtable rows (optional `--brand-name`).
- `node scripts/audit-brand-explorer-presentation-formats.mjs` — report cross-brand format conflicts.
| `overview.scenarios` | Scenario strip (3 cards) | Optional: up to **3** paragraphs in **Body** (blocks merged with `\n\n`); fills card bodies before per-card overrides. |
| `overview.scenario.1` … `overview.scenario.3` | Same | **Three separate Airtable rows** (each its own **Slot Key**). **Title** / **Body** override that card’s title and paragraph; **`Image`** (first attachment) replaces the gray “Image” placeholder. If those rows do not exist yet, add them manually or re-run apply from a fixture that includes them (e.g. Radisson example after `--replace`). |
| `overview.why_value` | “Why this brand wins” bullet list | If any row’s merged text is non-empty, that text (split like other bullets: newlines / `;` / `•`) replaces **Brand profile analysis** / **Brand value proposition** for the five list items. |
| `overview.owner_experience` | Owner Value Snapshot → **Owner experience** | Replaces **Brand History** as the line source when set (see existing behavior). |
| `overview.proof_operator` | Proof grid → **Operator-enabled execution** | Replaces **Brand profile analysis** for that card body only when set. |
| `overview.differentiators.identity` | **Key Differentiators** → **Experience & Identity** column | When this or `overview.differentiators.commercial` has copy, merged **Body** (lines from newlines / `•` / `;`) fills the **left** list; otherwise the UI splits **Brand Basics → Key Brand Differentiators** in half. |
| `overview.differentiators.commercial` | **Key Differentiators** → **Commercial & Distribution** column | Merged **Body** fills the **right** list when either slot is set; same fallback as above. |
| `overview.bestAt.1` … `overview.bestAt.3` | **What This Brand Is Best At** (three cards) | Optional **Body** (and optional **Title**) per card. Default headings: Conversion & Repositioning, Blended-Demand Markets, Owner Speed-to-Flag. If a slot’s body is empty, that card falls back to the matching line from **Brand Basics → Brand Pillars** (split like bullets). |

### Atelier Value to Owners (`renderValueToOwners`)

| Slot key | Where it appears | Behavior |
|----------|------------------|----------|
| `valueOwners.overview` | “What the Owner Is Really Buying” overview card | Merged **Body** (+ optional **Title**: **Body** per row) replaces the join of value proposition / history / customer promise. |
| `valueOwners.scenarios` | Value Creation Scenarios (4 cards) | Up to **4** paragraphs, merged with `\n\n`, mapped to cards 1–4 before per-card overrides. |
| `valueOwners.scenario.1` … `valueOwners.scenario.4` | Same | **Title** overrides default scenario title; **Body** overrides that card’s body (wins over `valueOwners.scenarios`). |
| `valueOwners.watchouts` | Key Watchouts list | Merged copy replaces **Key Brand Differentiators** / value proposition as bullet source when set. |
| `valueOwners.lifecycle.1` … `valueOwners.lifecycle.6` | Lifecycle timeline | **Title** overrides the default phase label; **Body** fills the phase detail line (plain text). |

### Atelier Economics & Obligations (`renderAtelierEconomicsObligations`)

Owner economics tab: KPI strip (FDD-style typical ranges from Brand Setup when populated), proof-point fee cards, cash rhythm, opening timeline, three fee buckets, risk, negotiability. No peer benchmarks or gated blocks.

| Slot key | Where it appears | Behavior |
|----------|------------------|----------|
| `economics.intro` | Economics at a Glance → **How to use this tab** | Merged **Body** |
| `economics.kpi.royalty` | KPI strip → Typical royalty | **Body** overrides; else min/max + basis from `feeStructure` |
| `economics.kpi.marketing` | → Marketing / brand fund | Same pattern |
| `economics.kpi.application` | → Application fee | Dollar min/max + basis |
| `economics.kpi.term` | → Initial franchise term | **Body** overrides; else `dealTerms` qty × length × duration |
| `economics.kpi.technology` | → Technology fee | Min/max + basis |
| `economics.kpi.loyalty` | → Loyalty program fee | Min/max % + basis |
| `economics.checklist` | Your Diligence Checklist | Lines from **Body**; fallback: `economics.diligence` then built-in defaults |
| `economics.diligence` | Checklist fallback | Same as checklist when `economics.checklist` empty |
| `economics.cash.preopening` | Cash & Capital Rhythm | **Body**: optional `Owner typically funds:` / `Brand typically provides:` paragraphs (`\n\n`); fallback: `economics.lifecycle.preopening` |
| `economics.cash.ramp` | → Early years (ramp) | Same; legacy `economics.lifecycle.ramp` |
| `economics.cash.steadystate` | → Steady state | Same; legacy `economics.lifecycle.steadystate` |
| `economics.cash.renewal` | → Renewal / repositioning | Same; legacy `economics.lifecycle.renewal` (+ PIP hints from Brand Setup when empty) |
| `economics.opening.step.1` … `economics.opening.step.5` | Opening & Conversion Path timeline | **Title** / **Body** per step; default labels if empty |
| `economics.opening.process` | → Process summary card | Merged **Body** |
| `economics.opening.financials` | → Financial planning themes | Merged **Body** (no dollar amounts) |
| `economics.fee.join` | Fees in Three Buckets → To Join | **Title** / **Body**; fallback: fee-structure categories (application, training) |
| `economics.fee.operate` | → To Operate | Fallback: royalty, marketing, technology, loyalty, reservation |
| `economics.fee.change` | → When Things Change | Fallback: PIP, termination, reserves themes from Brand Setup |
| `economics.fee_variability` | What drives variability (`oe-cluster`) | Merged **Body** |
| `economics.risk` | Term, Renewal & Exit Risk (multiple rows) | **Title** + **Body** scenario cards; fallback: `economics.risk_exit`, term/performance slots, `economics.legal`, then API text fields |
| `economics.negotiability` | Negotiability & Incentives → posture card | Merged **Body**; fallback: `economics.incentives` + Operational Support |
| `economics.negotiable_items` | → Often negotiated list | Lines from **Body** |
| `economics.rarely_negotiable` | → Usually standard list | Lines from **Body** |

**Legacy slots** (v1 fixture; still read as fallbacks): `economics.model`, `economics.kpi.*`, `economics.fee` (per-category rows), `economics.lifecycle.*`, `economics.term_renewal`, `economics.performance_exit`, `economics.legal`, `economics.support_burden`, `economics.incentives`.

Fixtures: [`fixtures/brand-explorer-presentation-economics-v2.json`](../fixtures/brand-explorer-presentation-economics-v2.json) (owner-focused v2). Legacy v1: [`fixtures/brand-explorer-presentation-economics-obligations.json`](../fixtures/brand-explorer-presentation-economics-obligations.json). Push new keys only: `npm run apply-brand-explorer-presentation -- --brand-name Radisson --fixture fixtures/brand-explorer-presentation-economics-v2.json --only-missing` (if keys already exist, update **Body** in Airtable or use `--replace` with the full fixture for a full reload).

When presentation rows are absent, the tab still renders from Brand Setup (`feeStructure`, `dealTerms`, `legalTerms`, `operationalSupport`). **Typical min/max ranges** (royalty, marketing, application, tech, loyalty, reservation) and **initial term** display when those fields are populated in Fee Structure / Deal Terms—always labeled as brand-level typicals, not a deal quote.

### Atelier Loyalty Program (`renderLoyaltyProgram`)

| Slot key | Where it appears | Behavior |
|----------|------------------|----------|
| `loyalty.hero_title` | Section **H2** | **Body** preferred, else **Title**, else fallback: *Typical Loyalty Program Name* from Brand Setup + “ — Loyalty at a Glance”. |
| `loyalty.kpi.members` | KPI strip | **Title** / **Body** override default label and value; default value uses **Total Global Members (Approx. Millions)** when present. |
| `loyalty.kpi.hotels` | KPI strip | Same pattern; default uses **footprint** open hotel count. |
| `loyalty.kpi.markets` | KPI strip | Default uses **number of markets** + cities/markets text from footprint form values when present. |
| `loyalty.kpi.mix` | KPI strip | Default uses **Typical % of Rooms from Loyalty** when present. |
| `loyalty.ecosystem` | Program Positioning → **Ecosystem** | Plain text (escaped); replaces default paragraph when set. |
| `loyalty.owner_lens` | **Owner Lens** | Same. |
| `loyalty.proof` | Proof grid | **Multiple rows**, same slot key: each **Title** = headline, **Body** = support (replaces entire static grid when at least one row has title or body). |
| `loyalty.earn` | Sample Mechanics → **Earn** | Merged body, lines → `<ul><li>…` (no HTML in Airtable); if empty, illustrative static list remains. |
| `loyalty.redeem` | **Redeem** | Same. |
| `loyalty.elite` | Elite tiers | **Multiple rows** → `diff-card` blocks (**Title** + **Body**); if none, static illustrative tiers remain. |
| `loyalty.implications.pnl` | Property & Owner Implications → P&amp;L card | Merged body or fallback copy. |
| `loyalty.implications.ops` | Operations & Guest Experience | Same. |
| `loyalty.implications.systems` | Systems & Data | Same. |

### Atelier Commercial Engine (`renderCommercialEngine`)

Benefit/impact framing for owners comparing brands—not sales scripts or per-lever diligence checklists. Lever **Body** uses two parts separated by `Project impact:` (legacy `Owner lens:` still parses).

| Slot key | Where it appears | Behavior |
|----------|------------------|----------|
| `commercial.intro` | Intro under **How This Brand Can Lift Your Project** | Merged **Body**; brand-specific in Tier 1 fixtures (tagline + positioning). |
| `commercial.differentiator` | **Commercial edge on this brand** card | **Title** optional; **Body** = one-line benefit hook (Tier 1: first `bestAt` from profile). |
| `commercial.kpi.channels` | KPI strip | **Title** = label, **Body** = value; defaults: channels in franchise materials. |
| `commercial.kpi.campaigns` | KPI strip | Same pattern (campaign rhythm). |
| `commercial.kpi.b2b` | KPI strip | B2B programs. |
| `commercial.kpi.lens` | KPI strip | Owner underwriting lens. |
| `commercial.lever.distribution` | Strength card | **Title** = lever headline (optional override). **Body** = what the lever is + `\n\nProject impact: …` (brand-specific benefit in Tier 1; see `scripts/lib/choice-tier1-commercial-impacts.mjs`) |
| `commercial.lever.revenue_management` | Strength card | Same. |
| `commercial.lever.digital_marketing` | Strength card | Same. |
| `commercial.lever.corporate_group` | Strength card | Same. |
| `commercial.lever.leisure_destination` | Strength card | Same. |
| `commercial.lever.international` | Strength card | Same. |
| `commercial.lever.sales_catering` | Strength card | Same. |
| `commercial.lever.reputation_qa` | Strength card | Same. |
| `commercial.lever.data_analytics` | Strength card | Same. |
| `commercial.theme` | **Where This Brand Tends to Win** (multiple rows) | **Body** = bullet line; Tier 1 uses `bestAt` from profile (replaces static list when any row present). |
| `commercial.demand` | **Demand Scenario View** (multiple rows) | **Title** = scenario name, **Body** = directional label (Strong, Moderate–strong, Not a fit, etc.). |

Tier 1 Choice full fixtures include these keys via `scripts/lib/choice-explorer-full-builder.mjs` (`buildCommercialRows`). Regenerate: `npm run generate-choice-tier1-explorer-full`. Push: `npm run apply-brand-explorer-presentation -- --brand-name "Comfort Inn & Suites" --fixture fixtures/brand-explorer-presentation-comfort-inn-suites-full.json --only-missing`.

**Market Perception** still uses Brand Basics **Brand Positioning** (`explorerDetailCard`) unless a future `commercial.perception` slot is added.

### Atelier Dealality Insight (`renderDealalityInsight`)

| Slot key | Where it appears | Behavior |
|----------|------------------|----------|
| `insight.summary` | Dealality Insight → **Summary** card | **Body** = editorial Dealality summary (long text). There is **no** Brand Basics column for this in most bases—use this presentation row. If empty, the UI falls back to **Brand Value Proposition**, then first paragraph of **Key Brand Differentiators**, then **Brand Positioning**. |
| `insight.similar` | Dealality Insight → **Similar Brands** (multiple rows) | **Title** = brand name on card. **Body** optional parenthetical subtitle (e.g. `(Hilton · full-service conversion mainstream)`). Illustrative peers only—not equivalency claims. |

Radisson portfolio / compliance / similar fixture: [`fixtures/brand-explorer-presentation-radisson-portfolio-compliance-similar.json`](../fixtures/brand-explorer-presentation-radisson-portfolio-compliance-similar.json). Push: `npm run apply-brand-explorer-presentation -- --brand-name Radisson --fixture fixtures/brand-explorer-presentation-radisson-portfolio-compliance-similar.json --only-missing`.

Optional footprint editorial slots (same table): `footprint.editorial`, `footprint.geo_intro`, `footprint.growth_editorial`, `footprint.growth_themes`, `footprint.growth_fit` — merged **Body** for Footprint & Growth copy blocks when set.

**Footprint & Growth — Geographic Footprint + Growth Priorities (voco-style)**

| Slot key | Where it appears | Body format |
|----------|------------------|-------------|
| `footprint.geo_intro` | **Geographic Footprint** intro paragraph | Plain text (one or more paragraphs merged). |
| `footprint.region.am` … `footprint.region.apac` | Region status cards + schematic strip | **Body:** line 1 = status badge label (e.g. `Strong CALA momentum`); blank line; line 2+ = card narrative. **Title** optional region name (defaults: Americas, CALA, Europe, MEA, APAC). Status containing `limited` or `selective` dims the card and turns schematic segment off. |
| `footprint.growth_themes` | **Growth Priorities** → tag chips | One theme per line (or `;` / `•` separated). Avoid a single word like `Mature` unless intentional—the UI no longer falls back to Brand Development Stage when this row exists. |
| `footprint.growth_editorial` | Growth Priorities → right column paragraph | Plain text. |
| `footprint.growth_fit` | **Most Likely Growth Fit** bullets | One bullet per line. |
| `footprint.editorial` | **Dealality View on Market Presence** → interpretation paragraph | Plain text (inside yellow-bordered Dealality card). |
| `footprint.editorial_bullets` | Same section → bullet list | One bullet per line. |
| `footprint.momentum_label` | **Recent Momentum** sub-label under section hint | Plain text (default: Recent openings & pipeline · linked announcements). |
| `footprint.momentum` | **Recent Momentum** timeline items (multiple rows) | **Permanent template (all brands):** **Title** = named opening/conversion/membership headline. **Body:** date line (e.g. `Oct 2024` or `2025`); blank line; owner-useful summary; blank line; trailing `https://…` announcement URL → Proper Case hyperlink in UI. Newest → oldest. Contract: `lib/partner-intelligence/brand-explorer-recent-momentum-contract.js`. Do not use untitled diligence blobs. |
| `footprint.portfolio_mix` | **Portfolio Mix** pills under Recent Momentum (multiple rows) | **Title** = category (e.g. `Urban`). **Body** = level label (e.g. `High`). Alternate: one row, **Body** lines as `Category\|Level` or `Category: Level`. |

| `operations.compliance.qa_cadence` | Operations & Standards → **Compliance & Oversight** → QA Cadence | **Body** only; overrides Brand Setup QA / deal terms fallback when set. |
| `operations.compliance.training_rigor` | → Training Rigor | **Body** only; overrides HR training / owner education fallback. |
| `operations.compliance.reporting` | → Reporting Expectations | **Body** only; overrides **Compliance & Safety** fallback. |
| `operations.compliance.brand_interaction` | → Brand Interaction Frequency | **Body** only; overrides communication / decision-making fallback. |

Radisson momentum fixture: [`fixtures/brand-explorer-presentation-radisson-footprint-momentum.json`](../fixtures/brand-explorer-presentation-radisson-footprint-momentum.json). Push: `npm run apply-brand-explorer-presentation -- --brand-name Radisson --fixture fixtures/brand-explorer-presentation-radisson-footprint-momentum.json --only-missing`.

Radisson fixture: [`fixtures/brand-explorer-presentation-radisson-footprint-geo-growth.json`](../fixtures/brand-explorer-presentation-radisson-footprint-geo-growth.json). Push: `node scripts/patch-footprint-presentation-by-slot.mjs --brand-name Radisson`.

### Atelier Footprint & Growth — Openings (`renderFootprintGrowth`)

| Slot key | Where it appears | Behavior |
|----------|------------------|----------|
| `footprint.openings` | **Openings / Examples / Properties** (property-example cards + **View Property** modal) | **Permanent Ascend template (all brands):** **Title** = `{Property} {Brand} — {City}` (not `— Property Example`). **Body** (blank lines preferred; single newlines also OK): (1) comma-separated tag chips, (2) location line, (3) meta / asset line (country or conversion · keys · amenities), (4) optional scenario accent (uppercase lime in UI), (5) property-specific opening teaser, (6) optional `https://…`. **Case Summary …** columns for modal. **Image** = card hero. Contract: `lib/partner-intelligence/brand-explorer-openings-property-card-contract.js`. Forbidden: generic “affiliation fit / design narrative” boilerplate. |

**`footprint.openings` — Case Summary column → View Property modal** (same Airtable columns as case studies; labels differ on screen):

| Airtable column | Modal section |
|-----------------|---------------|
| Case Summary Overview | Property overview |
| Case Summary Brand Relevance | Why it is relevant |
| Case Summary Owner Objective | What it suggests about the brand |
| Case Summary Interpretation | Dealality takeaway |
| Case Summary Tags | Similar property types (comma-separated) |

Radisson example: [`fixtures/brand-explorer-presentation-radisson-footprint-openings.json`](../fixtures/brand-explorer-presentation-radisson-footprint-openings.json). Push: `npm run apply-brand-explorer-presentation -- --brand-name Radisson --fixture fixtures/brand-explorer-presentation-radisson-footprint-openings.json --only-missing`. To replace copy on rows that already exist, update those fields in Airtable or delete the `footprint.openings` rows and re-run.

### Atelier Brand Materials (`renderBrandMaterials`)

| Slot key | Where it appears | Behavior |
|----------|------------------|----------|
| `materials.file` | Official Brand Materials → file cards | **Multiple rows.** **Title** = file name on the card (e.g. `Brand Overview Deck.pdf`). **Body** = meta line under the title (e.g. `PDF · 4.2 MB · Updated Feb 12, 2026`). Optional `Badge: …` line overrides the default badge (`Unverified by Brand`). Optional `https://…` line in **Body** (first URL wins) for View/Download when the file is not attached. **Preferred:** attach PDF/ZIP to **Image** for View/Download (Body meta only). PDF/ZIP icon is inferred from the attachment URL or **Title** extension. If no rows, four placeholder cards with sample meta (no links). |
| `materials.caseStudy` | Case Studies & Proof of Application | **Multiple rows** replace the three empty shells when at least one row exists. **Title** = property / card headline. **Body** (card): **double newlines** between paragraphs — **six blocks:** (1) comma-separated chip labels, (2) location, (3) asset descriptor, (4) situation, (5) why the brand fit, (6) owner takeaway on the card. Optional **final paragraph that is only** an `https://…` URL is stripped for the card and used for **Open external link** in the modal. **Modal-only copy in the same Body:** after those six blocks, add a blank line, a line containing only `---`, another blank line, then five paragraphs (Property overview; Owner objective; Brand relevance; Dealality interpretation; Related tags as comma-separated). They populate the **Case summary** modal when the **Case Summary …** Airtable columns are empty (columns always win). **View Summary** opens that modal (voco-style sections); a URL is optional. Optional **Image** = hero thumb. |
| `materials.gallery.1` … `materials.gallery.6` | Image Gallery (six tiles) | One row per slot; **Image** (first attachment) fills the tile; **Title** overrides the default caption (Lobby, Guest Room, …). If no image for a slot, that tile stays the empty gradient shell with the caption. |

Add more keys the same way: read in `brand-explorer-atelier-from-api.js` with `explorerMergedBody`, `explorerFirstBlock`, `explorerParagraphs`, or `explorerCardRowsForSlot`.

---

## Inventory

Run `npm run export-brand-setup-airtable-inventory` — the CSV/JSON includes schema rows for this table under **Brand Explorer · Presentation**.

---

## PATCH / Brand Setup UI

Not implemented in this MVP: create and edit rows in **Airtable** (or add a PATCH handler + Brand Setup subform later).
