# Brand Explorer — Protected Baseline Usage Rules

> **Status:** Binding for agents and PRs  
> **Current protected Active/Live public-full universe:** **62** (`docs/data-intelligence/brand-explorer-62-active-public-full-baseline.md`)  
> **Baseline type:** Active/Live + public-full + PVQL-clean + quality freeze + evidence-quality (mandatory wave) + AI-Assisted footnote always-on  
> **Quality-clean revision:** all 62 `approve_for_baseline_freeze` (no accepted minors; MGallery resolved) — `docs/data-intelligence/brand-explorer-62-active-public-full-quality-clean-freeze.md`  
> **Historical freezes:** 24 public-full · 25 Tapestry wave · interim 27 Active/Live-only · protected 27 public-full (pre-Wave 12) · protected 39 public-full (pre-Wave 13) · protected 45 public-full (pre-SO/ release) · protected 46 public-full (pre-Wave 14) · protected 54 public-full (pre-Wave 15) · prior 62 semantic-clean (MGallery minor accepted)  
> **Freeze decision (current):** `frozen_62_active_public_full_baseline_quality_clean_flex_held`  
> **Product status:** `brand_explorer_62_active_public_full_quality_clean_frozen_ready_for_child_table_validation`  
> **No Airtable / Presentation / image writes in this rules doc.**

Future Brand Explorer work that touches Active/Live profiles **must compare against** the current protected 62-brand public-full baseline. Do not treat PRIMARY_RELEASE, restore lanes, factory queues, or prior 23/24/27/39/45/46/54 lists as the active universe.

---

## 1. Active universe source of truth

| Rule | Detail |
|------|--------|
| **SoT** | Brand Basics `Brand Status` ∈ {`Active`, `Live`} |
| **Loader** | `lib/partner-intelligence/brand-explorer-active-universe.js` |
| **Formula** | `OR({Brand Status}='Active', {Brand Status}='Live')` |
| **APIs** | `GET /api/brand-library/brands`, `GET /api/brand-explorer/brands` |

Reconcile with:

```bash
npm run brand-explorer-active-universe-source-of-truth -- --dry-run
```

**Not the universe (operational overlays only):**

- `PRIMARY_RELEASE_SLUGS` (release cohort)
- Prior 23-brand reconciliation lists
- Historical 24 / 25 / protected 27 / protected 39 / protected 45 / protected 46 public-full freezes (artifacts only)
- Lane / restore / factory-supported slug lists
- Intentional public restore registry
- LEGACY_SEED / built-blocked / full-build identity maps (identity helpers only)

---

## 2. Stale 23/24/27/39/45/46-brand lists are forbidden as universe

- Never hardcode or treat a **23-brand**, historical **24-brand**, prior **27-brand**, prior **39-brand**, prior **45-brand**, or prior **46-brand** list as the live Active/Live universe.
- If universe count leaves **62**, stop and revise the protected baseline explicitly — do not silently absorb the change.
- Historical 24/25/27/39/45/46/54 freezes and the interim 27 Active/Live-only freeze remain predecessor artifacts; do not use them as the live expected count.

---

## 2b. Factory Preview Mode (not Active/Live)

Local/internal visual review of Draft / Under Review factory candidates **must not** use Brand Status Active/Live.

| Rule | Detail |
|------|--------|
| Module | `lib/partner-intelligence/brand-explorer-factory-preview-candidates.js` |
| Query | `?beInternalPreview=1&factoryPreview=1` (+ `brandId=rec…`) |
| Display state | `factory_preview_internal` (never `active_profile_ready` / public-full) |
| Banner | `Factory Preview — Not Public / Not Active Baseline` |
| Audit | `npm run brand-explorer-factory-preview-mode -- --dry-run` |
| Test | `npm run test:brand-explorer-factory-preview-mode` |

Factory Preview does **not** join the Active/Live universe, does not weaken this baseline, and does not write Brand Status / Company Validated / Source Library / Registry.

---

## 3. Required regression gates for active-profile changes

Any change that can affect Active/Live Brand Explorer profiles must run **before merge / after apply**:

```bash
npm run test:brand-explorer-62-active-public-full-baseline
npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only
npm run test:brand-explorer-recent-momentum-evidence-quality
npm run brand-explorer-ai-assisted-footnote-standardization -- --audit
```

Also suggested when quality/recommendation risk is high:

```bash
npm run brand-explorer-62-active-public-full-baseline -- --dry-run
npm run brand-explorer-24-tab-section-quality-audit -- --dry-run
npm run test:brand-explorer-mandatory-release-gates
```

When Airtable 429 / rate-limit thrash is a risk, prefer quiet sequential validation:

```bash
node scripts/brand-explorer-quiet-sequential-pvql.mjs
node scripts/brand-explorer-quiet-sequential-quality-audit.mjs
```

**Wave preflight / live-clean gate:** do **not** treat `--allow-cached-pvql-if-pass` alone as proof the protected 62 is live-clean. Prefer a same-session fresh public-full PVQL (or quiet sequential PVQL) before freeze / release decisions.

PR automation: `npm run dealality:pr-check-suggest` suggests the protected-baseline commands when matching Brand Explorer paths change.

---

## 4. Broad remediation is forbidden unless explicitly approved

- Default posture after freeze: **targeted** cleanup only (exact findings, named brands/slots).
- **Forbidden without explicit founder/task approval:** full-profile rebuilds, wholesale gallery replacement, multi-brand “fix everything” writers, broad copy rewrites across unflagged sections.
- Prefer: audit → minor cleanup → re-audit → baseline regression.

---

## 5. Explicit exclusions (not in 62 Active/Live public-full)

| Brand | Slug | Status posture | Notes |
|-------|------|----------------|-------|
| The House of Originals | `the-house-of-originals` | Excluded | Excluded from Wave 13 |
| Morgans Originals | `morgans-originals` | Not created | Not created / not modified |
| Radisson Collection | `radisson-collection` | Draft / excluded | Excluded unless separately promoted |
| Four Points Flex by Sheraton | `four-points-flex-by-sheraton` | Held (Under Review) | Held out of Wave 14 partial promotion; not Active/Live public-full |

**SO/ is included** in the 46/54/62 freeze (`so-hotels-and-resorts` — Active/Live public-full after founder acceptance + status promotion + public release + section-pattern cleanup).

**Promoted into Active/Live public-full (intentional baseline revision 54→62 via Wave 15 Hilton eight; Four Points Flex held):**

- `hilton-hotels-and-resorts`, `homewood-suites-by-hilton`, `home2-suites-by-hilton`, `tru-by-hilton`, `doubletree-by-hilton`, `hampton-by-hilton`, `hilton-garden-inn`, `spark-by-hilton`

**Prior baseline revision (46→54 via Wave 14 Marriott International partial release, 8 of 9 brands; Four Points Flex held):**

- `marriott-hotels`, `sheraton`, `westin`, `residence-inn-by-marriott`, `springhill-suites-by-marriott`, `towneplace-suites-by-marriott`, `aloft-hotels`, `studiores`

**Prior baseline revision (45→46 via SO/ release)** remains historical (`so-hotels-and-resorts`).

**Prior Wave 13 public six (39→45)** remains historical (Mama Shelter, Mercure, ibis, Novotel, Pullman, Fairmont).

**Prior Wave 12 promotion (27→39)** remains historical (IHG / Marriott / Hilton cohort).

---

## 6. When a baseline revision is required

1. Active/Live count leaves **62**.
2. A held/excluded brand is promoted to Active/Live public-full.
3. A current Active/Live brand is demoted / locked / fails PVQL or quality freeze permanently.
4. Explicit **baseline revision** (expected count leaves 62; update freeze artifacts + regression contract).

---

## 7. AI-Assisted Profile footnote (global)

Every Brand Explorer profile must render:

```text
AI-Assisted Profile
Last Reviewed: [MMM D, YYYY] · Source Basis: […] · Region: […]
```

- Module: `lib/partner-intelligence/brand-explorer-ai-assisted-footnote.js`
- Gate: `ai_assisted_profile_footnote_visible`
- Use **enriched** public response path (not raw native governance chip alone)
- Docs: `docs/data-intelligence/brand-explorer-ai-assisted-footnote-standardization.md`

---

## 8. Pre-merge checklist (Active/Live touches)

- [ ] Universe loaded via Active/Live SoT (not a 23/24/27/39/45/46-list / PRIMARY_RELEASE-as-universe)
- [ ] Touched brands compared to freeze table in `brand-explorer-62-active-public-full-baseline.*`
- [ ] Active count matches current protected baseline (62) or intentional revision in progress
- [ ] `test:brand-explorer-62-active-public-full-baseline` PASS
- [ ] PVQL `--public-full-only` PASS
- [ ] Recent Momentum evidence quality PASS
- [ ] Footnote audit PASS (`brand-explorer-ai-assisted-footnote-standardization -- --audit`)
- [ ] House of Originals / Morgans / Radisson Collection / Four Points Flex by Sheraton still excluded/held unless separate promotion task

---

## Artifact index

| Artifact | Path |
|----------|------|
| Rules (this doc) | `docs/data-intelligence/brand-explorer-protected-baseline-rules.md` |
| Current freeze (62 public-full) | `docs/data-intelligence/brand-explorer-62-active-public-full-baseline.md` |
| Quality-clean freeze overlay | `docs/data-intelligence/brand-explorer-62-active-public-full-quality-clean-freeze.md` |
| Predecessor freeze (54) | `docs/data-intelligence/brand-explorer-54-active-public-full-baseline.md` |
| Freeze JSON (62) | `reports/brand-explorer-62-active-public-full-baseline.json` |
| Quality-clean freeze JSON | `reports/brand-explorer/brand-explorer-62-active-public-full-quality-clean-freeze.json` |
| Freeze CLI (62) | `npm run brand-explorer-62-active-public-full-baseline -- --dry-run` |
| Regression (62) | `npm run test:brand-explorer-62-active-public-full-baseline` |
| Contract (62) | `lib/partner-intelligence/brand-explorer-62-active-public-full-baseline.js` |
| Predecessor 46 public-full | `docs/data-intelligence/brand-explorer-46-active-public-full-baseline.md` |
| Predecessor 45 public-full | `docs/data-intelligence/brand-explorer-45-active-public-full-baseline.md` |
| Predecessor 39 public-full | `docs/data-intelligence/brand-explorer-39-active-public-full-baseline.md` |
| Predecessor 27 public-full | `docs/data-intelligence/brand-explorer-27-active-public-full-baseline.md` |
| Historical 24 freeze | `docs/data-intelligence/brand-explorer-24-active-public-full-baseline.md` |
| Historical 25 freeze | `docs/data-intelligence/brand-explorer-25-active-public-full-baseline.md` |
| Footnote standardization | `docs/data-intelligence/brand-explorer-ai-assisted-footnote-standardization.md` |

---

## Rollback

- Freeze artifacts are report-only; no Airtable writes occur during freeze.
- To change the protected set: run a deliberate baseline revision task; update JSON/MD freeze artifacts and expected-count contract (`lib/partner-intelligence/brand-explorer-62-active-public-full-baseline.js`).
