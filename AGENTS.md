# Dealality — project memory for agents

> **Last updated:** 2026-08-05  
> **Keep this file under ~150 lines.** Process rules live in `.cursor/rules/deal-capture-implementation-partner.mdc`.  
> **Schema authority:** `docs/*-airtable-fields.md` and `.env.example` — never invent field names or select options.

## What this repo is

- **Product:** Dealality — confidential hotel deal-flow platform (Webflow front-end + Node proxy + Airtable).
- **Stack:** `server.js` API routes in `api/`, static UI in `public/`, scripts in `scripts/`, schema docs in `docs/`.
- **Two Airtable bases:** main product base (`AIRTABLE_BASE_ID`) and platform/GTM base (`AIRTABLE_BASE_ID_ALT`, GTM `AIRTABLE_GTM_BASE_ID=appKZuK006BWIVjNW`).

## Non-negotiables

1. **Never assume Airtable schema** — read the matching `docs/*-airtable-fields.md` before reads/writes.
2. **Validate before every write** — required fields, types, allowed select options, linked-record IDs.
3. **Dry-run first** — any script with `--apply` or live upsert: run `--dry-run` (or `DRY_RUN=true`) and review output before apply.
4. **Central field maps** — use existing mapping objects (`map_*`, `api_*`); do not scatter raw Airtable field names in UI code.
5. **UI states** — loading, empty, error, success for all data-driven UI; no blank components.
6. **No silent catches** — log or rethrow; never empty `catch () {}`.

## Dealality AI Build Operating System

For meaningful product, platform, data, or AI-output work, read the relevant docs **before** implementation.

**Batch learning (Census + Brand Explorer):** every enrichment/validation batch must improve code via the learning loop — `docs/data-intelligence/dealality-batch-learning-system.md`, ledger `npm run dealality:batch-learning-ledger`, audit `npm run dealality:batch-learning-audit`. Webhound = hard-case pattern discovery only (never production writes).

**Core docs (always start here):**

- `docs/ai-build-system/DEALALITY_PRODUCT_CONSTITUTION.md`
- `docs/ai-build-system/AI_BUILD_PROTOCOL.md`
- `docs/ai-build-system/CURSOR_IMPLEMENTATION_PROTOCOL.md`
- `docs/ai-build-system/OLD_HOME_ANTI_GLITCH_RULES.md` — `/old-home` FOUC: bake final chrome before reveal
- `docs/ai-build-system/BUILD_DECISIONS.md`
- `docs/ai-build-system/NAMING_AND_COPY_GUIDE.md`
- `docs/ai-build-system/DEALALITY_QA_CHECKLIST.md`

**Also read when the task touches** brand/operator profiles, market intelligence, PDFs, source extraction, platform content, AI-generated content, validation status, confidence, or source-backed intelligence:

- `docs/data-intelligence/DATA_VALIDATION_PROTOCOL.md`
- `docs/data-intelligence/INTELLIGENCE_GOVERNANCE.md`
- `docs/data-intelligence/CONTENT_QA_CHECKLIST.md`
- `docs/data-intelligence/SOURCE_RANKING_GUIDE.md`

**Workflow:** Inspect → Summarize Current State → Identify Risks → Plan → Implement → Test/Check → Summarize → Capture Durable Learnings

**Do not** invent Airtable fields, overwrite company-validated data, bypass access assumptions, or make unrelated changes. Update documentation only when a durable future-use decision, pattern, naming rule, governance rule, or testing requirement is created.

**Docs index:** `docs/README.md` · **Copy-paste prompts:** `docs/ai-build-system/CURSOR_PROMPTS.md`

## Repo layout (where things live)

| Area | Path | Notes |
|------|------|--------|
| API routes | `api/*.js` | Auth via Memberstack; check `test:batch*-route-auth` when touching routes |
| Operator Setup UI | `public/third-party-operator-setup-new-two.html` | 13-tab intake; 417 form fields |
| Operator Setup writer | `api/lib/operator-setup-new-base-writer.js` | New-base writer; bindings in `api/lib/third-party-operator-new-two-field-bindings.json` |
| Operator Explorer | `public/js/operator-explorer*.js` | Reads new-base; gold profiles keyed by Master `rec…`; quality bar = Arbor + Hotel Equities |
| Brand Explorer | `public/js/brand-explorer*.js`, `api/brand-library.js` | Presentation rows in Airtable + `fixtures/brand-explorer-presentation-*`. No-login share: `/brand-explorer-share.html?pack=choice` or `?pack=ihg` (OE parallel: `/operator-explorer-share`) |
| GTM / pilot | `api/target-list.js`, `api/outreach-setup.js`, scripts `*gtm*`, `*pilot-target*` | GTM base only; never expose CoStar data product-facing |
| Master To-Do | `lib/dealality-master-todo/`, `docs/dealality-master-todo.md` | Upserts to Founder Project Plan table |
| Radar / CALA | `lib/travel-infrastructure/`, `lib/radar-buildout/` | TI audits → `data/*-travel-infrastructure-audit.json` |
| Automations | `airtable/automations/` | Paste into Airtable; not executed from Node |

## Operator Setup — writer flags (`.env`)

- `OPERATOR_SETUP_USE_NEW_BASE_WRITER=0` — **legacy writer is primary** by default.
- `OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE=0` — parallel shadow writes off unless explicitly enabled.
- Field-mapping audit: `docs/operator-setup-to-explorer-field-mapping-audit.md` (477 rows; 266 still static-form-only).
- Before operator save-path changes: run `npm run test:operator-setup-new-base-save-coverage` if available.

## Brand Explorer — fixtures, audits, protected baseline

- **Protected 62 Active/Live public-full baseline (binding):** `docs/data-intelligence/brand-explorer-protected-baseline-rules.md` — universe SoT = Brand Status Active/Live; never use stale 23/24/27/39/45/46/54-lists; after active-profile changes freeze/compare via `npm run brand-explorer-62-active-public-full-baseline -- --dry-run`, `npm run test:brand-explorer-62-active-public-full-baseline`, `npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only`, `npm run test:brand-explorer-recent-momentum-evidence-quality`, and `npm run brand-explorer-ai-assisted-footnote-standardization -- --audit`. Wave preflight must use fresh live PVQL (do not rely on stale `--allow-cached-pvql-if-pass` alone). Quiet sequential (429 avoidance): `scripts/brand-explorer-quiet-sequential-pvql.mjs`, `scripts/brand-explorer-quiet-sequential-quality-audit.mjs`. Freeze: `docs/data-intelligence/brand-explorer-62-active-public-full-baseline.md` (predecessors: 24, 25, 27, 39, 45, 46, 54, interim 27 Active/Live-only).
- Presentation patches: `fixtures/brand-explorer-presentation-*.json`
- **Tab Factory (permanent setup):** `docs/data-intelligence/brand-explorer-tab-factory-build-operation.md` — new brands enter here; no baseline bypass
- **Factory Preview Mode (local/internal visual QA):** `lib/partner-intelligence/brand-explorer-factory-preview-candidates.js` — preview Draft/Under Review factory candidates without Brand Status Active; query `?beInternalPreview=1&factoryPreview=1`; `npm run brand-explorer-factory-preview-mode -- --dry-run` · `npm run test:brand-explorer-factory-preview-mode`. Does **not** join Active/Live universe or weaken the 27-brand public-full baseline.
- **Recent Momentum / Openings:** contracts + Ascend/CALA audits (see prior notes); remediation dry-run before apply
- **Flexibility Indicators:** canonical levels only via `sanitizeFlexibilityPresentationBody`
- Gap/format audits before PR touching fixtures or brand presentation
- Do not change Company Validated / Source Library / Registry / Brand Status in content cleanup; Radisson Collection + Tapestry stay excluded until Brand Status promotion

## Operator Explorer — quality baseline + Tab Factory

- **Protected quality baselines (binding):** Arbor Lodging (CALA) `recF5Z87OAqFgndoq` + Hotel Equities (CALA) `recWPKu5laVZxsvpn` — same tab-by-tab / field-by-field bar for all future operators. Rules: `docs/data-intelligence/operator-explorer-protected-baseline-rules.md`. Freeze: `docs/data-intelligence/operator-explorer-arbor-hotel-equities-quality-baseline.md`. Registry: `lib/partner-intelligence/operator-explorer-quality-baseline.js`.
- **Tab Factory (permanent setup):** `docs/data-intelligence/operator-explorer-tab-factory-build-operation.md` — parallel to Brand Explorer; PI governance alone is not Explorer quality readiness.
- **Gates:** `npm run test:operator-explorer-quality-baseline` · `npm run test:operator-explorer-mandatory-release-gates` · `npm run test:operator-explorer-tab-factory-audit` · `npm run test:operator-explorer-section-pattern-parity` · `npm run test:operator-explorer-source-provenance-by-tab` · `npm run test:operator-explorer-os` · dry-runs: tab-factory / section-pattern / source-provenance / baseline-gap-remediation / `operator-explorer-os` (`--source=fixtures|merged`)
- **Next Operator Explorer:** when `npm run operator-explorer-os -- --dry-run` reports `canStartNextOperatorExplorer=true` — queue head is GHL Hoteles (`reciI2tYQBfMoMK9G`). Checklist: `docs/data-intelligence/operator-explorer-ready-for-next-operator.md`. Scaffold: `npm run operator-explorer-factory-init -- --operators ghl-hoteles --dry-run`
- Fixtures: `fixtures/operator-*-arbor-cala.json`, `fixtures/operator-*-he-cala.json`
- Do not remediate golden operators without an explicit baseline revision task

## CALA / Travel Infrastructure backfills

- Audit (read-only): `npm run audit:<country>-ti` or `node scripts/audit-market-travel-infrastructure.mjs --country … --market … --output data/…`
- Backfills: always `--dry-run` first; outputs under `data/`
- Submarket options: `npm run ensure:radar-submarket-options` with `--dry-run` before apply

## GTM / Master To-Do

- Structure audit: `node scripts/audit-dealality-master-todo-structure.mjs --dry-run`
- Upsert: `node scripts/upsert-dealality-master-todo.mjs --dry-run` before apply
- Seed test: `npm run test:dealality-master-todo`
- Daily views setup: `reports/founder-project-plan-daily-views-manual.md`

## Hotel Census geography (product)

- **Dealality-defined geography is authoritative** for product UI (Scout, property profiles, Radar). Do **not** recommend STR Market/Submarket as the enrichment target.
- **`Market`** = Dealality commercial market (e.g. `Puerto Rico`, `Greater Santo Domingo`) from `lib/radar-buildout/country-configs.js` / steward assignment.
- **`Submarket`** = Dealality corridor label (e.g. `East Coast / Island Access`) via `lib/hotel-census/census-dealality-submarket.js` — replaces legacy STR `*Regional` buckets.
- STR Excel import paths for Market/Submarket are **legacy reference only**; corridor backfill: `scripts/plan-hotel-census-dealality-submarket.mjs`.
- **Do not cite STR as census lineage** in user-facing explanations or enrichment notes — use Dealality Hotel Census + Affiliation/Parent Company.
- **Normalize legacy `name` suffixes** — strip STR-era `, a Member of {Brand}` from census display names; identity lives in Affiliation (`scripts/apply-design-hotels-census-name-normalize.mjs`).

## Common mistakes (append when you fix one)

- Treating STR Market/Submarket as product geography — use Dealality Market + corridor Submarket instead.
- Explaining census gaps as "STR-sourced" or leaving `a Member of Design Hotels` in census `name` — use Affiliation + canonical property names.
- Assuming new-base operator writer is primary — it is **not** unless `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1`.
- Inventing Airtable select options — options must exist in schema doc or be created via `ensure-*` scripts first.
- Stripping Recent Momentum announcement URLs or shipping untitled diligence blobs — use openings/press cards + `buildRecentMomentumCard` (see Recent Momentum contract).
- Atelier HTML tests / evidence gates that omit `URL` in the VM sandbox — `isSafeHttpUrl` then fails and raw URLs stay in momentum body text; always expose `URL`/`URLSearchParams` in `brand-explorer-atelier-render-test-loader.js`.
- Running GTM CoStar import/apply without dry-run — always preview JSON/CSV first.
- Patching brand explorer fixtures without running gap audit afterward.
- Using stale 23/24/27/39/45/46-brand lists or PRIMARY_RELEASE as the Active/Live Brand Explorer universe — use Brand Status Active/Live + protected 54 public-full baseline gates.
- Using `Done` for master todo status — Airtable uses **`Completed`** (see `docs/dealality-master-todo.md`).
- TI audits fall back to Cancún keywords only for **Mexico** — other countries need config in `lib/radar-buildout/market-travel-infrastructure-audit-configs.js`.
- IHG/Marriott Scene7 gallery URLs — `-2x1` / `-4x3` / `-3x2` are the **same photograph**; uniqueness must collapse them (`image-uniqueness-v2`). Never invent Exterior/Lobby/F&B captions when DAM type codes / fixture `role` are missing.
- Brand Explorer `overview.scenario.1–3` as “Reference / International Reference Comparison / source pack / match by property name / keep geography labels” copy — those cards must be **three distinct owner-value topics** (Kimpton/Curio/Design Hotels bar). Run `npm run brand-explorer-scenario-owner-value-bar-audit` before shipping scenario changes.
- Brand Explorer gallery mono-packs / repeat exteriors — `materials.gallery.1–6` must prefer **CALA properties when inventory exists**, mix Exterior / Guest Room / Public Space / F&B when available, and use multiple properties (not one hotel × 6). Fall back to International Reference only when no CALA photos exist. Gate: `lib/partner-intelligence/brand-explorer-gallery-selection.js`.
- Brand Explorer Value Creation Scenarios as blanks, one-liners, or one long blob — must be **four** `valueOwners.scenario.1–4` short paragraphs (Ascend gold bar). Run `npm run brand-explorer-value-creation-scenarios-audit` before shipping.
- Brand Explorer trust footnote missing on some profiles — do **not** rely on External Display Status / Presentation rows; every profile must render AI-Assisted Profile · Last Reviewed · Source Basis · Region via `applyBrandExplorerAiAssistedFootnote` (`ai_assisted_profile_footnote_visible` gate).
- After a factory cohort graduates to Active/Live, add durable `EXTRA_ACTIVE_IDENTITY_ANCHORS` (Wave 13 Accor / Wave 14 Marriott / Wave 15 Hilton pattern). Missing anchors → PVQL `brand_not_found` / protected baseline flake.
- Wave 15 Hilton CDN images: do **not** proxy `hilton.com/im/` through wsrv (404); append Hilton `impolicy` sizing in `toAirtableFetchableImageUrl`.
- Wave 15 momentum evidence: register CALA posture in `CALA_AVAILABLE_BY_SLUG` or CALA-labeled cards fail as missing International Reference.
- Census geocode apply blocked without `MAPBOX_PERMANENT_GEOCODING=1` or `GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED=1`; provenance backfill must not change lat/lng.
- Census Rooms / Keys: never trust VIC IHG `rooms=22` claims (JS `\x22rooms` false positive); High-only official hotel counts via `census:queue-run --queue rooms_keys_missing`.
- Census production processing is **parent company + region** or `--scope active-brand-setup` via `npm run census:autopilot` with `--strategy fastest-safe`, `--run-until-complete`, and `--batch-size` (chunk only). Prefer `--mode production-cycle` + confirms + `--enable-production-writes` for continuous High allowlisted Hotel Property Census writes (no per-bundle ChatGPT gate). Brand Setup/Explorer are read-only. Webhound is hard-case learning only.

## PR checklist

See **`docs/dealality-pr-validation-matrix.md`** — run the scripts for your changed paths and note risk tier before merge.

```bash
npm run dealality:pr-check-suggest
```

## Parallel / overnight work

See **`docs/dealality-parallel-dry-run-experiment.md`** — start with read-only audits; no live Airtable writes in parallel until validation pipeline is trusted.

## Planning multi-option UX

Use Canvas or HTML mockups (see `engagements/*/`) before coding Webflow/JS when there are 3+ viable UX options. Schema/data tasks use markdown + dry-run reports, not mockups.
