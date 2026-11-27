# Dealality — project memory for agents

> **Last updated:** 2026-08-19  
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
7. **Dealality GitHub merge discipline + integrated delivery** — product work in registered `ws-*` homes (HE/ADP/GDI/BE); short-lived feature branches; no automatic per-task worktrees; IC-authorized merge into `main` then `npm run dealality:post-merge-sync` so product homes return to home + FF `origin/main` when safe; refresh canonical `:8080` and verify cumulative app. See `.cursor/rules/gdi-github-branch-merge-discipline.mdc`, `docs/ai-build-system/POST_MERGE_WORKSPACE_SYNC.md`, and **`docs/ai-build-system/DEALALITY_INTEGRATED_DELIVERY_V1.md`**. Opening a PR is not completion. Normal founder-review status: **`INTEGRATED_ON_8080 — AWAITING_FOUNDER_REVIEW`**. Do not routinely stop at PR OPEN / AWAITING_INTEGRATION_CONTROLLER. Product streams initiate `npm run dealality:remote-pr-merge -- --pr <n>` when CI is green (IC retains merge authority). Workspace closeout still uses `COMPLETE — MERGED_AND_HOME_SYNCED` / `BLOCKED — …` after merge+sync.
8. **Radar authenticated-app acceptance** — Radar/HE visibility, filter, search, map, auth, HE navigation, or dynamic HPC Open/Pipeline changes require PASS on `http://localhost:8080/app#/opportunity-radar` (not API 200 / unit / demo page / CI alone). States: PASS / FAIL / BLOCKED; tests cannot upgrade BLOCKED→PASS. See `docs/ai-build-system/RADAR_AUTHENTICATED_APP_ACCEPTANCE_GATE.md` and `.cursor/rules/dealality-radar-authenticated-acceptance-gate.mdc`.
9. **Universal PR completion + workspace ownership** — merge ≠ done. Owning permanent home must return to `ws/*` (or Integration `main`), sync `origin/main`, correct Cursor branch, retire feature branches when safe, and report `COMPLETE — MERGED_AND_HOME_SYNCED — CLEAN_WORKTREE|PRESERVED_DIRTY` or a `BLOCKED — …` status. Cross-stream checkouts fail closed. See `docs/ai-build-system/PR_COMPLETION_AND_WORKSPACE_OWNERSHIP.md`.
10. **Airtable persistence gate** — durable intelligence cannot be FULL E2E COMPLETE from UI/API/PDF/local JSON alone. Require Airtable SoT mapping, idempotent persistence, retrieval proof, provenance/history, and an explicit classification (`PROTOTYPE` → `CUSTOMER_PUBLISHED`). See `docs/ai-build-system/AIRTABLE_PERSISTENCE_GATE.md`.
11. **Additive development & non-regression** — new work must be additive; do not silently regress accepted UI, inventory, data access, or customer surfaces. Compare before/after by record IDs; IC blocks unintentional regression; intentional material change needs founder approval. Complete only when new **and** prior accepted functionality both work. See `docs/ai-build-system/DEALALITY_ADDITIVE_NON_REGRESSION_PRINCIPLES.md`.
12. **GDI Full E2E completion = STAGED on :8080** — permanent lifecycle is **FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT**. Operational statuses: RESEARCH IN PROGRESS / HOLD / STAGED / CURRENT. Proposed Qualified and similar labels are internal only (not founder gates). An authorized E2E run auto-stages the complete qualified inventory; CURRENT requires explicit founder approval of the exact package version. Reports/tests/write-sets alone ≠ complete. Docs: `GDI_FULL_E2E_COMPLETION_AND_PUBLICATION_LIFECYCLE_V1.md`, `GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1.md`. Gates: `test:gdi-canonical-e2e-to-staged-lifecycle-v1`, `Invoke-GdiValidateFullE2E.ps1`.
13. **Inter-prompt non-regression (binding)** — before any implementation: `npm run dealality:assignment-preflight -- --stream <gdi|he|adp|be|integration>`. Contract **`DEALALITY_INTER_PROMPT_NON_REGRESSION_V1`**. Verifies home/workspace, `origin/main`, cumulative functionality manifest, live `:8080` listener **server.js path** (not meta PID alone), and Wyndham STAGED tip. If FAIL → STOP feature work; reclaim via `npm run dealality:gdi-local-8080` + `npm run dealality:canonical-8080-guard -- --once`; restore via IC. `:8080` ownership requires **cwd AND serverScriptDir** match authorized home (Integration cwd + foreign `_verify_* /server.js` is denied). Feature streams use 8091/8092/8093/8095 only.

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
- `docs/ai-build-system/RADAR_AUTHENTICATED_APP_ACCEPTANCE_GATE.md` — HE/Radar authenticated `/app#/opportunity-radar` gate
- `docs/ai-build-system/PR_COMPLETION_AND_WORKSPACE_OWNERSHIP.md` — universal home-sync completion gate
- `docs/ai-build-system/AIRTABLE_PERSISTENCE_GATE.md` — durable intelligence SoT / E2E classification gate
- `docs/ai-build-system/DEALALITY_ADDITIVE_NON_REGRESSION_PRINCIPLES.md` — additive development / non-regression gate
- `docs/ai-build-system/DEALALITY_INTEGRATED_DELIVERY_V1.md` — end-to-end integrated delivery → `:8080` founder review
- `docs/ai-build-system/GDI_FULL_E2E_COMPLETION_AND_PUBLICATION_LIFECYCLE_V1.md` — GDI E2E → STAGED → CURRENT completion standard
- `docs/ai-build-system/GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1.md` — auto-STAGED lifecycle contract

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

- Counting completed/committed/renewed/retained/awarded hotel deals as open **Qualified** Opportunities (e.g. AC Cartagena reflag, GHL–Sonesta MFA to 2034, appointed operators) — preserve as Historical/Committed/Retained; Qualified only when BCT/MCT exists, remains unresolved, and a credible competitive pathway is evidenced. Gate: `lib/hotel-intelligence/dealsignal/dealsignal-qualified-opportunity-classification-v1.js` · `npm run test:dealsignal-qualified-opportunity-classification-v1` · rule `.cursor/rules/dealsignal-qualified-opportunity-classification.mdc`.
- Putting Dealality infrastructure in customer dossiers (Census, Airtable, Webhound, Research Center, fixtures, auto_promote, ontology codes) — reports are external-facing; see `docs/data-intelligence/hotel-intelligence-external-facing-report-copy.md`.
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
- Brand Explorer Image Gallery is **hotel property photography only** (exterior / interior / labeled renderings) for **all brands** — Curio/Ascend bar. Never people/lifestyle marketing, guest/staff portraits, or cocktail close-ups. Captions must name the hotel (or International Reference) **and** the actual scene; never label a lounge/bedroom/portrait as Exterior. Permanent: `be-permanent-six-image-gallery-standard-v1.js` (`isPeopleOrLifestyleMarketingImage`, `galleryRoleToSceneLabel`); Factory gate `npm run test:be-factory-2-gallery-hotel-only-v1`; remediate `npm run be-factory-2-remediate-gallery-hotel-only`.
- Brand Explorer **Where This Brand Creates the Most Value** (`overview.scenario.1–3`) must show hotel exterior / interior / rendering photos for **all brands** — never empty IMAGE placeholders when a hotel photo pool exists. Gate: `npm run test:be-factory-2-scenario-hotel-images-v1`; remediate `npm run be-factory-2-remediate-scenario-hotel-images`.
- Brand Explorer **Requirement Area** titles (`standards.requirement`) must be **Proper Case** and use **&** (never standalone "and") for **all brands** — Factory permanent rule via `toEditorialTitleCase`. Gate: `npm run test:be-factory-2-requirement-area-title-case-v1`; remediate `npm run be-factory-2-remediate-requirement-area-title-case`.
- Brand Explorer **Openings / Examples** (`footprint.openings`) must show hotel exterior / interior / labeled renderings for **all brands** — never people/lifestyle; never blank property cards when a hotel photo pool exists. Gate: `npm run test:be-factory-2-openings-hotel-images-v1`; remediate `npm run be-factory-2-remediate-openings-hotel-images`.
- Brand Basics **Brand Positioning** (`fld8jsY5nekd6AMah`) is the canonical concise market-proposition attribute (Title Case, no slashes, not Chain Scale). Shown under Overview **Brand Positioning** (not duplicated in Brand Snapshot Product & Segment; brand tiles use Value Proposition / Customer Promise / Tagline, never short Positioning). Enrich: `npm run be-enrich-brand-positioning` (`--apply` founder-authorized). Gate: `npm run test:be-brand-positioning-canonical-v1`. Mapping: `fixtures/brand-positioning-master-mapping-v1.json`.
- Brand Explorer images (Gallery / Scenarios / Openings / Materials) must be **same-brand only** — never sibling-flag photography (e.g. Hilton Hotels & Resorts assets on Waldorf Astoria). Gate: `npm run test:be-factory-2-same-brand-images-v1`; remediate `npm run be-factory-2-remediate-same-brand-images`. Module: `be-permanent-same-brand-image-affinity-v1.js`.
- Brand Explorer Value Creation Scenarios as blanks, one-liners, or one long blob — must be **four** `valueOwners.scenario.1–4` short paragraphs (Ascend gold bar). Run `npm run brand-explorer-value-creation-scenarios-audit` before shipping.
- Shipping two Executive Summary tiles with the same category (e.g. two “Largest Competitive Gap”) — enforce unique finding type/title via `selectDiverseFindings`.
- Brand Explorer trust footnote missing on some profiles — do **not** rely on External Display Status / Presentation rows; every profile must render AI-Assisted Profile · Last Reviewed · Source Basis · Region via `applyBrandExplorerAiAssistedFootnote` (`ai_assisted_profile_footnote_visible` gate).
- Stage B stability / Narrative & Source Intelligence reads — use **only** `reports/ai-visibility/repeated-testing-stage-b-report-final-wave.json` (wave `aiv_stability_stage_b_20260817_f0a829`); never aggregate recurrence from `22c195` or `repeated-testing-stage-b-report.json`.
- Brand longitudinal measurement — `docs/ai-visibility/brand-longitudinal-measurement.md`; cohort `BRAND_LONGITUDINAL_COHORT_V1`; **SYNTHETIC_HISTORY=0**; Stage B same-day repetitions ≠ trend dates; first multi-parent period `aiv_brand_longitudinal_period_20260818_6579d2` (`DEMO_VALIDATION`, peer v3 = frozen v2 + Radisson); live Brand AI comparative reads stay on peer v2; **monitoring universe v4 = all 19 showcase portfolio brands**; store under `data/ai-visibility/runtime/brand-longitudinal/` (not Airtable); **SCHEDULER_ENABLE=0**; gates `npm run test:ai-visibility-brand-longitudinal-foundation` and `npm run test:ai-visibility-radisson-longitudinal-gate`. **Owner Intent Chg vs Prior Run V1** — compact column shows certified index-point change only when the live exact scope has a comparable certified prior; two periods ≠ trend; live current remains federated `2026-08-14` until a governed join; gate `npm run test:brand-ai-owner-intent-chg-vs-prior-v1`; docs `docs/ai-visibility/owner-intent-chg-vs-prior-v1.md`.
- Operator AI Intelligence is a **separate** Market Intelligence page (`/operator/ai-intelligence`), not a Brand AI tab. Universe lock = 9 Operator Master IDs (includes Remington Hospitality CALA `rec6UB6RpMKSs2tAo`); grain = prompt × provider; Presence = `OPERATOR_SIGNAL_PRESENCE` (**PRODUCTION_VALIDATED** on live corpus `aiv_operator_presence_validation_20260818_1342_20ee11`). Competitive intelligence V1 = Questions Missing + derived All Providers + diagnostic Competitive Gap; **partial client promotion** of CORE same-model gaps only; Arbor stays `INSUFFICIENT_OPERATOR_SPECIFIC_EVIDENCE`. Gate `npm run test:operator-competitive-intelligence-v1`. Docs: `docs/ai-visibility/operator-ai-intelligence-foundation.md`, `docs/ai-visibility/operator-competitive-intelligence-v1.md`.
- **Competitive moat architecture V1** — SHOW THE BENCHMARK / HIDE THE BENCHMARK ENGINE; validated signals only; no Recommendation/Preference/Win-Loss client promotion. **Customer prompt disclosure V1** — SHOW WHAT WE MEASURE / PROTECT EXACTLY HOW WE MEASURE IT; customer UI/API expose Owner Intent + Decision Context only; canonical production prompts INTERNAL_ONLY; server redaction via `customer-prompt-disclosure.js`; gate `npm run test:brand-ai-customer-prompt-moat-ui-v1`; docs `customer-prompt-disclosure-policy-v1.md`. Canonical intent bridge + observation ledger schema + `BENCHMARK_ENGINE_V1` (Presence-only **AI Presence Index**). Customer payloads allowlisted at API layer. **Internal benchmark (peer v5) may exceed customer-visible 19-brand portfolio** — never expand dropdown merely to strengthen benchmarking. Pilot peer set: `peers_uu_collection_lifestyle_owner_decision_v5` = frozen v2 + 7 INTERNAL_BENCHMARK_ONLY brands; `npm run brand-presence-index-pilot:run`, `npm run test:brand-presence-index-pilot-v1`. **BENCHMARK RELEVANCE FIRST** — scenario-specific commercial cohorts; **NO UNION-GRAIN PRESENCE BENCHMARKS**; **NO BROAD PEER FALLBACK**; **MEASUREMENT GAP ≠ NON-COMPARABLE**. Eligible universe `benchmark_eligible_brands_v1` (includes Vignette/Voco/Radisson even when absent from v5). Headline index and mean/median certification **DEFERRED**. Scenario-index measurement math **certified** (45/45 EXACT_MATCH). **CORE-first denominator** (`CORE_BENCHMARK_PLUS_SECONDARY_CONTEXT`) — SECONDARY is context, not the median. **Final scenario certification (3 rows):** Autograph/Tapestry soft-brand 103, Ascend soft-brand 67 — `PRODUCTION_VALIDATED`; customer index rendering **EXACT_SCOPE_CERTIFIED** (All Providers + any provider scope that passes independent certification; never cross-fill). **Provider-scoped certification V1:** registry `reports/ai-visibility/provider-scoped-benchmark-certification-v1.json`; prompt→scenario bridge `prompt-scenario-bridge.js` (governed promptId metadata precedes display labels); audit `npm run provider-scoped-benchmark-certification:run`; gate `npm run test:brand-ai-provider-scoped-benchmark-certification-v1` + `npm run test:brand-ai-owner-intent-provider-ui-v1`. **Expansion audit V2:** next 3 = Curio/Tribute/Vignette soft-brand after the next Brand longitudinal period (provider-conflict; $0 special wave). **Longitudinal recertification readiness V1:** period-scoped indices only (`CROSS_PERIOD_DEDUPLICATION=PROHIBITED`); post-wave offline recert via `npm run scenario-benchmark-longitudinal-recertification:run -- --current-period=<id>`; gates `npm run test:scenario-benchmark-longitudinal-recertification-v1`. Gates: `npm run scenario-benchmark-final-certification:run`, `npm run test:scenario-benchmark-final-certification-v1`, `npm run scenario-benchmark-certification-expansion-audit:run`, `npm run test:scenario-benchmark-certification-expansion-audit-v2`. Docs: `benchmark-cohort-contract-v2.md`, `scenario-peer-eligibility-v1.md`, `brand-presence-index-remediation-v1.md`, `scenario-benchmark-validation-v1.md`, `scenario-benchmark-composition-v1.md`, `scenario-benchmark-certification-expansion-audit-v2.md`, `benchmark-engine-v1.md`, `competitive-data-access-policy-v1.md`; module `lib/ai-visibility/competitive-moat/`.
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

## Executive copy governance

- For Brand AI Executive Summary findings, follow `docs/ai-visibility/executive-intelligence-copy-contract.md`.
- Preserve semantic distinction: brand `appears/is represented` vs source `cited/citation`.
- Evidence lines must use one coherent construct only (Association vs Narrative vs Truth vs Provider Comparison), with no blended denominators.
- Executive Summary tiles must have unique finding types and category titles — never two “Largest Competitive Gap” cards.

## Parallel / overnight work

See **`docs/dealality-parallel-dry-run-experiment.md`** — start with read-only audits; no live Airtable writes in parallel until validation pipeline is trusted.

## Planning multi-option UX

Use Canvas or HTML mockups (see `engagements/*/`) before coding Webflow/JS when there are 3+ viable UX options. Schema/data tasks use markdown + dry-run reports, not mockups.
