# UMusic Hotels — current Brand Explorer build-system discovery

**Date:** 2026-10-13  
**Tree inspected:** `origin/main` at `d3a3f5f9` (branch `cursor/umusic-build-system-discovery-3df5`)  
**Method:** repository trace only. Wave reports and batch write-ups were not treated as the current contract.  
**Outcome:** `umusic_hotels_brand_explorer_build_architecture_requires_consolidation`

This discovery did not create a Brand Basics record, Presentation rows, or a preview URL.

---

## Build pipeline

There is no single path from brand identity to a master release review.

What exists today is several overlapping generators, plus npm script names for a later contract layer whose files were never committed.

### Path that docs still call the permanent setup

`docs/data-intelligence/brand-explorer-tab-factory-build-operation.md` describes this sequence:

1. Source pack
2. Brand lens
3. Tab-by-tab generation
4. Field-by-field tab validation
5. Source provenance by tab
6. Rendered payload audit
7. Patch / suppress / unavailable-state remediation
8. Image distinctiveness validation
9. Golden benchmark comparison
10. Founder visual review
11. Active release only after gates pass

Code that implements pieces of that sequence:

| Step | Entry | Writes Airtable? |
| --- | --- | --- |
| Readiness router | `npm run brand-explorer-os` → `scripts/brand-explorer-os.mjs` → `lib/partner-intelligence/brand-explorer-os-run.js` | No. Refuses `--apply`. |
| Tab audit | `npm run brand-explorer-tab-factory-audit` | No |
| Tab remediation | `npm run brand-explorer-tab-factory-remediation` | Presentation only, with confirm flags |
| Lane-2 full Presentation build | `npm run brand-explorer-full-tab-factory-build` → `lib/partner-intelligence/brand-explorer-full-tab-factory-build.js` | Presentation only, allowlisted slugs, confirm flags |
| Founder packet | `npm run brand-explorer-v42-founder-visual-review` | Review packet, not status promotion |
| Active release | `brand-explorer-v43-active-release-apply` and later | Release fields. Out of scope for Under Review. |

OS canonical states live in `lib/partner-intelligence/brand-explorer-os-state-machine.js` (`V41_OS_VERSION = "v41"`):

`not_started`, `sources_seeded`, `knowledge_ready`, `asset_ready`, `draft_ready`, `draft_applied_with_defects`, `internal_preview_blocked`, `internal_preview_ready`, `founder_review_ready`, `active_release_ready`, `active_profile_ready`, `state_conflict`.

`resolveCanonicalBrandState` never returns `knowledge_ready`, `draft_ready`, or `internal_preview_ready`. Those names are dead.

`founder_review_ready` is reached only when visuals, live internal preview, residual Presentation copy, and the mandatory tab-factory bundle pass, and `Founder Visual Review Pass` is still false.

### Paths that are still executable and cohort-specific

These are not historical archives. Each has a script and an apply stage.

| Generation | Entry | What it builds |
| --- | --- | --- |
| Choice P1 factory | `scripts/brand-explorer-factory.mjs` | Fixture → restore. Optional `--apply`. |
| Wave 12–15 factories | `scripts/brand-explorer-wave{12,13,14,15}-factory.mjs` | Parent-cohort stages: preflight, manifest, factory-preview cohort, source packs, Basics create (wave 13 open-items), tab build, image materialization, founder review, status promotion, public release. |
| Wave 16A | `scripts/brand-explorer-wave16a-stage*.mjs` | Marriott source foundation, controlled tab build, images, promotion. |
| Wave 17 | `scripts/brand-explorer-wave17-*.mjs` | Hyatt foundation and batch tab/image/promotion. |
| v36–v40c | `scripts/brand-explorer-v36*` through `v40c*` | Contract audit, lifestyle seeding, display lock, economics chrome. Some apply Presentation. |
| v42–v47 | founder review, active release, image batches | Mixed apply. |
| Wyndham queue | `lib/partner-intelligence/brand-explorer-wyndham-factory-build-queue.js` | Separate non-Active queue. |

Wave 13 Basics create (`lib/partner-intelligence/brand-explorer-wave13-factory-plan.js`, `WAVE13_BASICS_FIELD_MAP`) writes only `Brand Name`, `Brand Status`, `Parent Company`, `Internal Notes`, and requires `Brand Status = Under Review`. That is a cohort script, not a general brand constructor.

There is no Brand Explorer `factory-init`. `scripts/operator-explorer-factory-init.mjs` is Operator Explorer only.

### Path named in package.json that does not exist

Commit `ac04cc4d` (2026-09-04) added these npm scripts. `git log --all` has no blobs for the target files. They are absent on `origin/main` and on the recovery / Fairfield branches checked.

| npm script | Missing file |
| --- | --- |
| `brand-explorer-atomic-collection-audit` | `scripts/brand-explorer-atomic-collection-audit.mjs` |
| `test:brand-explorer-atomic-collection` | `scripts/test-brand-explorer-atomic-collection.mjs` |
| `brand-explorer-atomic-collection-remediation-apply` | `scripts/brand-explorer-atomic-collection-remediation-apply.mjs` |
| `brand-explorer-visual-geometry-audit` | `scripts/brand-explorer-visual-geometry-audit.mjs` |
| `test:brand-explorer-visual-geometry` | same missing audit script |
| `brand-explorer-factory-backscan` | `scripts/brand-explorer-factory-backscan.mjs` |
| `test:brand-explorer-master-release-contract` | same missing backscan script |
| `brand-explorer-quality-lessons-upgrade` | `scripts/brand-explorer-quality-lessons-upgrade.mjs` |

Also missing, repo-wide:

- `configs/brand-explorer-quality-lessons.json`
- `configs/brand-explorer-content-item-contract.json`
- `configs/brand-explorer-collection-cardinality-contract.json`
- `configs/brand-explorer-presentation-contract.json`
- `configs/brand-explorer-visual-geometry-contract.json`
- the string `MASTER_RELEASE_CONTRACT_PASS`
- `backscanRequired`
- any quality-lesson registry in `lib/` or `scripts/`

`npm run test:brand-explorer-master-release-contract` cannot run.

### Why these paths are incompatible

1. **UI vs tab contract.** The renderer has 10 atelier tabs. `TAB_CONTRACTS` in `lib/partner-intelligence/brand-explorer-tab-contracts.js` covers 7. Commercial Engine, Loyalty Program, and Brand Materials are enforced only through slot manifests and section minimums, not the tab-factory field matrix.
2. **OS vs docs.** `docs/data-intelligence/brand-explorer-mandatory-release-gates.md` lists `section_pattern_parity` and `image_role_match` as release gates. Section pattern is folded inside `tab_factory_audit`. `image_role_match` is an OS gate and an action-router block, but `resolveCanonicalBrandState` does not read `imageRoleMatchPass`.
3. **Footnote gate.** Tab-factory docs require `ai_assisted_profile_footnote_visible`. That name is not an OS gate in `evaluateBrandExplorerOsGates`.
4. **Stale OS cohort.** `PRIMARY_RELEASE_SLUGS` is still the seven-brand v41 set (Everhome, Kimpton, Radisson Individuals, Design Hotels, Hotel Indigo, MGallery, SLH). The binding public universe is Brand Status Active/Live, frozen as the protected 62 in `docs/data-intelligence/brand-explorer-protected-baseline-rules.md`.
5. **Preview allowlists diverge.** Server factory preview (`FACTORY_PREVIEW_VERSION = factory-preview-mode-v6-wave16a-stage2a`) allowlists Fairfield, Four Points, and Delta. The client also hardcodes a candidate slug list in `public/js/brand-explorer-atelier-from-api.js`. A new brand is not previewable until both are updated.
6. **Named master contract has no implementation.** Atomic collection, visual geometry, quality lessons, and master release are script names only.
7. **Geography labels disagree with the taxonomy this task expected.** The live footprint cards are Americas, CALA, Europe, MEA, APAC. See Contracts below.

Building UMusic on any one of these paths would not satisfy the others.

---

## Files/modules

### Identity and universe

| Concern | Location |
| --- | --- |
| Brand fetch / field map | `api/brand-library.js` (`F.brandBasics`, Presentation normalizer) |
| Active universe | `lib/brand-status-active.js`, `lib/partner-intelligence/brand-explorer-active-universe.js` |
| Protected baseline rules | `docs/data-intelligence/brand-explorer-protected-baseline-rules.md` |
| Freeze list | `docs/data-intelligence/brand-explorer-62-active-public-full-baseline.md` |
| Basics table | `Brand Setup - Brand Basics` |
| Presentation table | `Brand Setup - Brand Explorer Presentation` |
| Evidence | `Partner Intelligence - Source Library`, `Partner Intelligence - Extracted Facts`, `Partner Intelligence - Brand Asset Registry` |
| Slot catalog | `docs/brand-explorer-presentation-slots.md` |
| Brand Status options at runtime | Airtable Metadata API via `getBrandStatusOptions` in `api/brand-library.js`. Repo-enforced values used by product code: `Active`, `Live` (public universe), `Under Review` (factory), `Draft` (some seed scripts). |

Child Brand Setup tables loaded with Basics (comment block in `api/brand-library.js`): Footprint, Project Fit, Portfolio & Performance, Brand Standards, Fee Structure, Deal Terms, Operational Support, Legal Terms, Loyalty & Commercial, Sustainability & ESG.

### Build, audit, remediate

| Concern | Location |
| --- | --- |
| OS gates | `lib/partner-intelligence/brand-explorer-os-gate-evaluator.js` (`evaluateBrandExplorerOsGates`) |
| OS state | `lib/partner-intelligence/brand-explorer-os-state-machine.js` |
| OS next action | `lib/partner-intelligence/brand-explorer-os-action-router.js` |
| Tab contracts | `lib/partner-intelligence/brand-explorer-tab-contracts.js` |
| Tab evaluate / audit / remediate | `brand-explorer-tab-factory-evaluate.js`, `brand-explorer-tab-factory-audit.js`, `brand-explorer-tab-factory-remediation.js` |
| Full-tab slot families | `lib/partner-intelligence/brand-explorer-full-tab-content-contract.js` |
| Plan-row checks | `lib/partner-intelligence/brand-explorer-presentation-plan-contract.js` |
| Slot inventory | `lib/partner-intelligence/brand-explorer-slot-standard-manifest.js` |
| Section minimums | `lib/partner-intelligence/brand-explorer-required-section-population-contract.js` |
| Field completeness | `brand-explorer-rendered-field-completeness-inventory.js`, `brand-explorer-rendered-field-completeness-evaluate.js` |
| Source provenance | `lib/partner-intelligence/brand-explorer-source-provenance-by-tab.js` |
| Empty components | `lib/partner-intelligence/brand-explorer-no-empty-rendered-components.js` |
| Golden copy | `lib/partner-intelligence/brand-explorer-golden-content-quality.js` |
| Section pattern | `lib/partner-intelligence/brand-explorer-section-pattern-parity.js` |
| Display state | `lib/partner-intelligence/brand-explorer-display-state.js` |
| Factory preview | `lib/partner-intelligence/brand-explorer-factory-preview-candidates.js` |
| HTML test render | `lib/partner-intelligence/brand-explorer-atelier-render-test-loader.js` |
| Batch learning (not a lesson registry) | `lib/data-intelligence/dealality-batch-learning-system.js` |

### Renderer

| Concern | Location |
| --- | --- |
| Combined page | `public/brand-explorer-combined.html` |
| Atelier renderer | `public/js/brand-explorer-atelier-from-api.js` |
| Route query | `public/js/brand-explorer-route-state.js` (`id`, `brandId`, `brand`, `name`) |
| Share shell | `public/brand-explorer-share.html` (`pack=choice|ihg|hilton`) |
| Fetch | `public/js/brand-explorer-brand-fetch.js` → `GET /api/brand-library/brand?brandId=` |

---

## Contracts

The files named in the task brief do not exist. The contracts below are the ones the code actually enforces. They do not form one atomic ledger with a single denominator of `BASE_CANONICAL_ITEMS + INSTANTIATED_COLLECTION_CHILDREN`.

### Content

Renderer tabs (`ATELIER_TAB_DEFS` in `public/js/brand-explorer-atelier-from-api.js`):

| tabId | Label |
| --- | --- |
| `atelier-overview` | Overview |
| `atelier-value-owners` | Value to Owners |
| `atelier-ops` | Operating Model |
| `atelier-standards-owner` | Owner Considerations |
| `atelier-commercial` | Commercial Engine |
| `atelier-economics` | Economics & Obligations |
| `atelier-loyalty` | Loyalty Program |
| `atelier-footprint` | Footprint & Growth |
| `atelier-materials` | Brand Materials |
| `atelier-insight` | Dealality Insight |

`TAB_CONTRACTS` tab names: Overview, Value to Owners, Operations & Standards, Owner Considerations, Economics & Obligations, Footprint & Growth, Dealality Insight. The Operations tab name in the contract does not match the UI label Operating Model.

Slot families and coded cardinality (from section population, completeness inventory, and dedicated contracts — not from a collection-cardinality JSON):

| Collection | Slots | Coded rule |
| --- | --- | --- |
| Overview scenarios | `overview.scenario.1–3` | Exact 3 |
| Proof points | `overview.proof.1–4` | 4 slots |
| Key differentiators | `overview.differentiators.identity`, `.commercial` | ≥3 bullets each |
| Best at | `overview.bestAt.1–3` | 3 cards |
| Value creation scenarios | `valueOwners.scenario.1–4` | Exact 4; 26–58 words (`brand-explorer-value-creation-scenarios-bar.js`) |
| Owner lifecycle | `valueOwners.lifecycle.1–6` | 6 phase boxes |
| Flexibility | six `operations.flexibility.*` keys | Exact 6; body is one canonical level |
| Operating model KV | `operations.model.*` | 12 fields in inventory |
| Compliance | `operations.compliance.*` | 4 cards |
| Owner considerations table | `standards.requirement` | Golden ≥6 rows; population contract ≥1 table package |
| Owner questions | `standards.questions` | ≥5 bullets |
| Commercial / demand | `commercial.demand` | ≥3 rows, target 6 |
| Economics opening path | `economics.opening.step.1–5` | 5 phases in inventory; many `economics.*` slots are documented and not wired |
| Loyalty mechanisms | `loyalty.hero_title`, `.earn`, `.redeem`, `.elite`, `.proof` | Minimum 5 of these present |
| Geographic regions | `footprint.region.am\|cala\|eu\|mea\|apac` | 5 rows |
| Growth themes | `footprint.growth_themes` and related editorial slots | ≥2 chips; geo intro ≥30 words |
| Representative properties | `footprint.openings` | ≥3 complete cards |
| Recent momentum | `footprint.momentum` | Contract minimum 2 named cards; population contract ≥3 complete rows |
| Portfolio mix | `footprint.portfolio_mix` | ≥3 rows/chips |
| Similar brands | `insight.similar` | ≥2 rows |
| Gallery | `materials.gallery.1–6` | 6 image URLs for `active_profile_ready` |
| Dealality insight | `insight.summary` | Core required in slot manifest |

There is no coded mode enum of `EXACT` / `MINIMUM` / `ALL_SOURCE_MEMBERS` / `CONDITIONAL` for these collections. Loyalty tiers are not `ALL_SOURCE_MEMBERS`. Builders such as `build-hilton-loyalty-presentation-slots.js` and `build-ihg-loyalty-presentation-slots.js` hardcode parent programs.

### Suppression

No `DEPENDENT_SUPPRESSED_JUSTIFIED`, `APPLICABLE_POPULATED_POSITIVE`, `APPLICABLE_POPULATED_NEGATIVE`, `RESEARCH_REQUIRED`, or `BLOCKED` disposition enum exists.

Actual states:

- Tab contract resolution: `intentionally_suppressed`, `clean_unavailable_state`, plus fail states (`blocked_*`, `needs_patch`).
- Completeness evaluator: `should_suppress` / `suppress_component` for suppressible fields; `cleanly_unavailable` / `replace_with_not_publicly_disclosed` for derived KV without public data.
- Display state: `hidden_incomplete`, `internal_preview_only`, `external_owner_ready`, `active_profile_ready` (`brand-explorer-display-state.js`).
- Openings quarantine for internal language (`brand-explorer-openings-ui-quarantine-governance.js`).
- Presentation plan blocks visible source URLs except trailing announcement URLs on `footprint.openings` and `footprint.momentum`.

Negative answers are not a first-class rendered type. Suppression and “not publicly disclosed” are.

### Facts

There is no brand fact graph shared across tabs. Consistency is indirect: golden content quality, source provenance by tab, and copy scans (`scanOwnerFacingForbiddenLanguage`, internal-preview owner-copy scan). Cross-page contradictions are not blocked by a fact-node id.

### Presentation

Product structure is split across the atelier renderer, `docs/brand-explorer-presentation-slots.md`, the slot manifest, and the presentation plan contract. There is no `configs/brand-explorer-presentation-contract.json`.

Flexibility labels (product-owned levels, not brand-redefinable) in `lib/brand-explorer-flexibility-levels.mjs`:

- Slots: Design, Conversion Friendliness, Localization, Operational Rigidity, PIP Sensitivity, Prototype Dependence
- Levels: `Minimal`, `Low`, `Moderate`, `Medium`, `High`, `Very high`
- Writer: `sanitizeFlexibilityPresentationBody`

### Geometry

No visual-geometry contract and no DOM geometry audit. `brand-explorer-visual-geometry-audit` is an npm alias to a missing script. Image and layout checks that do exist are image uniqueness, image role match, gallery selection, and empty-component HTML scans. They do not measure column widths, title zones, badge alignment, or dead space.

### Properties

`lib/partner-intelligence/brand-explorer-openings-property-card-contract.js`

- Slot `footprint.openings` only
- One hotel per Presentation row
- Title shape `{Property} {Brand} — {City}`
- Forbidden titles containing `— Property Example` or `— CALA Property Example`
- Body: chips, location, meta, optional scenario line, property teaser, optional trailing https URL
- Modal uses Case Summary columns
- Completeness (`openingIsComplete`): title, `imageUrl`, location, summary, URL

There is no separate “representative properties” collection distinct from openings cards, and no atomic property-entity contract with individual modal provenance fields beyond Case Summary.

### Images

`VERIFIED_EXACT_PROPERTY`, `VERIFIED_WRONG_PROPERTY`, `UNVERIFIED_PROPERTY_IDENTITY`, `CROSS_PROPERTY_DUPLICATE`, `MISSING_PROPERTY_IMAGE`, `APPROVED_NO_IMAGE`, and `NON_PROPERTY_VISUAL` are not defined.

Actual image states:

| Mechanism | File | States / thresholds |
| --- | --- | --- |
| Uniqueness v3 | `brand-explorer-image-uniqueness.js` | `missing`, `invalid_url`, `unique_candidate`, `unique`, `duplicate_or_near_duplicate`. Mins: gallery 6, scenario 3, property 3 distinct groups. |
| Display visuals | `brand-explorer-display-state.js` | 6 gallery URLs + 3 openings URLs, plus uniqueness and role match |
| Gallery selection | `brand-explorer-gallery-selection.js` | CALA-first when inventory exists; role mix; max 2 per property when alternatives exist |
| Asset registry | `brand-explorer-visual-slot-requirements.js` | Validation statuses such as Valid for Slot, Needs Property Confirmation, Do Not Use. `propertyConfirmed`: Yes / No / Unknown |

Same-brand donor reassignment exists as an operational distinctiveness fix in `brand-explorer-built-blocked-remediation.js`. That conflicts with an exact-property-only evidence rule, which is not implemented.

### Geography (canonical, from the renderer)

```javascript
FOOTPRINT_REGION_SLOT_DEFS = [
  { slot: 'footprint.region.am', defaultName: 'Americas' },
  { slot: 'footprint.region.cala', defaultName: 'CALA' },
  { slot: 'footprint.region.eu', defaultName: 'Europe' },
  { slot: 'footprint.region.mea', defaultName: 'MEA' },
  { slot: 'footprint.region.apac', defaultName: 'APAC' }
]
```

`North America`, `Caribbean & Latin America`, `Middle East & Africa`, and `Asia Pacific` are not the footprint card defaults. `public/js/brand-explorer-gold-detail.js` still uses older geography labels on optional gold append tabs. A profile must not invent `Europe / other international`. It also must not rename Americas / CALA / MEA / APAC to a different five-region taxonomy unless the product contract changes first.

### Loyalty

`renderLoyaltyProgram` in the atelier renderer. Slots include `loyalty.hero_title`, `loyalty.ecosystem`, `loyalty.owner_lens`, `loyalty.proof`, `loyalty.earn`, `loyalty.redeem`, `loyalty.elite`, KPIs `loyalty.kpi.members|hotels|markets|mix`.

When slots are thin, the renderer falls back to static proof, earn/redeem lists, elite cards, and a `LOY_DEMAND` matrix (`brand-explorer-full-tab-content-contract.js`). There is no canonical “no traditional loyalty program” treatment in a contract file. An empty Loyalty tab can be filled by those fallbacks, which is a product defect for a brand that does not participate in a chain program.

### Momentum

`lib/partner-intelligence/brand-explorer-recent-momentum-contract.js`

- `buildRecentMomentumCard` / `buildRecentMomentumBody`
- Body: date line, summary, trailing `https://` announcement URL
- Newest first
- Minimums inside this contract: 2 cards, 2 URLs, 2 structured dates
- Population contract separately requires ≥3 complete rows
- Forbidden diligence-filler patterns
- Evidence quality: `brand-explorer-recent-momentum-evidence-quality.js`
- CALA posture must be registered in `CALA_AVAILABLE_BY_SLUG` or CALA-labeled cards fail

---

## Release gates

`MASTER_RELEASE_CONTRACT_PASS` does not exist. No code requires expected = activated = measurable = audited = 10 on one server PID. `brand-explorer-atelier-render-test-loader.js` can render all atelier panels in one HTML string. That is not a 10/10/10/10 live session check.

### What OS and the mandatory-gates doc actually require before `founder_review_ready`

From `docs/data-intelligence/brand-explorer-mandatory-release-gates.md` and `evaluateBrandExplorerOsGates`:

1. `source_provenance_by_tab`
2. `tab_factory_audit` (`auditPass` only when `failFindings === 0`)
3. `rendered_field_completeness_audit` / remediation
4. `no_empty_rendered_components`
5. `image_distinctiveness`
6. `image_role_match` (gate + action router; not in the state-machine mandatory block)
7. `section_pattern_parity` (docs; implemented inside tab-factory evaluate, not as its own OS gate name)
8. `golden_content_quality`
9. `brand_specific_source_validation`
10. `ai_assisted_profile_footnote_visible` (docs and PVQL; not an OS gate name)

Additional OS gates in `brand-explorer-os-gate-evaluator.js`:

`brand_record_exists`, `factory_config_exists`, `source_coverage_ready`, `visual_asset_pack_ready`, `gallery_six_imageurl`, `property_examples_three_imageurl`, `required_tab_contract`, `no_fallback_sections`, `no_empty_owner_facing_cards`, `no_visible_source_urls`, `no_forbidden_copy_presentation`, `internal_preview_owner_copy_live`, `external_quality_lock`, `renderer_chrome_clean`, `residual_presentation_clean`, `founder_visual_review_passed`, `active_release_approval_set`, `company_validated_untouched_or_true`.

Company Validated is never auto-written.

The 22-layer list in the task brief (research foundation, atomic content, dynamic enumeration, cardinality, negative intelligence, fact graph, source support, metric semantics, product structure, canonical labels, presentation, visual geometry, atomic entities, property examples, property image evidence, loyalty benefits, loyalty tiers, momentum, similar brands, geography, planned/written/rendered reconciliation, stable live all-tab validation) is **not** implemented as a master contract. Several of those concerns exist only as scattered gates above. Visual geometry, atomic entity contracts, fact graph, metric evidence states (`KNOWN_VALUE`, `KNOWN_ZERO`, `NOT_PUBLICLY_DISCLOSED`, `OUT_OF_SCOPE`, `RESEARCH_REQUIRED`), and live 10/10/10/10 validation do not exist.

### Preview route (current code)

Factory preview, only if the slug is an allowlisted candidate **and** Presentation rows exist:

`/brand-explorer-combined.html?brandId={recordId}&beInternalPreview=1&factoryPreview=1`

`canRenderFactoryPreview` returns false for any slug outside `FACTORY_PREVIEW_CANDIDATE_SLUGS` / `BRAND_EXPLORER_FACTORY_PREVIEW_SLUGS`.

Internal preview without the factory flag (`?beInternalPreview=1`) is a separate renderer mode in `public/js/brand-explorer-atelier-from-api.js`. It is not a general Under Review constructor.

UMusic is not on the factory allowlist. There is no UMusic record id. **No UMusic preview URL exists.**

### Pattern examples (not gold standards)

No automated gold designation was applied. These profiles are the ones current contracts cite for pattern depth only:

| Profile | What current code uses it for |
| --- | --- |
| Tribute Portfolio | Tab-factory / lifestyle benchmark references; momentum link rules |
| Kimpton | Scenario owner-value bar; section-pattern content; fixtures `fixtures/brand-explorer-presentation-kimpton-*.json` |
| Radisson Individuals | Momentum / openings parity writers; protected PVQL |
| Design Hotels | Scenario bar; value-scenario package |
| Ascend Hotel Collection | Value-creation scenario bar (`valueOwners.scenario.1–4`) |
| Everhome Suites | Protected structure bar; `PROTECTED_BRANDS` |
| Curio | Scenario bar; `fixtures/brand-explorer-presentation-curio-full.json` |

Their prose must not be copied into another brand.

---

## Current lesson registry

| Field | Value |
| --- | --- |
| File | Missing. Expected path `configs/brand-explorer-quality-lessons.json` is not in git. |
| Version | None |
| Active lesson count | **0** |
| Validators | None |
| Prevention rules | None |
| Remediation rules | None |
| Mutation tests | None. `test:brand-explorer-visual-geometry` and `test:brand-explorer-master-release-contract` point at missing scripts. |
| Backscan | `brand-explorer-factory-backscan` is missing. `backscanRequired` does not appear in the repo. |

`fixtures/data-intelligence/dealality-batch-learning-seed-entries.json` records past Brand Explorer batch learnings. That ledger is not a cumulative quality-lesson registry and cannot be inherited as 70 active lessons.

A new UMusic defect class cannot be registered in the requested lesson shape, and an Active-68 read-only backscan cannot be run.

---

## UMusic build plan

**Do not instantiate UMusic on the current tree.**

Reason: multiple incompatible build paths are still executable, and the master release contract, content-item ledger, collection-cardinality contract, suppression dispositions, visual geometry audit, property-image evidence states, quality-lesson registry, and live 10/10/10/10 validation are absent. The task stop state is `umusic_hotels_brand_explorer_build_architecture_requires_consolidation`.

### What was found locally about the name

Repo search finds no Brand Basics fixture, presentation fixture, or factory identity for UMusic Hotels.

`lib/gtm-owner-target/company-profile-enrichments.js` has a GTM stub `dakia-umusic` / “Dakia UMusic Hospitality” with website `https://umusic-hotels.com`. Its notes cite a CoStar company profile. That stub is not a Brand Explorer identity. CoStar figures must not be copied into an owner-facing profile. Identity, parent, operating model, loyalty, portfolio, and pipeline were **not** resolved, because resolution belongs to Phase B after a single build contract exists.

### What a later build would have to wait for

Before any Under Review UMusic record:

1. One canonical constructor from Basics → research ledger → Presentation → preview → master audit.
2. Retire or clearly archive wave / v36–v47 / Choice factory apply paths so they cannot be used as the UMusic writer.
3. Commit the missing master-release modules or delete the dangling npm scripts so `test:brand-explorer-master-release-contract` is real.
4. One content ledger with an explicit denominator, suppression dispositions, and collection cardinality modes.
5. One geography taxonomy. Today that taxonomy is Americas, CALA, Europe, MEA, APAC. Changing it is a product change, not a brand-content change.
6. One property-image evidence enum, with no same-brand donor substitution.
7. One Loyalty negative-state that does not inject chain-program fallbacks.
8. Live all-tab validation on one server PID, if that remains a release requirement.
9. A real lesson registry before any “new lesson + Active backscan” step.

Until that consolidation lands, UMusic stays unbuilt. Brand Status is not set because no record was created. The Active/Live universe is unchanged.
