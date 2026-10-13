# Brand Explorer canonical build system

Version: `canonical-build-v1`  
Materialization: **disabled**. This pipeline audits and gates. It does not write Brand Basics or Presentation.

The live product shell is the 10-tab atelier renderer. Tab Factory, wave factories, and v36–v47 scripts are legacy. Their `--apply` paths refuse to run unless `BRAND_EXPLORER_ALLOW_LEGACY_APPLY=1`.

## Architecture

One module tree owns the contracts:

| Concern | Path |
| --- | --- |
| Product shell (10 tabs) | `configs/brand-explorer-product-shell.json` |
| Content items and dispositions | `configs/brand-explorer-content-item-contract.json` |
| Collection cardinality | `configs/brand-explorer-collection-cardinality-contract.json` |
| Presentation labels, geography, flexibility | `configs/brand-explorer-presentation-contract.json` |
| Visual geometry | `configs/brand-explorer-visual-geometry-contract.json` |
| Lesson registry | `configs/brand-explorer-quality-lessons.json` |
| Loader | `lib/brand-explorer/canonical/product-shell.js` |
| Ledger | `lib/brand-explorer/canonical/content-item-ledger.js` |
| Lessons | `lib/brand-explorer/canonical/lessons.js` |
| Master gate | `lib/brand-explorer/canonical/master-release.js` |
| Image evidence | `lib/brand-explorer/canonical/image-evidence.js` |
| Live 10-tab session | `lib/brand-explorer/canonical/live-tabs.js` |
| Doctor | `lib/brand-explorer/canonical/doctor.js` |
| Read-only backscan | `lib/brand-explorer/canonical/backscan.js` |
| Mutation suite | `lib/brand-explorer/canonical/mutations.js` |
| Legacy apply guard | `lib/brand-explorer/canonical/legacy-apply-guard.js` |

`lib/partner-intelligence/brand-explorer-tab-contracts.js` imports `CANONICAL_TAB_IDS` and throws at load time if its tab list is not the same 10 ids in the same order.

## Lifecycle

`npm run brand-explorer:build` prints this lifecycle and refuses `--apply`:

1. IDENTITY
2. FOUNDATION / build brief
3. PRODUCT SHELL (10 tabs)
4. ITEM APPLICABILITY
5. RESEARCH
6. ATOMIC CONTENT
7. COLLECTION ENUMERATION
8. EVIDENCE / FACT GRAPH
9. MATERIALIZATION (disabled)
10. IMAGE / PROPERTY EVIDENCE
11. RENDER
12. STRUCTURE, CONTENT, COLLECTION, SUPPRESSION, FACT, PRESENTATION, GEOMETRY audits
13. LIVE ALL-TAB VALIDATION
14. REMEDIATION LOOP (not executed by this pipeline)
15. `MASTER_RELEASE_CONTRACT_PASS`
16. `READY_FOR_FOUNDER_REVIEW` only when that pass is true

The contract decides what is evaluated. It does not pick the answer. Allowed dispositions:

`APPLICABLE_POPULATED_POSITIVE`, `APPLICABLE_POPULATED_NEGATIVE`, `RENDER_QUALIFIED`, `RENDER_KNOWN_ZERO`, `RENDER_NOT_DISCLOSED`, `DEPENDENT_SUPPRESSED_JUSTIFIED`, `RESEARCH_REQUIRED`, `BLOCKED`.

Missing evidence is not zero. A negative answer is not suppression. A validator defect is not a rewrite. Dependent suppression is valid only after a visible parent conclusion.

## Tabs

Renderer source: `ATELIER_TAB_DEFS` in `public/js/brand-explorer-atelier-from-api.js`.

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

`Operating Model` is the live UI label. `Operations & Standards` remains an accepted alias in the shell. Inventory files that still say Operations & Standards were not rewritten.

## Geography and flexibility

Region cards, in order: **Americas, CALA, Europe, MEA, APAC**.

Flexibility indicators: Design Flexibility, Conversion Friendliness, Localization Flexibility, Operational Rigidity, PIP Sensitivity, Prototype Dependence.

Levels: `Minimal`, `Low`, `Moderate`, `Medium`, `High`, `Very high`.

## Collections

Modes are `EXACT`, `MINIMUM`, `ALL_SOURCE_MEMBERS`, and `CONDITIONAL`. Counts live in `configs/brand-explorer-collection-cardinality-contract.json`.

Unresolved product note: Recent Momentum is `MINIMUM` 2, matching `brand-explorer-recent-momentum-contract.js`. The older population contract still says 3. The canonical gate uses 2 until a founder pick. That population file was not rewritten.

Loyalty tiers are `ALL_SOURCE_MEMBERS` only when `conventionalLoyaltyProgram` is true. A brand with no hotel loyalty program does not invent tiers.

Representative properties: one card, one property. Image states:

`VERIFIED_EXACT_PROPERTY`, `VERIFIED_WRONG_PROPERTY`, `UNVERIFIED_PROPERTY_IDENTITY`, `CROSS_PROPERTY_DUPLICATE`, `MISSING_PROPERTY_IMAGE`, `APPROVED_NO_IMAGE`, `PROPERTY_IMAGE_RESEARCH_REQUIRED`, `NON_PROPERTY_VISUAL`.

Unverified is not treated as wrong. A same-brand donor image does not pass. Acceptable states are `VERIFIED_EXACT_PROPERTY` and `APPROVED_NO_IMAGE`.

## Airtable

Read by the backscan. Not written by this pipeline.

| Table | Role |
| --- | --- |
| `Brand Setup - Brand Basics` | Identity. Universe filter `OR({Brand Status}='Active', {Brand Status}='Live')` |
| `Brand Setup - Brand Explorer Presentation` | Slot Key, Title, Body, Image, Brand link |

The active universe is that Brand Status filter. `PRIMARY_RELEASE_SLUGS` and the protected 62 freeze are overlays, not the universe.

## Renderer and preview

Pages: `public/brand-explorer-combined.html`. Fetch: `GET /api/brand-library/brand?brandId=`.

Factory preview, only for an allowlisted slug with Presentation rows:

`/brand-explorer-combined.html?brandId={recordId}&beInternalPreview=1&factoryPreview=1`

Defined in `lib/partner-intelligence/brand-explorer-factory-preview-candidates.js`.

## Master release

`evaluateMasterRelease` in `lib/brand-explorer/canonical/master-release.js` is the only function that may return `READY_FOR_FOUNDER_REVIEW`.

Gates: identity, foundation, item completion, research required = 0, blocked = 0, collection cardinality, suppression integrity, source support, fact consistency, metric semantics, structure, canonical labels, presentation, visual geometry, property atomicity, property image evidence, loyalty tiers, loyalty benefits, momentum, similar brands, geography, planned/written/rendered reconciliation, live application.

Live application passes only when one session has `pid`, `buildSignature`, and `expected = activated = measurable = audited = 10`, with no server restart and no 0×0 bypass. Code: `LIVE_APP_ALL_TABS_VALIDATED` in `lib/brand-explorer/canonical/live-tabs.js`.

## Lessons

Registry: `configs/brand-explorer-quality-lessons.json`.  
Active lessons are the ones whose `validator` exists in `lessons.js` and whose mutation case is in `mutations.js`.  
`REQUIRES_REIMPLEMENTATION` lessons are not active.

No historical file of ~70 lessons was recovered. The active count is the executable set (22), plus one unrecovered-registry record that is not active.

## Commands

```bash
npm run brand-explorer:doctor
npm run brand-explorer:build
npm run brand-explorer:audit
npm run brand-explorer:backscan
npm run brand-explorer:test
npm run test:brand-explorer-master-release-contract
```

`brand-explorer:build` and `brand-explorer:backscan` do not write Airtable. Backscan requires `AIRTABLE_API_KEY` and `AIRTABLE_BASE_ID` and only sends GET requests.

`brand-explorer:audit -- --profile path.json` evaluates one JSON profile.

## Doctor

`npm run brand-explorer:doctor` checks the repo root files, the 10-tab renderer parity, the lesson registry, the mutation suite, Airtable table names, the preview route, the master function, the live validator, and that every Brand Explorer npm script points at a file that exists. Legacy npm apply commands must go through `scripts/brand-explorer-legacy-guard.mjs`.

`materializationEnabled` stays false.

## Legacy writers

Historical scripts remain in `scripts/`. Classification is `listLegacyScripts()`:

- `MIGRATED_TO_CANONICAL` — the new entry points
- `ARCHIVED_READ_ONLY` — writers, remediations, and wave factories; `--apply` is refused
- `TEST_FIXTURE` — audit and preview-mode scripts
- `STILL_REQUIRED` — other Brand Explorer scripts that are not apply writers

Break-glass for a historical apply: `BRAND_EXPLORER_ALLOW_LEGACY_APPLY=1`. Do not set it for a new brand.

## Server

`server.js` must be started with the working directory ending in `my-operators-backend`. A bind mount at another path fails that guard. The files that would be served are this repo's `server.js` and `public/`.
