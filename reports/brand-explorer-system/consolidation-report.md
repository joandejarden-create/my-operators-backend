# Brand Explorer canonical build system — consolidation report

**Final state:** `brand_explorer_canonical_build_system_consolidated_and_verified`

UMusic Hotels was not created. No Brand Basics record was created. No Presentation rows were written. No Active brand was modified. Airtable writes = 0.

## Environment

| Item | Value |
| --- | --- |
| Repository root | `/workspace` (`my-operators-backend`) |
| Branch | `cursor/umusic-build-system-discovery-3df5` |
| Worktree | `/workspace` only |
| Remote | `origin` → `joandejarden-create/my-operators-backend` |
| Package executed | `/workspace/package.json` |
| Brand Explorer code | `/workspace/public/js/brand-explorer-atelier-from-api.js`, `/workspace/api/brand-library.js`, `/workspace/lib/partner-intelligence/`, `/workspace/lib/brand-explorer/canonical/` |
| Server cwd | `/tmp/deal-capture-proxy` is a bind mount of `/workspace`. `server.js` refused to start because that cwd does not end in `my-operators-backend`. No server was left running. |
| Other local Dealality repos | None. Home, `/opt`, and `/tmp` do not contain a second checkout. |
| Other GitHub repos for this owner | `ah-commercial-performance-hub`, `Shorecast`. They are not this backend. GitHub code search was rate-limited, so those repos were not file-searched. |

## Recovery

Searched tracked files, ignored paths outside `node_modules`, `git log -S` on all local branches, worktrees (one), and `/cursor/stores`.

| Artifact | Found? | Where | Tracked? | Current before this change? | Recoverable? |
| --- | --- | --- | --- | --- | --- |
| `brand-explorer-quality-lessons.json` | No | — | No | No | Not recoverable. Implemented as `configs/brand-explorer-quality-lessons.json` |
| `brand-explorer-content-item-contract.json` | No | — | No | No | Not recoverable. Implemented |
| `brand-explorer-collection-cardinality-contract.json` | No | — | No | No | Not recoverable. Implemented |
| `brand-explorer-presentation-contract.json` | No | — | No | No | Not recoverable. Implemented from the renderer |
| `brand-explorer-visual-geometry-contract.json` | No | — | No | No | Not recoverable. Implemented from renderer grids |
| `content-item-ledger.js` | No | — | No | No | Not recoverable. Implemented |
| `brand-explorer-factory-backscan` | npm name only | `package.json` since `ac04cc4d` | Script missing | No | Script added. It calls the read-only backscan |
| `MASTER_RELEASE_CONTRACT_PASS` | Only in the prior discovery report | this branch | Docs only | No | Implemented in `master-release.js` |
| `VERIFIED_EXACT_PROPERTY` | No | — | No | No | Implemented in `image-evidence.js` |
| `PROPERTY_IMAGE_RESEARCH_REQUIRED` | No | — | No | No | Implemented |
| `APPROVED_NO_IMAGE` | No | — | No | No | Implemented |
| `LIVE_APP_ALL_TABS_VALIDATED` | No | — | No | No | Implemented in `live-tabs.js` |
| `continuous-quality-loop` | No | — | No | No | Not recovered as a separate daemon. The mutation suite is the regression loop |
| `atomic-collection` | npm name only | `package.json` | Script missing | No | Audit script added. Apply script refuses writes |
| `visual-geometry` | npm name only | `package.json` | Script missing | No | Audit script added |

Prior reports that described 60–70 lessons, a master contract, or an Active-68 backscan did not point at executable modules. Those claims are class **D** (report without an implementation), except where an older validator already enforced a slice of the rule (empty cards, flexibility levels, momentum URLs, image uniqueness). Those slices were reimplemented as lessons. They are not proof the old report is still operational.

Dead npm scripts whose files were never committed were removed, including `brand-explorer-68-active-public-full-baseline` and the Wave 17 Caption apply aliases (`brand-explorer-wave17-batch-b-caption-*`, `brand-explorer-caption-full-quality-restoration-apply`). Those files are not in git history.

## Contradictions resolved

### 62 vs live Active count

Live read of `Brand Setup - Brand Basics` on this run: **68 records, all Brand Status = Active, Live = 0**.

The protected 62 file (`reports/brand-explorer-62-active-public-full-baseline.json`, generated 2026-08-08) is a public-full freeze. It is not the universe.

Six Active records are in the live 68 and not in that freeze:

- Thompson Hotels `rec4Mga6ejz3L1M3P`
- Delta Hotels by Marriott `rec50qkCj6yt9fMPg`
- Four Points by Sheraton `recH5ZF9V6ivz9p5h`
- Hyatt Centric `recNy2efMm4N1JtgC`
- Hyatt Regency `recP9SqDootMrzaU1`
- Fairfield by Marriott `recpUTDtwt1wPMDPj`

`PRIMARY_RELEASE_SLUGS` remains a 7-brand operational overlay. It is not the universe. Statuses were not written.

### 7 tabs vs 10 tabs

The renderer’s `ATELIER_TAB_DEFS` has 10 tabs. `TAB_CONTRACTS` now uses that same list, including Commercial Engine, Loyalty Program, and Brand Materials. Load time throws if the order drifts.

### Geography and product taxonomy

Canonical regions are the renderer defaults: Americas, CALA, Europe, MEA, APAC. The earlier North America / Caribbean & Latin America / Middle East & Africa / Asia Pacific list is not the product UI. It was not adopted.

Flexibility labels and levels follow `lib/brand-explorer-flexibility-levels.mjs` and the atelier indicator list.

Unresolved, recorded and not silently rewritten: Recent Momentum minimum is 2 in the dedicated momentum contract and 3 in the population contract. The canonical collection gate uses 2.

### Dead package scripts

Missing Factory targets were either pointed at real files or removed. Doctor checks that remaining Brand Explorer npm scripts resolve to files on disk.

### Lesson registry

There was no registry to recover. Active executable lessons: **22**. `REQUIRES_REIMPLEMENTATION`: **1** (`BE-L-UNRECOVERED-REGISTRY`). The reported ~70 count is not active.

### Old writers

Wave 12–17 factories, v36–v47 apply/remediation scripts, Choice factory, tab-factory remediation, and other Brand Explorer scripts that accept `--apply` now call `refuseLegacyApply`. npm scripts for writers and factories go through `scripts/brand-explorer-legacy-guard.mjs`. Dry-runs still run. `--apply` exits 2 unless the break-glass env var is set.

## Canonical pipeline

`IDENTITY → FOUNDATION → PRODUCT SHELL → ITEM APPLICABILITY → RESEARCH → ATOMIC CONTENT → COLLECTION ENUMERATION → EVIDENCE / FACT GRAPH → MATERIALIZATION (disabled) → IMAGE / PROPERTY EVIDENCE → RENDER → AUDITS → LIVE VALIDATION → MASTER_RELEASE_CONTRACT_PASS → READY_FOR_FOUNDER_REVIEW`

Only `evaluateMasterRelease` may set `READY_FOR_FOUNDER_REVIEW`.

## Lessons

| | Count |
| --- | --- |
| Recovered from an existing lesson file | 0 |
| Reimplemented as executable validators with mutation tests | 22 |
| `REQUIRES_REIMPLEMENTATION` | 1 |
| Active executable | 22 |
| Registry version | `quality-lessons-v1` |

## Tests

`npm run brand-explorer:test` and `npm run brand-explorer:doctor` passed.

- Passing fixture reaches `READY_FOR_FOUNDER_REVIEW` and `MASTER_RELEASE_CONTRACT_PASS`.
- All 22 mutation cases are detected: empty item, thin item, generic template, invalid suppression, hidden negative, collection underpopulation, missing loyalty tier, wrong geography label, wrong grid cardinality, partial width, property multi-identity, property/image mismatch, same-brand donor, blank metric, missing unit/scope, invalid zero, fact contradiction, interrupted live validation, subsystem-ready bypass, noncanonical flexibility level, momentum missing URL, tab parity drift.
- Doctor: `pass=true`, `auditEnabled=true`, `materializationEnabled=false`.
- Legacy guard: `node scripts/brand-explorer-legacy-guard.mjs scripts/brand-explorer-wave13-factory.mjs --stage tab-factory-build --apply` exits 2.

## Legacy writers

| Status | Script count from `listLegacyScripts()` |
| --- | --- |
| `MIGRATED_TO_CANONICAL` | 9 |
| `ARCHIVED_READ_ONLY` | 105 |
| `TEST_FIXTURE` | 32 |
| `STILL_REQUIRED` | 119 |

`STILL_REQUIRED` files are not the production apply path. Scripts that mention `--apply` were given the guard even when their names are not `writer`. Historical files were not deleted.

## Active universe

Read-only backscan: `reports/brand-explorer-system/active-universe-backscan.json`.

| | |
| --- | --- |
| Active count | 68 |
| Status mix | Active 68, Live 0 |
| Clean / `MASTER_RELEASE_CONTRACT_PASS` | 0 |
| Failing | 68 |
| Writes | 0 |

Defect classes on the live set (a brand can have several):

| Class | Brands |
| --- | --- |
| `collection_underpopulation` | 68 |
| `interrupted_live_validation` | 68 |
| `momentum_missing_url` | 63 |
| `wrong_geography_label` | 29 |
| `empty_item` | 15 |
| `hidden_negative` | 15 |
| `property_multi_identity` | 1 |

Image evidence on `footprint.openings` rows, classified at read time. Presentation does not store an exact-property evidence state, so a row with an attachment is `UNVERIFIED_PROPERTY_IDENTITY`, not verified and not wrong.

| State | Rows |
| --- | --- |
| `UNVERIFIED_PROPERTY_IDENTITY` | 213 |
| `MISSING_PROPERTY_IMAGE` | 24 |

Live 10/10/10/10 was not granted. No stable server PID was attached, so `live_application` fails closed for every brand. That is the current MASTER result. Old freeze reports were not reused as passes.

Collection underpopulation is expected: the new ledger requires the full canonical collections, and this backscan did not treat partial slot presence as a pass.

## 21c / Caption

Both records exist. Neither is Active. Neither passes the consolidated master gate. They were not grandfathered and not published.

| Brand | Record | Brand Status | MASTER |
| --- | --- | --- | --- |
| Caption by Hyatt | `recSgIQ0bhRpPXZbU` | Under Review | `NOT_READY` |
| 21c Museum Hotel | `recqS2DNnVlxAGPUR` | Under Review | `NOT_READY` |

Failed gates for both include foundation, item completion, collection cardinality, suppression integrity, source support, canonical labels, presentation, property image evidence, momentum, geography, and live application. 21c also fails similar brands.

The Wave 17 Caption apply scripts named in `package.json` were missing on disk and were removed from npm. They were not run.

## UMusic

Not created. No Brand Basics row matched UMusic on the read-only name probe. UMusic is the next canary only after this factory is accepted. It was not started.

## Safety

| Control | Result |
| --- | --- |
| Airtable method | GET only |
| Brand Status writes | 0 |
| Presentation writes | 0 |
| Active brand content edits | 0 |
| Materialization | Disabled |
| Unattended activation | Off |
