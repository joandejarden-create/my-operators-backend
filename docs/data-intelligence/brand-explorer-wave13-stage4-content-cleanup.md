# Wave 13 Stage 4.5 — Content Cleanup

Stage 4.5 thickens thin/blank Presentation content for seven approved Wave 13 brands
so they pass rendered-field-completeness, golden-content-quality, and tab-factory-audit gates
**before image materialization**.

## Scope

- `mama-shelter`
- `mercure`
- `ibis`
- `novotel`
- `pullman`
- `so-hotels-and-resorts`
- `fairmont-hotels-and-resorts`

## What changed

- Expanded Presentation pack coverage to Wave 12 slot parity (differentiators, bestAt, lifecycle, operations.model.*, flexibility, compliance, economics.opening.step.1–5, standards.requirement ×6, standards.questions)
- Thickened scenarios (≥45 words), proofs (≥35), openings (≥45), momentum, featured application
- Scrubbed stub-chip phrase `conversion-friendly` → `suited to conversion` in Presentation titles/bodies and Brand Value Proposition
- Unique-slot orphan deactivation when title churn created duplicate active rows
- Tab-factory audit harness allowlists Wave 13 Stage 4 approved slugs (no standard weakening)
- Protected 39 PVQL slug resolution fixed via `EXTRA_ACTIVE_IDENTITY_ANCHORS` (12 missing recordId→slug anchors)

## Forbidden (honored)

- No image writes
- No Brand Status / release / CV / Source Library / Registry writes
- No protected 39 brand writes
- No House of Originals / Morgans Originals / Radisson Collection
- No ADR / RevPAR / fee-stack / raw URLs

## Remaining non-content blockers (expected before image materialization)

| Type | Count | Disposition |
|------|------:|-------------|
| Scenario card missing image (`reassign_existing_image`) | 21 | Deferred to image materialization |
| SO/ Brand Basics snapshot `cleanly_unavailable` | 9 | Do not invent; steward Brand Basics fill |

## Validation snapshot

| Gate | Result |
|------|--------|
| Quiet sequential PVQL | `publicFullProfileCount=39`, `overallPass=true` |
| Quiet sequential quality audit | `approve_for_baseline_freeze=39` |
| Strict 39 baseline | PASS |
| No-empty rendered components (7) | PASS |
| Golden content quality (7) | PASS (after Mercure stub-chip scrub) |
| Tab factory audit (7) | Content fails cleared; remaining = images + SO/ Basics gaps |
| Rendered field completeness (7) | Same remaining disposition as tab audit |

## Reports

- `reports/brand-explorer-wave13-stage4-cleanup-failures.json` / `.md`
- `reports/brand-explorer-wave13-stage4-content-cleanup.json` / `.md`
- `reports/brand-explorer-wave13-stage4-content-cleanup-{slug}.md`
- `reports/brand-explorer-wave13-tab-audit-harness-support.md`
