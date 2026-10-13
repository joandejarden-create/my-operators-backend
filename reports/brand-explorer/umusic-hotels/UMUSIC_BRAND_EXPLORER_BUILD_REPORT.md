# UMusic Hotels Brand Explorer — build report

**Date:** 2026-10-13  
**Final state:** `umusic_hotels_brand_explorer_build_architecture_requires_consolidation`

UMusic Hotels was not published. It was not written. No second brand was started.

Discovery detail: `reports/brand-explorer/umusic-hotels/current-build-system-discovery.md`.

---

## Phase A — System discovery

The current repository does not have one Brand Explorer build system.

Executable paths that still disagree with each other:

- Tab Factory, documented as the permanent setup (`docs/data-intelligence/brand-explorer-tab-factory-build-operation.md`), with a **7-tab** field contract against a **10-tab** renderer.
- Brand Explorer OS v41 (`lib/partner-intelligence/brand-explorer-os-state-machine.js`), read-only, still defaulting to a 7-brand `PRIMARY_RELEASE_SLUGS` cohort while the binding public universe is Brand Status Active/Live (protected 62).
- Wave 12–17 factories, v36–v47 pipelines, the Choice factory, and the lane-2 full-tab build, each with its own apply script.
- npm scripts added in `ac04cc4d` for atomic collections, visual geometry, factory backscan, master release, and quality lessons. The script files and `configs/brand-explorer-*.json` contracts are not in git. `MASTER_RELEASE_CONTRACT_PASS` does not occur anywhere.

### Lesson registry

| | |
| --- | --- |
| Version | None |
| Active lessons | **0** |
| File | `configs/brand-explorer-quality-lessons.json` missing |

### Master gates that actually exist

There is no master release contract. The gates that OS and `docs/data-intelligence/brand-explorer-mandatory-release-gates.md` use before `founder_review_ready` are:

`source_provenance_by_tab`, `tab_factory_audit`, rendered-field completeness (`auditPass` iff `failFindings === 0`), `no_empty_rendered_components`, `image_distinctiveness`, `image_role_match`, `section_pattern_parity` (inside tab-factory evaluate), `golden_content_quality`, `brand_specific_source_validation`, plus the footnote rule `ai_assisted_profile_footnote_visible` in docs/PVQL.

Live validation of expected = activated = measurable = audited = 10 on one PID does not exist.

### Canonical contracts that actually exist

Documented in the discovery report from code, not from the missing JSON configs:

- Content slots and mixed exact/minimum counts in section-population and completeness inventory
- Suppression as `intentionally_suppressed` / `clean_unavailable_state`, not the requested disposition enum
- No fact graph
- Presentation via renderer + slot manifest + plan contract
- No visual geometry contract
- Openings cards: one property per row (`brand-explorer-openings-property-card-contract.js`)
- Image uniqueness statuses, not `VERIFIED_EXACT_PROPERTY`
- Geography: **Americas, CALA, Europe, MEA, APAC**
- Flexibility: six slots, levels `Minimal` through `Very high`
- Momentum: `buildRecentMomentumCard`, minimum 2 in the momentum contract and 3 in the population contract

---

## UMusic identity

Not resolved. No Brand Basics record was created or updated.

Repo search found no Brand Explorer fixture or factory identity for UMusic Hotels.

A GTM enrichment stub `dakia-umusic` (“Dakia UMusic Hospitality”, site `https://umusic-hotels.com`) exists in `lib/gtm-owner-target/company-profile-enrichments.js`. Its notes cite CoStar. That stub was not treated as brand identity and was not copied into a profile.

| Item | Status |
| --- | --- |
| Canonical brand name | Unresolved |
| Slug | Unresolved |
| Parent / company | Unresolved |
| Operating structure | Unresolved |
| Official sources used for a profile | None. Phase B research was not started. |
| Portfolio | Unresolved |
| Loyalty | Unresolved |
| Brand Status | Not set. No record. |

---

## Research

Build Brief was not written.

Owner implications, portfolio, pipeline, residences, loyalty, and commercial engine were not researched. Doing that research and then materializing it would choose one of the incompatible writers.

Evidence gaps: the entire owner-facing fact base. No primary-source dossier was opened because the build contract it would have to satisfy is missing.

---

## Content

No content ledger was instantiated.

| Disposition | Count |
| --- | --- |
| Base canonical items | Not instantiated. No content-item contract file. |
| Dynamic children | 0 |
| Positive | 0 |
| Negative | 0 |
| Qualified | 0 |
| Dependent suppressions | 0 |
| RESEARCH_REQUIRED | Not a coded disposition |
| BLOCKED | Architecture blocker on the whole profile, not per item |

Denominator `BASE_CANONICAL_ITEMS + INSTANTIATED_COLLECTION_CHILDREN` cannot be computed. The repo has no base-item registry.

---

## Collections

Nothing was written or rendered for UMusic. Every collection is 0 / 0 / 0.

| Collection | Expected by current code | Written | Rendered |
| --- | --- | --- | --- |
| Flexibility | 6 slots | 0 | 0 |
| Owner outcomes / value scenarios | 4 `valueOwners.scenario.*` | 0 | 0 |
| Key differentiators | ≥3 bullets × 2 groups | 0 | 0 |
| Best at | 3 | 0 | 0 |
| Loyalty benefits | No ALL_SOURCE_MEMBERS rule | 0 | 0 |
| Loyalty tiers | No ALL_SOURCE_MEMBERS rule | 0 | 0 |
| Geographic regions | 5 (Americas, CALA, Europe, MEA, APAC) | 0 | 0 |
| Portfolio presence | ≥3 portfolio-mix rows in inventory | 0 | 0 |
| Properties (`footprint.openings`) | ≥3 complete cards | 0 | 0 |
| Momentum | 2 (momentum contract) and 3 (population contract) — the two minimums already disagree | 0 | 0 |
| Similar brands | ≥2 rows | 0 | 0 |

---

## Metrics

No metric was asserted.

| Metric | Value | Unit | Scope | Evidence state |
| --- | --- | --- | --- | --- |
| Open hotels | — | — | — | Not researched. Not zero. |
| Pipeline | — | — | — | Not researched. Not zero. |
| Rooms / keys | — | — | — | Not researched. Not zero. |

Census was not queried. A CALA zero was not inferred.

---

## Properties

No representative property was selected. No image, modal, or provenance record was created.

| Property | Status | Image evidence | Modal | Result |
| --- | --- | --- | --- | --- |
| — | — | — | — | Not built |

---

## Presentation

No labels, order, layout, or geometry were rendered for UMusic.

Product-owned structure that a future profile would have to follow, once a single contract exists, is the current atelier: 10 tabs, six flexibility levels, five footprint regions named Americas / CALA / Europe / MEA / APAC. That geography is what the renderer ships. It is not the North America / Caribbean & Latin America / Europe / Middle East & Africa / Asia Pacific list. Renaming regions inside a brand profile is not allowed. Changing the product taxonomy is a separate product change.

No DOM geometry audit was run. The audit script is missing.

---

## MASTER

`MASTER_RELEASE_CONTRACT_PASS` cannot be evaluated. The gate is not implemented.

| Check | Result |
| --- | --- |
| Research foundation | Not run |
| Atomic content ledger | Missing |
| Collections | Not instantiated |
| Suppression dispositions | Missing |
| Fact graph | Missing |
| Source support | Not run |
| Metric semantics | Missing |
| Product structure | Split across 7-tab contract and 10-tab UI |
| Presentation contract file | Missing |
| Visual geometry | Missing |
| Atomic property entities | Partial openings-card contract only |
| Property image evidence enum | Missing |
| Planned / written / rendered reconciliation | Not run |
| Live 10/10/10/10 | Not implemented. No server PID. |

Remaining blocker: consolidate the build architecture before any UMusic write.

Preview route: **none.** Factory preview requires an allowlisted slug plus `beInternalPreview=1&factoryPreview=1`. UMusic is not allowlisted and has no record id.

---

## New learning

| | |
| --- | --- |
| Lessons before | 0 (registry file absent) |
| Lessons added | 0 |
| Active-68 backscan | Not run. `brand-explorer-factory-backscan.mjs` is missing, and there is no 68-brand lesson backscan. |

No new lesson was added. A lesson without a validator, mutation test, and backscan would not match the requested registry, and that registry does not exist.

The defect class this stop records is architectural: dangling master-release npm scripts beside older wave and tab-factory writers. Fixing it means one committed contract and the retirement of the other writers. It is not a UMusic copy edit.

---

## Safety

| Control | Result |
| --- | --- |
| UMusic Brand Status | Not created. Remains unpublished. |
| Active / Live count | Unchanged. No Brand Status write. |
| Unrelated brands | No Presentation, Source Library, Registry, or Basics writes. |
| Protected 62 baseline | Untouched. |
| Company Validated | Untouched. |
| Unattended publish / activation | Off. No status promotion, no factory-preview registration, no scheduler enable. |

---

## Stop

`umusic_hotels_brand_explorer_build_architecture_requires_consolidation`

Do not publish UMusic Hotels. Do not start another brand until one build path is the master contract and the missing modules are either implemented or removed.
