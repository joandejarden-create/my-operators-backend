# Brand Explorer Active Profile Factory v34

Consolidation of Everhome (v32*) and WoodSpring (v33*) factory lessons.

## Global rules (implemented)

1. **Gallery** — minimum 6 visible `materials.gallery` cards with API `imageUrl`; hidden/registry-only/logo rows do not count.
2. **Property examples** — real hotel/property images; U.S. examples allowed with clear labeling.
3. **Scenario images** — no IMAGE placeholders on visible cards; hide or build fewer cards.
4. **Registry traceability** — presentation Image + API imageUrl + approved registry + durable URLs.
5. **UI fallbacks** — atelier prefers API presentation rows; hardcoded scenario/proof fallbacks only when no rows exist.
6. **Copy safety** — FDD, Item 19, ADR, fees, performance claims blocked in factory preflight.
7. **Standard Detail** — surfaced in preflight/founder-review, not a surprise Complete Build blocker.
8. **Image Asset Pack** — `asset-pack` stage plans gallery, openings, scenario requirements.
9. **Founder Review** — single markdown report per brand with pass/fail and apply command.
10. **Orchestrated commands** — see package.json `brand-explorer-active-profile-*` scripts.

## Suburban readiness (2026-07-14T07:36:58.451Z)

- Factory pass: **no**
- Recommendation: **proceed_after_v34_suburban_writer_chain_implementation**
- Missing dedicated writers: brand-explorer-suburban-source-registry-readiness-writer, brand-explorer-suburban-presentation-build-writer, brand-explorer-suburban-property-examples-writer, brand-explorer-suburban-gallery-completion-writer, brand-explorer-suburban-founder-visual-review-writer, brand-explorer-suburban-standard-detail-governance-writer

## Implementation plan

| Phase | Command | Status |
|-------|---------|--------|
| Preflight | `brand-explorer-active-profile-preflight` | implemented |
| Asset pack | `brand-explorer-active-profile-asset-pack` | implemented |
| Build draft | `brand-explorer-active-profile-build-draft` | planned writer chain |
| Founder review | `brand-explorer-active-profile-founder-review` | implemented |
| Apply approved | `brand-explorer-active-profile-apply-approved` | gated |
| Final QA | `brand-explorer-active-profile-final-qa` | implemented |

## WoodSpring / Everhome → shared modules

- `brand-explorer-active-profile-factory-rules.js` — rule evaluators
- `brand-explorer-active-profile-factory.js` — stage orchestration
- `brand-explorer-footprint-opening-image-governance.js` — hoteldam discovery
- `brand-explorer-brand-asset-image-governance.js` — gallery minimum + registry gates

## Suburban next steps

1. Run source capture (`brand-explorer-choice-extended-stay-source-capture-writer`)
2. Run v31A partial backfill for scenario/portfolio copy
3. Implement suburban-specific writers (property catalog, gallery, founder visual) parameterized from factory config
4. Re-run preflight until factory pass + founder review clean
