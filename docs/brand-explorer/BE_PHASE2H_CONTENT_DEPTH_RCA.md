# Phase 2H — Content depth & rendering RCA

**Status:** NOT founder-approved · Curio / Ascend remain unpublished  
**Branch:** `feat/be-phase2h-component-parity-v1`  
**Prior:** Phase 2G PR #457 (paired architecture accepted as improvement); Phase 2F PR #453 founder-rejected

## Founder verdict addressed

1. Comparison flattened real Brand Explorer components into generic text cards → **presentation parity failure**.
2. Reconstructed content is materially thinner than the live Curio profile → **substantive parity failure**.

## Root causes

### A. Comparison Studio rendering (presentation)

| Cause | Evidence |
|---|---|
| Compare studio never loads atelier JS | `public/brand-explorer-compare.html` only loads `brand-explorer-compare-studio.js` |
| `renderCard` flattens every slot to title + escaped body | `public/js/brand-explorer-compare-studio.js` |
| Product CSS loaded but unused for subsection HTML | OE panels CSS present; no `.be-atelier-oe` injection |

### B. Content depth regression (generation)

| Cause | File / function | Effect |
|---|---|---|
| Stamp templates broadcast one diligence paragraph across sibling slots | `be-phase2e-field-quality-gates-v1.js` → `repairFailedFields` | Ops / economics / footprint look “full” but are duplicates |
| Brand-wide HPC stamped into every `footprint.*` / `metric.*` | Same, `/^footprint\./` branch | Regional cards repeat brand totals |
| Phase 2F min-word padding | `be-phase2f-presentation-repair-v1.js` | Artificial length; founder-rejected |
| Phase 2G sanitize clears near-duplicates to empty | `be-phase2g-presentation-sanitize-v1.js` | ~52 Curio bodies emptied vs baseline; mean WC 33→15 |
| Evidence→field composition never builds multi-claim `why_value`, case studies, per-leaf ops | `be-phase2e-universal-reconstruction-v1.js` | Structural depth never enters the candidate |

**Anti-padding is correct as a gate.** The failure is clear-without-refill and stamp-generation upstream.

## Depth snapshot (Curio)

| Slot | Baseline WC | Phase 2F | Phase 2G |
|---|---:|---:|---:|
| `overview.why_value` | 102 | 32 | 18 |
| `materials.caseStudy` | 108 | 29 | 20 |
| `loyalty.owner_lens` | 71 | 33 | 0 |
| `operations.standards_philosophy` | 82 | 34 | 0 |
| `footprint.region.*` | distinct regional | identical brand HPC | identical brand HPC |

## Phase 2H repair principles

1. **One renderer:** Comparison Studio injects the same atelier tab/section HTML builders as the product.
2. **Paired document preserved:** one scroll; left/right product sections aligned by subsection.
3. **Compose from evidence, not baseline copy:** baseline is a coverage checklist only.
4. **No stamp broadcast:** sibling slots need distinct claim fragments or explicit `COVERAGE_LIMITED` / research gap (no empty customer padding).
5. **Footprint SoT:** brand-wide HPC only on brand-level metrics; regions require regional rollup or labeled coverage limit.
6. **Substantive gates:** insight count, examples, owner relevance, item coverage — not a single 99% score.

## Out of scope this cycle

- Production Airtable writes, paid research, mass population, publication, auto-approval
- Merging PR #453 or claiming founder approval
- Touching ADP / GDI / HE worktrees
