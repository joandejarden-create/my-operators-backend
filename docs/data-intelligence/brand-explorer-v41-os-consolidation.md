# v41 — Brand Explorer Build OS Consolidation

Single operating-system layer for Brand Explorer readiness. Live API + internal preview + external DOM + Presentation + assets + copy are the source of truth. Report-only readiness never unlocks a brand.

```bash
npm run brand-explorer-os -- --brands everhome-suites,kimpton,radisson-individuals-by-choice,hotel-indigo,mgallery-collection,design-hotels,small-luxury-hotels-of-the-world --stage release-readiness --dry-run
```

## What it consolidates

| Concern | Module |
|---------|--------|
| Canonical state | `brand-explorer-os-state-machine.js` |
| Gate evaluation | `brand-explorer-os-gate-evaluator.js` |
| Next action | `brand-explorer-os-action-router.js` |
| Founder packet | `brand-explorer-os-founder-review-packet.js` (summary) · **v42** `brand-explorer-founder-visual-review.js` (tab + release recommendation) |
| v40C patch safety | `brand-explorer-os-patch-safety.js` |
| Runner / reports | `brand-explorer-os-run.js` + `scripts/brand-explorer-os.mjs` |

## Canonical states

`not_started` → `sources_seeded` → `knowledge_ready` → `asset_ready` → `draft_ready` → `draft_applied_with_defects` → `internal_preview_blocked` → `internal_preview_ready` → `founder_review_ready` → `active_release_ready` → `active_profile_ready` · or `state_conflict`

## Important routing rule (v40C)

If residual Presentation patches are still pending, next action is **`apply_remediation`**, not founder review — even when live DOM looks clean because renderer scrub hides some tokens.

## Mandatory content/source gates (permanent)

Before `founder_review_ready` / `active_profile_ready`, OS + factory require:

1. brand-specific-source-validation  
2. rendered-field-completeness-audit  
3. rendered-field-completeness-remediation (`auditPass` = `failFindings === 0`)  
4. golden-content-quality  

See `docs/data-intelligence/brand-explorer-mandatory-release-gates.md`.

## Golden suite

```bash
npm run test:brand-explorer-golden-release-suite -- --brands everhome-suites,kimpton,radisson-individuals-by-choice
```

Fails if FDD/LOI/fee-stack chrome returns, gallery &lt; 6, openings &lt; 3, external lock leaks, or active release is allowed without founder approval.

## Guardrails

- Read-only / dry-run only
- No Airtable writes, unlock, active release, Company Validated changes, or new brand content
