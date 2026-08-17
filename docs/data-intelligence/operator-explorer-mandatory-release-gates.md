# Operator Explorer mandatory release gates

Before any operator may reach `founder_review_ready` or `active_profile_ready`, the Operator Explorer OS and Tab Factory require:

1. **source_provenance_by_tab**
2. **tab_factory_audit**
3. **rendered-field-completeness** (`auditPass` = failFindings === 0)
4. **no_empty_rendered_components**
5. **section_pattern_parity** — sections must match Arbor Lodging + Hotel Equities product pattern (filled-but-wrong format fails)
6. **golden-content-quality** — operator-specific, owner-useful, comparable to baselines
7. **operator-specific-source-validation** — company-controlled / approved sources preferred for positioning and differentiators

Company Validated is never auto-written. Golden baselines (Arbor, Hotel Equities) are never modified by factory remediation unless the task is an explicit baseline revision.

## Audit semantics

| Flag | Meaning |
| --- | --- |
| `auditComplete` | Completeness audit ran |
| `patchPlanComplete` | Every fail has a proposed fix / intentional handling |
| `auditPass` | `failFindings === 0` in the rendered / fixture payload |

`auditPass` must be **false** whenever `failFindings > 0`. A complete patch plan is not a pass.

Every rendered field must be one of:

- complete
- intentionally suppressed
- cleanly marked unavailable
- included in an unapplied patch plan

## Source hierarchy (operators)

1. Operator official website (entity-specific)  
2. Operator official CALA / regional pages and decks  
3. Operator case studies / materials  
4. Parent / enterprise pages (only for labeled enterprise context)  
5. Third-party (supplementary only)

Parent / enterprise pages may support ownership, platform scale, and brand approvals when clearly labeled.

Parent pages may **not** be the only source for CALA positioning, differentiators, leadership for the regional profile, or owner-facing fit claims.

### Canonical domains (baselines)

| Operator | Required | Allowed parent context |
| --- | --- | --- |
| `arbor-lodging-cala` | `arborlodging.com` | Enterprise Arbor materials when labeled parent platform |
| `hotel-equities-cala` | `hotelequities.com` | Enterprise HE materials when labeled parent platform |

## Commands

```bash
# Unit gates (no Airtable)
npm run test:operator-explorer-mandatory-release-gates
npm run test:operator-explorer-quality-baseline
npm run test:operator-explorer-tab-factory-audit
npm run test:operator-explorer-section-pattern-parity
npm run test:operator-explorer-source-provenance-by-tab

# Tab factory / completeness (dry-run default; fixtures|live|merged)
npm run operator-explorer-tab-factory-audit -- --operators arbor-lodging-cala,hotel-equities-cala --source=fixtures --dry-run
npm run operator-explorer-tab-factory-audit -- --operators arbor-lodging-cala,hotel-equities-cala --source=merged --dry-run

# Section pattern parity vs Arbor + HE
npm run operator-explorer-section-pattern-parity-audit -- --operators arbor-lodging-cala,hotel-equities-cala --dry-run

# Source provenance by tab
npm run operator-explorer-source-provenance-by-tab -- --operators arbor-lodging-cala,hotel-equities-cala --source=fixtures --dry-run
npm run operator-explorer-source-provenance-by-tab -- --source=merged --dry-run

# OS release readiness + next-operator signal
npm run operator-explorer-os -- --operators arbor-lodging-cala,hotel-equities-cala,ghl-hoteles --stage release-readiness --dry-run
npm run test:operator-explorer-os
npm run operator-explorer-factory-init -- --operators ghl-hoteles --dry-run
```

Ready-for-next checklist: `docs/data-intelligence/operator-explorer-ready-for-next-operator.md`

## Modules

- `lib/partner-intelligence/operator-explorer-quality-baseline.js`
- `lib/partner-intelligence/operator-explorer-tab-contracts.js`
- `lib/partner-intelligence/operator-explorer-os.js`
- `lib/partner-intelligence/operator-explorer-factory-queue.js`
- `lib/partner-intelligence/operator-explorer-factory-init.js`
- `lib/partner-intelligence/operator-explorer-section-pattern-parity.js`

## Change impact

**High** — blocks founder/active readiness until gates clear.

Rollback: revert OS/factory gate wiring; audits themselves are read-only unless remediation `--apply` is used.
