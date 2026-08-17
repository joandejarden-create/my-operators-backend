# Brand Explorer mandatory release gates

Before any brand may reach `founder_review_ready` or `active_profile_ready`, the Brand Explorer OS and Active Profile Factory require:

1. **source_provenance_by_tab**
2. **tab_factory_audit**
3. **rendered-field-completeness** (`auditPass` = failFindings === 0)
4. **no_empty_rendered_components**
5. **image_distinctiveness** — **distinct** gallery (≥6), scenario (≥3), and property (≥3) images; near-duplicate crops fail (not slot count alone)
6. **image_role_match**
7. **section_pattern_parity** — Recent Momentum / Geographic Footprint / Portfolio Context / Growth Priorities must match benchmark product pattern (filled-but-wrong format fails). **Recent Momentum** requires named openings/press cards with dated Body + trailing announcement URL, newest→oldest, Proper Case link labels — see `brand-explorer-recent-momentum-contract.js` and `docs/data-intelligence/brand-explorer-section-pattern-parity.md`.
8. **golden-content-quality**
9. **brand-specific-source-validation**

Section pattern parity module: `lib/partner-intelligence/brand-explorer-section-pattern-parity.js`  
Recent Momentum contract: `lib/partner-intelligence/brand-explorer-recent-momentum-contract.js`  
Image uniqueness module: `lib/partner-intelligence/brand-explorer-image-uniqueness.js`  
Legacy approved profiles: `docs/data-intelligence/brand-explorer-legacy-approved-profile-reconciliation.md`

Company Validated is never auto-written.

## Audit semantics

| Flag | Meaning |
| --- | --- |
| `auditComplete` | Completeness audit ran |
| `patchPlanComplete` | Every fail has a proposed fix / intentional handling |
| `auditPass` | `failFindings === 0` in the rendered payload |

`auditPass` must be **false** whenever `failFindings > 0`. A complete patch plan is not a pass.

Every rendered field must be one of:

- complete
- intentionally suppressed
- cleanly marked unavailable
- included in an unapplied patch plan

## Source hierarchy

1. Brand-specific official brand site  
2. Brand-specific official development page  
3. Brand-specific property pages  
4. Parent-company brand page  
5. Parent-company corporate page  
6. Third-party (supplementary only)

Parent pages may support ownership, brand family, enterprise platform, portfolio context, and development organization.

Parent pages may **not** be the only source for positioning, guest audience, design story, property examples, images, scenarios, owner-facing brand fit, or differentiators.

### Canonical brand domains

| Brand | Required | Allowed parent context |
| --- | --- | --- |
| `hotel-indigo` | `hotelindigo.com` | `ihg.com`, `ihgplc.com`, `development.ihg.com` |
| `mgallery-collection` | `mgallery.accor.com` | `group.accor.com`, `all.accor.com`, `accor.com` |
| `small-luxury-hotels-of-the-world` | `slh.com` | none (no forced franchise/parent logic) |

## Commands

```bash
# Unit gates (no Airtable)
npm run test:brand-explorer-mandatory-release-gates

# Live completeness (exit 3 when failFindings > 0)
npm run brand-explorer-rendered-field-completeness-audit -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --dry-run

# Remediation dry-run / apply
npm run brand-explorer-rendered-field-completeness-remediation -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --dry-run

# Golden content quality
npm run test:brand-explorer-golden-content-quality -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world

# OS release readiness (routes to remediation when gates fail)
npm run brand-explorer-os -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --stage release-readiness --dry-run
```

## Modules

- `lib/partner-intelligence/brand-explorer-brand-specific-source-validation.js`
- `lib/partner-intelligence/brand-explorer-rendered-field-completeness-audit.js`
- `lib/partner-intelligence/brand-explorer-os-gate-evaluator.js`
- `lib/partner-intelligence/brand-explorer-os-state-machine.js`
- `lib/partner-intelligence/brand-explorer-active-profile-factory-rules.js`

## Change impact

**High** — blocks founder/active readiness and factory `pass` until gates clear.

Rollback: revert OS/factory gate wiring; completeness audit semantics can be restored from git history. Does not write Airtable by itself.
