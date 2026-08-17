# Brand Explorer Tab Factory — permanent build operation

This is the **default** Brand Explorer setup process. Profile-level “did the tab render?” checks are not sufficient. Every brand must be built and gated **tab-by-tab and field-by-field** against benchmark quality (Tribute, Kimpton, Radisson Individuals, Design Hotels, Everhome structure).

## Permanent sequence

1. Source Pack  
2. Brand Lens  
3. Tab-by-tab generation  
4. Field-by-field tab validation  
5. Source provenance by tab  
6. Rendered payload audit  
7. Patch / suppress / unavailable-state remediation  
8. Image distinctiveness validation  
9. Golden benchmark comparison  
10. Founder visual review  
11. Active release only after all gates pass  

## Non-negotiable rule

Ask: *Is every visible rendered field complete, brand-specific, owner-useful, source-supported or intentionally suppressed, and comparable to benchmark profiles?*

Hard fails: visible empty fields, empty cards, empty phase boxes, empty bars, title-only cards, unsupported zeros, parent-umbrella-dominated brand-specific sections.

`auditPass = true` only when `failFindings === 0`. A patch plan is **not** a pass.

## Modules

| Concern | Module |
| --- | --- |
| Tab contracts | `lib/partner-intelligence/brand-explorer-tab-contracts.js` |
| Sync evaluation | `lib/partner-intelligence/brand-explorer-tab-factory-evaluate.js` |
| Live audit | `lib/partner-intelligence/brand-explorer-tab-factory-audit.js` |
| Remediation | `lib/partner-intelligence/brand-explorer-tab-factory-remediation.js` |
| Remediation content | `lib/partner-intelligence/brand-explorer-tab-factory-remediation-content.js` |
| Source provenance by tab | `lib/partner-intelligence/brand-explorer-source-provenance-by-tab.js` |
| Empty shell scan | `lib/partner-intelligence/brand-explorer-no-empty-rendered-components.js` |
| Section pattern parity | `lib/partner-intelligence/brand-explorer-section-pattern-parity.js` |
| Recent Momentum contract | `lib/partner-intelligence/brand-explorer-recent-momentum-contract.js` |
| Section pattern content packs | `lib/partner-intelligence/brand-explorer-section-pattern-parity-content*.js` |
| OS gates | `lib/partner-intelligence/brand-explorer-os-gate-evaluator.js` |

## Recent Momentum (template for every brand)

Active and future Brand Explorer profiles use the same openings/press card pattern:

- Named cards (`footprint.momentum`) with Body = date + summary + trailing announcement URL
- Newest → oldest; Proper Case hyperlink labels in the UI
- Content packs via `buildRecentMomentumCard` — never untitled tab-factory diligence blobs
- Gate: `recent_momentum_pattern_pass` (part of `section_pattern_parity`)
- Docs: `docs/data-intelligence/brand-explorer-section-pattern-parity.md`

Future brand checklist: copy `brand-explorer-section-pattern-parity-content-_TEMPLATE.js`, register in `CONTENT_BY_SLUG`, remediate, then `npm run brand-explorer-momentum-announcement-link-restore` if Bodies need URL restore.

## Value Creation Scenarios (permanent template)

Active and future Brand Explorer profiles must ship **four** `valueOwners.scenario.1–4` cards (Title + Body), not the legacy `valueOwners.scenarios` blob alone.

- Gold bar: Ascend Hotel Collection — Proper Case titles + short owner-value paragraphs (~26–58 words)
- Each body names the brand and explains when it creates the most value for owners/projects
- No blanks, no one-liners, no single wall-of-text card with empty siblings
- Packages: `lib/partner-intelligence/brand-explorer-value-creation-scenarios-packages.js`
- Bar: `lib/partner-intelligence/brand-explorer-value-creation-scenarios-bar.js`
- Gate: `npm run brand-explorer-value-creation-scenarios-audit` · `npm run test:brand-explorer-value-creation-scenarios-bar`
- Remediation: `npm run brand-explorer-value-creation-scenarios-remediation -- --dry-run` then `--apply` with `VALUE_CREATION_SCENARIOS_APPLY_FLAGS`

Future brand checklist: add a four-scenario package for the slug, dry-run audit, then remediate Title/Body only.

## Commands

```bash
npm run brand-explorer-tab-factory-audit -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --dry-run

npm run brand-explorer-source-provenance-by-tab -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --dry-run

npm run brand-explorer-tab-factory-remediation -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --dry-run

npm run brand-explorer-tab-factory-remediation -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --apply \
  --approve-tab-factory-remediation \
  --confirm-no-company-validation-changes \
  --confirm-no-release-field-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-protected-brands-unchanged \
  --confirm-no-empty-rendered-fields \
  --confirm-source-provenance-by-tab \
  --confirm-brand-specific-copy \
  --confirm-benchmark-quality-met
```

## OS readiness

`founder_review_ready` / `active_profile_ready` require:

- source_provenance_by_tab  
- tab_factory_audit  
- rendered_field_completeness  
- golden_content_quality  
- image_distinctiveness  
- no_empty_rendered_components  
- `ai_assisted_profile_footnote_visible` (always-on AI-Assisted Profile footnote with Last Reviewed · Source Basis · Region)

Company Validated is never auto-written. Protected brands are never modified by tab-factory remediation.

## Related docs

- `docs/data-intelligence/brand-explorer-ai-assisted-footnote-standardization.md`
- `docs/data-intelligence/brand-explorer-mandatory-release-gates.md`
- `docs/data-intelligence/brand-explorer-source-provenance-by-tab.md`
- `docs/data-intelligence/brand-explorer-rendered-field-completeness.md`
- `docs/ai-build-system/CURSOR_IMPLEMENTATION_PROTOCOL.md`
- `docs/ai-build-system/AI_BUILD_PROTOCOL.md`

## Change impact

**High** — readiness gates + Presentation remediation writes.

Rollback: disable new OS gates / revert Presentation rows from pre-apply remediation JSON.
