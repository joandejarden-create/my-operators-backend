# Operator Explorer Tab Factory — permanent build operation

This is the **default** Operator Explorer setup process (parallel to Brand Explorer Tab Factory). Profile-level “did the tab render?” checks are not sufficient. Every operator must be built and gated **tab-by-tab and field-by-field** against golden quality (**Arbor Lodging** + **Hotel Equities**).

## Permanent sequence

1. Source Pack (company-controlled first; PI Source Library)  
2. Operator Lens (CALA vs enterprise parent; what is in-scope for this Master row)  
3. Tab-by-tab generation (10 publishable tabs)  
4. Field-by-field tab validation  
5. Source provenance by tab  
6. Rendered payload / fixture audit  
7. Patch / suppress / unavailable-state remediation  
8. Section pattern parity vs Arbor + Hotel Equities  
9. Golden benchmark comparison  
10. Founder visual review  
11. Active release only after all gates pass  

## Non-negotiable rule

Ask: *Is every visible rendered field complete, operator-specific, owner-useful, source-supported or intentionally suppressed, and comparable to Arbor / Hotel Equities?*

Hard fails: visible empty fields, empty cards, title-only cards, unsupported zeros, generic name-swappable copy, parent-umbrella-dominated CALA-specific sections without clear labeling.

`auditPass = true` only when `failFindings === 0`. A patch plan is **not** a pass.

## Modules (target layout)

| Concern | Module |
| --- | --- |
| Quality baseline registry | `lib/partner-intelligence/operator-explorer-quality-baseline.js` |
| Tab contracts | `lib/partner-intelligence/operator-explorer-tab-contracts.js` |
| Live / fixture audit | `lib/partner-intelligence/operator-explorer-tab-factory-audit.js` |
| Fixture payload loader | `lib/partner-intelligence/operator-explorer-fixture-payload.js` |
| Sync evaluation | `lib/partner-intelligence/operator-explorer-tab-factory-evaluate.js` |
| Remediation | `lib/partner-intelligence/operator-explorer-tab-factory-remediation.js` |
| Source provenance by tab | `lib/partner-intelligence/operator-explorer-source-provenance-by-tab.js` |
| Empty shell scan | `lib/partner-intelligence/operator-explorer-no-empty-rendered-components.js` |
| Section pattern parity | `lib/partner-intelligence/operator-explorer-section-pattern-parity.js` |
| OS gates | `lib/partner-intelligence/operator-explorer-os.js` |
| Factory queue | `lib/partner-intelligence/operator-explorer-factory-queue.js` |
| Factory init | `lib/partner-intelligence/operator-explorer-factory-init.js` |
| Ready for next | `docs/data-intelligence/operator-explorer-ready-for-next-operator.md` |

Field inventory authority today:

- `lib/partner-intelligence/operator-explorer-registry-catalog.js`
- `docs/operator-explorer-dna-tab-field-audit.md`
- `api/lib/partner-intelligence-explorer-field-registry.js` → `OPERATOR_EXPLORER_TABS`

## Tab pattern notes (owner-facing)

| Tab | Pattern bar (Arbor / HE spirit) |
| --- | --- |
| Profile & Positioning | Specific company story, CALA lens labeled, best-at / why-owner cards with real body copy |
| Operating Platform | Capability depth with owner-useful specifics (not generic “full service”) |
| Brand & Relationships | Named brand relationships / approval status where supported |
| Markets & Footprint | Geography with honest zeros (e.g. CALA managed count) + regional experience labeled |
| Owner Engagement & Reporting | Concrete reporting / engagement model |
| Infrastructure & Data | Systems / data posture without empty chrome |
| Leadership | Named leaders with titles + substantive bios |
| Project Fit & Deal Profile | Best-fit criteria, asset/situation types, clear “not ideal for” |
| Proof & Track Record | Case studies / recognition with substance |
| Operator Materials | Real materials presentation rows — no empty shells |

## Commands

```bash
npm run operator-explorer-tab-factory-audit -- --operators arbor-lodging-cala,hotel-equities-cala --source=fixtures --dry-run

npm run operator-explorer-tab-factory-audit -- --operators arbor-lodging-cala,hotel-equities-cala --source=merged --dry-run

npm run operator-explorer-source-provenance-by-tab -- --operators <slugs> --dry-run

npm run operator-explorer-tab-factory-remediation -- --operators <slugs> --dry-run

npm run operator-explorer-tab-factory-remediation -- --operators <slugs> --apply \
  --approve-operator-tab-factory-remediation \
  --confirm-no-company-validation-changes \
  --confirm-no-release-field-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-protected-operators-unchanged \
  --confirm-no-empty-rendered-fields \
  --confirm-source-provenance-by-tab \
  --confirm-operator-specific-copy \
  --confirm-benchmark-quality-met
```

Reports: `reports/operator-explorer-tab-factory-audit.{json,md}`

`--source` modes:
- `fixtures` — baseline fixture packs only (default; no Airtable required)
- `live` — Airtable detail prefill only
- `merged` — fixtures fill gaps; live wins on non-empty keys

Dry-run is default. Apply requires every confirm flag. **Protected operators (Arbor, Hotel Equities) are never modified by tab-factory remediation** unless the task is an explicit baseline revision.

## OS readiness

`founder_review_ready` / `active_profile_ready` require (as gates land):

- source_provenance_by_tab  
- tab_factory_audit  
- rendered_field_completeness / no_empty_rendered_components  
- section_pattern_parity (vs Arbor + HE)  
- golden_content_quality  

Company Validated is never auto-written.

## Upstream PI workflow (required but not sufficient)

Still run Partner Intelligence production before claiming release readiness:

1. Source capture / stewardship  
2. Fact extract + approve  
3. Profile governance publish (dry-run → apply)  

See: `docs/data-intelligence/dealality-intelligence-production-workflow-v1.md` and `partner-intelligence-priority-profile-production-tracker.md`.

## Related docs

- `docs/data-intelligence/operator-explorer-protected-baseline-rules.md`
- `docs/data-intelligence/operator-explorer-arbor-hotel-equities-quality-baseline.md`
- `docs/data-intelligence/operator-explorer-mandatory-release-gates.md`
- `docs/data-intelligence/brand-explorer-tab-factory-build-operation.md` (Brand parallel)
- `docs/ai-build-system/CURSOR_IMPLEMENTATION_PROTOCOL.md`
- `docs/ai-build-system/AI_BUILD_PROTOCOL.md`

## Change impact

**High** — readiness gates + future explorer materials / fixture remediation writes.

Rollback: disable new OS gates / revert remediation from pre-apply JSON; never leave golden baselines half-written.
