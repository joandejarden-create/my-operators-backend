# Operator Explorer Section Pattern Parity

Permanent Tab Factory gate: **section_pattern_parity**.

A section does not pass simply because it has non-empty text. It must match the established Operator Explorer product pattern used by golden baselines (**Arbor Lodging** + **Hotel Equities**).

## Mandatory section gates

- `company_story_pattern_pass`
- `operating_platform_pillars_pattern_pass`
- `brand_relationships_pattern_pass`
- `markets_footprint_pattern_pass`
- `leadership_pattern_pass`
- `owner_engagement_pattern_pass`
- `deal_fit_pattern_pass`
- `proof_track_record_pattern_pass`

An operator cannot become `founder_review_ready` / `active_profile_ready` unless field completeness **and** section pattern parity pass.

## Markets / footprint special rule

Honest **zero CALA managed footprint** is allowed when clearly labeled **and** team/regional experience cards are present (Arbor pattern). Unlabeled empty markets fail.

## Commands

```bash
npm run operator-explorer-section-pattern-parity-audit -- --source=fixtures --dry-run
npm run operator-explorer-section-pattern-parity-audit -- --source=merged --dry-run
npm run test:operator-explorer-section-pattern-parity

# After pattern gate: baseline field-gap remediation (Arbor + HE)
npm run operator-explorer-baseline-gap-remediation -- --source=merged --dry-run
npm run operator-explorer-baseline-gap-remediation -- --source=merged --apply \
  --approve-operator-baseline-gap-remediation --confirm-fixture-overlay-only
```

Reports:
- `reports/operator-explorer-section-pattern-parity-audit.{json,md}`
- `reports/operator-explorer-baseline-gap-remediation.{json,md}`
- Overlays (apply): `fixtures/operator-explorer-baseline-overlays/*.json`
## Modules

- `lib/partner-intelligence/operator-explorer-section-pattern-parity.js`
- `lib/partner-intelligence/operator-explorer-section-pattern-parity-audit.js`

## Related

- `docs/data-intelligence/operator-explorer-tab-factory-build-operation.md`
- `docs/data-intelligence/operator-explorer-arbor-hotel-equities-quality-baseline.md`
- Brand parallel: `docs/data-intelligence/brand-explorer-section-pattern-parity.md`
