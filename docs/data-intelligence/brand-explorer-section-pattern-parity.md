# Brand Explorer Section Pattern Parity

Permanent Tab Factory gate: **section_pattern_parity**.

A section does not pass simply because it has non-empty text. It must match the established Brand Explorer product pattern used by benchmark profiles (Tribute, Kimpton, Radisson Individuals, Design Hotels).

## Mandatory gates

- `recent_momentum_pattern_pass`
- `geographic_footprint_pattern_pass`
- `portfolio_context_pattern_pass`
- `growth_priorities_pattern_pass`

A brand cannot become `founder_review_ready` or `active_profile_ready` unless rendered completeness, no-empty, provenance, image uniqueness, image role-match, **section pattern parity**, and golden content all pass.

## Commands

```bash
npm run brand-explorer-section-pattern-parity-audit -- --dry-run
npm run brand-explorer-section-pattern-parity-remediation -- --brands <failing-slugs> --dry-run
npm run brand-explorer-section-pattern-parity-remediation -- --brands <failing-slugs> --apply \
  --approve-section-pattern-parity-remediation \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-release-field-changes \
  --confirm-no-public-restore-fields \
  --confirm-section-pattern-only \
  --confirm-benchmark-pattern-aligned
npm run test:brand-explorer-section-pattern-parity -- --brands <slugs>
```

Latest audit: 2026-07-23T07:44:32.253Z · pass=18 fail=0

Failing: (none)
