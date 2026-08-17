# Intelligence Governance

What Dealality intelligence can be trusted, shown, scored, refreshed, or validated.

This is the primary data governance document for platform intelligence.

## Purpose

This document defines what Dealality intelligence can be trusted, shown, scored, refreshed, or validated.

## Validation Levels

### Company Validated

Directly confirmed by the brand, operator, owner, advisor, capital partner, or company. Use only when there is explicit confirmation — not inference from public materials.

### Company Published

Taken from official company websites, official PDFs, development brochures, investor materials, press releases, or company-published resources. Credible but not the same as direct validation.

### Source-Informed

Built from credible third-party sources, hospitality media, public databases, market reports, or other reputable materials.

### Owner-Provided

Provided by a hotel owner, investor, broker, or advisor for a specific opportunity. Scoped to that deal context unless separately validated for broader use.

### AI-Assisted

Generated or structured by AI from available inputs and Dealality logic. Must be labeled and must not be presented as fact without review.

### Needs Review

Captured but not yet safe to use for important user-facing outputs.

### Stale / Refresh Needed

Previously useful, but old enough or uncertain enough to require review before high-confidence use.

### Do Not Use

Untrusted, outdated, conflicting, or inappropriate for platform use.

## Usage Permissions

### Internal Only

For research, extraction, and review. Not for external or user-facing display without upgrade.

### Platform Display Allowed

May appear in Dealality UI according to validation level and role access.

### Scoring Allowed

May influence alignment signals, readiness logic, or match-style outputs. Requires appropriate validation and confidence.

### External Snapshot Allowed

May appear in owner-facing snapshots, print views, or shareable outputs. Higher bar than general platform display.

### Company Validated

Reserved for fields or content explicitly confirmed by the company. Implies protection from automatic AI overwrite.

### Do Not Use

Must not power UI, scoring, or exports.

## Do-Not-Overwrite Rule

AI may suggest updates, flag conflicts, create pending revisions, or mark content stale.

AI must not automatically overwrite company-validated data.

## Evidence / Interpretation / Next Action

Every important intelligence output should separate:

- **Evidence:** what the source or user input actually says.
- **Interpretation:** what Dealality believes it may mean.
- **Next Action:** what should happen next (clarify, validate, refresh, compare sources).

## Conflict Resolution

Use this hierarchy:

1. Company-validated regional data.
2. Recent company-published regional data.
3. Recent company-published global data.
4. Credible source-informed third-party data.
5. Owner-provided opportunity data for that specific deal.
6. AI-assisted interpretation.

AI inference never overrides sourced facts.

## Field-Level Confidence

Where possible, confidence should apply to individual fields, not only entire profiles.

Example:

- Company name: High
- CALA presence: Medium
- Third-party management availability: High
- Owner reporting style: Unknown
- Conversion requirements: Low / Needs Review

## External Visibility

Internal notes can be direct.

External language should be careful, neutral, and evidence-aware.

Example internal:

"CALA evidence weak."

Example external:

"Current reviewed sources do not yet provide enough CALA-specific evidence."

## Refresh Rules

Suggested starting refresh cadence:

- Brand/operator company profiles: every 6–12 months.
- Market radar data: every 12 months or after major market changes.
- Travel infrastructure: every 6–12 months.
- Clause library: every 12 months or after material legal/market changes.
- Owner opportunity data: whenever owner updates inputs.
- AI-generated intelligence: whenever underlying data changes.

## Related Documentation

- Validation flow: [DATA_VALIDATION_PROTOCOL.md](./DATA_VALIDATION_PROTOCOL.md)
- Source trust: [SOURCE_RANKING_GUIDE.md](./SOURCE_RANKING_GUIDE.md)
- Brand model: [BRAND_PROFILE_DATA_MODEL.md](./BRAND_PROFILE_DATA_MODEL.md)
- Operator model: [OPERATOR_PROFILE_DATA_MODEL.md](./OPERATOR_PROFILE_DATA_MODEL.md)
