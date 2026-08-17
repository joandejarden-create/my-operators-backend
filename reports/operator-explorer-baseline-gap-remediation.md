# Operator Explorer Baseline Gap Remediation

Version: `operator-baseline-gap-remediation-v1`
Generated: 2026-07-24T12:52:36.112Z
dryRun: **true** · source: **merged** · writeKind: **none**

## Summary

- Fail findings before → after: **41 → 0**
- fieldAuditPass after: **true**
- sectionPatternPass after: **true**
- full auditPass after: **true**

## Arbor Lodging (CALA)

- Before: fails=28 fieldPass=false sectionPass=true
- After: fails=0 fieldPass=true sectionPass=true
- Delta fails: **28**
- Intentional suppress: op.snapshot.parentCompany, op.markets.activeCountries, op.markets.activeMarkets, op.proof.ownerReferences, op.proof.lenderReferences
- Overlay path: (dry-run)

## Hotel Equities (CALA)

- Before: fails=13 fieldPass=false sectionPass=false
- After: fails=0 fieldPass=true sectionPass=true
- Delta fails: **13**
- Intentional suppress: op.snapshot.parentCompany, op.proof.ownerReferences, op.proof.lenderReferences
- Overlay path: (dry-run)
