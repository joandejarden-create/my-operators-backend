# Founder Quality Lessons Registry — reconciliation update

**Registry:** `configs/brand-explorer-founder-quality-lessons-registry-v1.json`

## Policy additions

- `contractIsNotImplementation: true`
- `masterRequirementsRegister` linked
- `requireDispositionForEveryRequirement: true`
- `futureBePrMustPublishLessonsImpactReport: true` (fields: new / preserved / changed / superseded / regressed)
- `ciCoverageCheck: test:be-founder-requirements-coverage-v1`
- `neverSilentlyReactivateSupersededRules: true`

## Lessons added this cycle

| ID | Status | Note |
|---|---|---|
| BE-QL-RECON-001 | TESTED | Contract suite ≠ product completion |
| BE-QL-RECON-002 | TESTED | Lifecycle 6 vs 7 OPEN conflict |
| BE-QL-RECON-003 | ENCODED | Owner Buying dual props contracted, not implemented |

## VERIFIED count

**0** — none marked VERIFIED without browser evidence.

## Traceability

Master register + conflict register + traceability matrix under `reports/brand-explorer/founder-requirements-reconciliation/`.
