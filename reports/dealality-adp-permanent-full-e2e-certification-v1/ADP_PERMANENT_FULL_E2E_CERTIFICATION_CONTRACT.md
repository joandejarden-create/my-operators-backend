# ADP Permanent Full E2E Certification Contract

**Contract ID:** `ADP_PERMANENT_FULL_E2E_CERTIFICATION_CONTRACT_V1`  
**Gate module:** `lib/ai-demand-positioning/certification/permanent-full-e2e/`  
**Orchestrator stage:** `S8_MEASUREMENT_RECONCILIATION` (hard gate before `S9_CUSTOMER_REPORT`)

## Mandatory sequence

```
AI research execution
  → Original response preservation
  → Scenario eligibility certification (frozen eligibleScenarioIds)
  → Property-specific mention adjudication (detectPropertyMention on raw text)
  → Canonical measurement calculation
  → Provider / scenario / territory / competitive reconciliation
  → Evidence and methodology certification
  → Airtable persistence verification (authorized separately)
  → Immutable report package generation
  → Full report and PDF reconciliation
  → Customer-facing browser rendering certification
  → APPROVED_FOR_PUBLICATION
  → Founder-authorized publication
  → Production Admin / PDF / share parity verification
  → CUSTOMER_READY
```

No stage may be skipped. Downstream cannot override a failed upstream gate.

## Authority rules

1. **Original AI response text** (or immutable hash reference) is authoritative.
2. **`detectPropertyMention(rawResponse)`** is the certified mention methodology.
3. **Runtime `mentioned` flags are non-authoritative.**
4. Missing/unreadable response text → **fail closed**.
5. Report builders, PDF renderers, Admin, and share APIs **must not independently recalculate** certified metrics.
6. Certification changes require a **new immutable package version**.

## Enforcement points

| Point | Code |
|-------|------|
| After measurement calculation | `AFTER_MEASUREMENT_CALCULATION` |
| Before report generation | `BEFORE_REPORT_GENERATION` |
| Before publication package approval | `BEFORE_PUBLICATION_PACKAGE_APPROVAL` |
| Before publication | `BEFORE_PUBLICATION` |
| After production publication | `AFTER_PRODUCTION_PUBLICATION` |
| Before Customer Ready | `BEFORE_CUSTOMER_READY` |

Implemented via `enforceMeasurementReconciliationGate` + `assertFullE2eManifestCertification` in the Full E2E orchestrator.

## Lifecycle states

Success: `RESEARCH_COMPLETE` → `EVIDENCE_VALIDATED` → `ELIGIBILITY_CERTIFIED` → `METRICS_RECONCILED` → `EVIDENCE_CERTIFIED` → `PERSISTENCE_VERIFIED` → `PACKAGE_CERTIFIED` → `RENDER_CERTIFIED` → `APPROVED_FOR_PUBLICATION` → `PUBLISHED` → `CUSTOMER_READY`

Failure: `EVIDENCE_INCOMPLETE` | `ELIGIBILITY_MISMATCH` | `METRIC_RECONCILIATION_FAILED` | `PERSISTENCE_INCOMPLETE` | `PACKAGE_INTEGRITY_FAILED` | `RENDER_VALIDATION_FAILED` | `PRODUCTION_PARITY_FAILED`

## One population contract

Every metric must declare: measurement family, eligible scenario IDs, eligible observation IDs, provider attribution, numerator, denominator, exclusion rules, calculation version.

Consideration / Provider Presence / Scenario Presence (appearance family) share frozen `eligibleScenarioIds` unless a metric documents an alternate population. Mixed 78-scenario + Independent Unprompted V1.1.1 populations are rejected.

## APIs

- `certifyAdpMeasurementReconciliation()`
- `certifyAdpImmutablePackage()`
- `certifyAdpCustomerVisibleParity()`
- `buildCorrectedPackageFromCertification()` (offline Group B prep only)

## Authorization

This contract does **not** authorize paid research, Airtable writes, production publish, or share rotation.
