# Phase 1.1 — Layer B durable evidence process (read-only)

**Status:** documented process only — not ordinary PR CI  
**Isolation:** `/agent/repos/my-operators-backend-adp-audit`  
**Do not** write Airtable production, invent packages, or commit durable payloads to Git.

## Purpose

Reconcile real durable published records from authoritative storage against certification / permanence contracts. Layer A offline fixtures prove code contracts; Layer B proves production durability.

## Authoritative storage

| Artifact | Path / source | Notes |
|----------|---------------|-------|
| Durable published current | `data/adp-durable-published/current/<propertyId>/manifest.json` | Gitignored; Railway volume / local materialization |
| Durable snapshots | `data/adp-durable-published/snapshots/<propertyId>/` | Immutable report wraps |
| Measured-dates audit | `reports/dealality-adp-single-version-publication-contract-v1/admin-published-vs-measured-dates/PRODUCTION_DURABLE_PUBLISHED_VS_MEASURED.json` | Read-only export; not fabricated |
| Filesystem Primary SoT | `data/ai-demand-positioning/published/<propertyId>/` | Published manifests (publication ≠ certification) |

## Prerequisites

1. Mount or copy durable volume into the ADP worktree (never into the primary GDI checkout as a side effect).
2. Confirm Hilton + Casas (or target) manifests exist under `current/`.
3. Confirm measured-dates JSON exists if running persistence Layer B checks.

## Commands (read-only)

```bash
# Layer A only (CI)
npm run test:adp-permanent-full-e2e-certification-v1

# Layer B required (local durable volume) — EXIT 2 if missing
ADP_PERMANENCE_REQUIRE_LAYER_B=1 npm run test:adp-permanent-full-e2e-certification-v1

# Persistence Layer A + Layer B status
npm run test:adp-e2e-persistence-contract-v1
```

## Reconciliation rules

1. **Publication ≠ certification.** A Live manifest does not authorize CERTIFIED status.
2. **Do not infer certification from publication.**
3. **Do not treat prior periods as comparable** without validating the measurement contract and comparability rules (`formalComparisonUsable`).
4. **Do not modify authoritative production certification states.**
5. Fail closed when durable artifacts are missing — report `BLOCKED_MISSING_AUTHORITATIVE_EVIDENCE`.

## Outputs

Record Layer B results under `reports/adp-hotel-explorer-baseline-audit/phase-1.1/` with:
- properties reconciled
- checksums / period IDs
- pass / fail / blocked
- uncertainties (missing freeze, missing measured-dates, etc.)
