# DEPRECATED — PROHIBITED LEGACY DATA SOURCE

**Founder correction:** 2026-11-20

Hotel Census (`tblgj2qEwxjTcg6q0`) is **not** a trusted data source.

The following artifacts are retained for engineering audit history only.
They must **not** feed research, enrichment, HPC population, identity matching,
alias import, relationship reconstruction, or product coverage inflation.

## Prohibited for pipeline input

| Artifact | Status |
| --- | --- |
| `LEGACY-TO-HPC-RECONCILIATION-MATRIX-FULL-V1.*` | DEPRECATED — PROHIBITED LEGACY DATA SOURCE |
| `LEGACY-TO-HPC-RECONCILIATION-MATRIX-DRY-RUN-V1.*` | DEPRECATED — PROHIBITED LEGACY DATA SOURCE |
| `LEGACY-TO-HPC-MUTATION-PACKAGE-DRY-RUN-V1.*` | DEPRECATED — PROHIBITED LEGACY DATA SOURCE |
| `LEGACY-HPC-POPULATION-COMPARE-V1.*` | DEPRECATED — PROHIBITED LEGACY DATA SOURCE |
| `LEGACY-CENSUS-LINKED-RECORD-MIGRATION-DRY-RUN-V1.*` | DEPRECATED — do not use legacy ids as HPC keys |
| `full-recon-run.log` | DEPRECATED — job canceled |

## Canceled jobs

- `dealality:legacy-census-hpc-reconcile-full` → exits 2
- `dealality:legacy-census-hpc-reconcile-dry-run` → exits 2
- `dealality:legacy-hpc-population-compare` → exits 2
- `dealality:legacy-to-hpc-mutation-package` → exits 2

## Allowed instead

- Hotel Property Census `tbl9aY5ijiuIzzWam`
- Shared HPC identity resolver
- Independently verified research / official sources
- Established canonical intelligence tables linked to HPC ids

**Do not reconcile Hotel Census. Do not migrate its records.**
