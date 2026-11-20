#!/usr/bin/env node
/**
 * DISABLED — FOUNDER CORRECTION 2026-11-20
 *
 * Hotel Census is NOT a trusted data source.
 * Do not reconcile, sample, import, or match legacy Census into HPC.
 *
 * Exit nonzero so CI / operators cannot accidentally resume this job.
 */
console.error(
  [
    "LEGACY_RECONCILIATION_DISABLED",
    "DEPRECATED — PROHIBITED LEGACY DATA SOURCE",
    "Hotel Census (tblgj2qEwxjTcg6q0) must not be reconciled into HPC.",
    "Use Hotel Property Census (tbl9aY5ijiuIzzWam) + independently verified evidence only.",
    "Canceled job: dealality:legacy-census-hpc-reconcile-full",
  ].join("\n")
);
process.exit(2);
