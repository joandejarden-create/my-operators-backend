#!/usr/bin/env node
/**
 * DISABLED — FOUNDER CORRECTION 2026-11-20
 * Legacy Census → HPC dry-run reconciliation is prohibited.
 */
console.error(
  [
    "LEGACY_RECONCILIATION_DISABLED",
    "DEPRECATED — PROHIBITED LEGACY DATA SOURCE",
    "Do not run legacy-to-HPC dry-run reconciliation.",
    "Use HPC-only identity + independent research.",
  ].join("\n")
);
process.exit(2);
