#!/usr/bin/env node
/**
 * DISABLED — FOUNDER CORRECTION 2026-11-20
 * Do not compare product coverage against Hotel Census inventory.
 */
console.error(
  [
    "LEGACY_RECONCILIATION_DISABLED",
    "DEPRECATED — PROHIBITED LEGACY DATA SOURCE",
    "Population comparison must not use Hotel Census.",
    "Validate against current HPC inventory + independently verified fixtures only.",
  ].join("\n")
);
process.exit(2);
