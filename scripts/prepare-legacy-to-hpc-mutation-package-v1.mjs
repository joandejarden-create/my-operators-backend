#!/usr/bin/env node
/**
 * DISABLED — FOUNDER CORRECTION 2026-11-20
 * Do not generate HPC CREATE / null-fill packages from Hotel Census inventory.
 */
console.error(
  [
    "LEGACY_DATA_IMPORT_BLOCKED",
    "DEPRECATED — PROHIBITED LEGACY DATA SOURCE",
    "Mutation packages must not originate from legacy Hotel Census records.",
    "Use independent research + existing HPC-native upsert/null-fill only.",
  ].join("\n")
);
process.exit(2);
