#!/usr/bin/env node
/**
 * Bethesda 29 historical title standardization.
 *
 *   node scripts/gdi-bethesda-29-title-standardization-v1.mjs --dry-run
 *   node scripts/gdi-bethesda-29-title-standardization-v1.mjs --apply
 */
import {
  stageBethesda29TitleStandardization,
  BETHESDA_29_TITLE_STANDARDIZATION_V1,
} from "../lib/group-demand-intelligence/research/bethesda-29-title-standardization-v1.js";

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run") || !args.includes("--apply");

const result = stageBethesda29TitleStandardization({ dryRun });
console.log(
  JSON.stringify(
    {
      version: BETHESDA_29_TITLE_STANDARDIZATION_V1,
      dryRun,
      ok: result.ok,
      changedCount: result.audit?.changedCount,
      intentionallyGenericCount: result.audit?.intentionallyGenericCount,
      writeApplied: result.writeApplied === true,
      crosswalk: result.audit?.crosswalk?.map((r) => ({
        id: r.id,
        before: r.beforeTitle,
        after: r.afterTitle,
        changed: r.changed,
      })),
    },
    null,
    2
  )
);
if (!result.ok) process.exitCode = 2;
