#!/usr/bin/env node
/**
 * Apply founder-approved 12 PASS Bethesda canonical contact patches.
 *
 *   node scripts/gdi-bethesda-88-apply-approved-canonical-contact-patches-v1.mjs --dry-run
 *   node scripts/gdi-bethesda-88-apply-approved-canonical-contact-patches-v1.mjs --apply --founder-authorized
 */
import { applyApprovedCanonicalContactPatchesV1 } from "../lib/group-demand-intelligence/research/bethesda-88-apply-approved-canonical-contact-patches-v1.js";

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run") || !args.includes("--apply");
const founderAuthorization = args.includes("--founder-authorized");

const result = applyApprovedCanonicalContactPatchesV1({
  dryRun,
  founderAuthorization,
});

console.log(JSON.stringify(result, null, 2));
if (!result.ok) process.exit(1);
if (!dryRun && !result.writeApplied) process.exit(2);
