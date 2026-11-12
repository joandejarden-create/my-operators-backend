#!/usr/bin/env node
/**
 * Apply founder-approved Woman's Club + Strathmore zero-paid contact updates.
 *
 *   node scripts/gdi-bethesda-apply-approved-contact-updates-v1.mjs --dry-run
 *   node scripts/gdi-bethesda-apply-approved-contact-updates-v1.mjs --apply
 */
import { applyFounderApprovedContactUpdates } from "../lib/group-demand-intelligence/research/bethesda-approved-contact-updates-v1.js";

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run") || !args.includes("--apply");
const result = applyFounderApprovedContactUpdates({ dryRun });
console.log(JSON.stringify(result, null, 2));
if (!result.ok) process.exitCode = 2;
