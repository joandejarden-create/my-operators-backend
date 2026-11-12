#!/usr/bin/env node
/**
 * Bethesda 88 commercial demand-type remediation (Guest Rooms / Local Catering / Both).
 *
 * Dry-run:
 *   node scripts/gdi-bethesda-88-commercial-demand-type-remediation-v1.mjs
 *
 * Apply (founder-authorized):
 *   node scripts/gdi-bethesda-88-commercial-demand-type-remediation-v1.mjs \
 *     --apply --founder-authorized \
 *     --approval-token FOUNDER_APPROVE_BETHESDA_COMMERCIAL_DEMAND_TYPE_20261112
 */
import {
  runBethesda88CommercialDemandTypeRemediation,
  BETHESDA_88_COMMERCIAL_DEMAND_TYPE_REMEDIATION_V1,
  DEMAND_TYPE_APPROVAL_TOKEN,
} from "../lib/group-demand-intelligence/research/bethesda-88-commercial-demand-type-remediation-v1.js";

const args = process.argv.slice(2);
const apply = args.includes("--apply");
const founderAuthorization = args.includes("--founder-authorized");
const tokenIdx = args.indexOf("--approval-token");
const approvalToken =
  tokenIdx >= 0
    ? args[tokenIdx + 1]
    : apply && founderAuthorization
      ? DEMAND_TYPE_APPROVAL_TOKEN
      : null;

const report = runBethesda88CommercialDemandTypeRemediation({
  apply,
  founderAuthorization,
  approvalToken,
});

console.log(
  JSON.stringify(
    {
      version: BETHESDA_88_COMMERCIAL_DEMAND_TYPE_REMEDIATION_V1,
      ok: report.ok === true,
      counts: report.counts || null,
      facets: report.facets || null,
      qualityFailures: report.qualityFailures ?? null,
      writeApplied: report.writeApplied === true,
      storeShaBefore: report.storeShaBefore ?? null,
      storeShaAfter: report.storeShaAfter ?? null,
      paidAuthorized: false,
      approvalTokenRequired: DEMAND_TYPE_APPROVAL_TOKEN,
      reportDir: "reports/gdi-bethesda-88-commercial-demand-type-remediation-v1",
      code: report.code || null,
    },
    null,
    2,
  ),
);

if (report.ok !== true) process.exit(1);
