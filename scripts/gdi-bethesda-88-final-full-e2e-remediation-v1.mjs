#!/usr/bin/env node
/**
 * Bethesda 88 final Full E2E remediation.
 *
 * Dry-run:
 *   node scripts/gdi-bethesda-88-final-full-e2e-remediation-v1.mjs
 *
 * Apply (founder-authorized):
 *   node scripts/gdi-bethesda-88-final-full-e2e-remediation-v1.mjs \
 *     --apply --founder-authorized \
 *     --approval-token FOUNDER_APPROVE_BETHESDA_FINAL_FULL_E2E_REMEDIATION_20261112 \
 *     --wave2-token FOUNDER_APPROVE_BETHESDA_WAVE2_CONTACT_PATCHES_20261112
 */
import {
  runBethesda88FinalFullE2eRemediation,
  BETHESDA_88_FINAL_FULL_E2E_REMEDIATION_V1,
  FINAL_REMEDIATION_APPROVAL_TOKEN,
} from "../lib/group-demand-intelligence/research/bethesda-88-final-full-e2e-remediation-v1.js";
import { WAVE2_CANONICAL_APPROVAL_TOKEN } from "../lib/group-demand-intelligence/research/bethesda-88-contact-completion-wave2-v1.js";

const args = process.argv.slice(2);
const apply = args.includes("--apply");
const founderAuthorization = args.includes("--founder-authorized");
const tokenIdx = args.indexOf("--approval-token");
const approvalToken = tokenIdx >= 0 ? args[tokenIdx + 1] : null;
const waveIdx = args.indexOf("--wave2-token");
const wave2ApprovalToken =
  waveIdx >= 0 ? args[waveIdx + 1] : WAVE2_CANONICAL_APPROVAL_TOKEN;

const report = runBethesda88FinalFullE2eRemediation({
  applyHistorical: true,
  applyAcvnuContact: apply && founderAuthorization,
  applyFactualPatches: apply && founderAuthorization,
  founderAuthorization,
  approvalToken:
    approvalToken ||
    (apply && founderAuthorization ? FINAL_REMEDIATION_APPROVAL_TOKEN : null),
  wave2ApprovalToken,
});

console.log(
  JSON.stringify(
    {
      version: BETHESDA_88_FINAL_FULL_E2E_REMEDIATION_V1,
      ok: report.ok === true,
      beforeNamed: report.before?.raw?.named ?? null,
      afterNamed: report.after?.raw?.named ?? null,
      credibleMissingBefore: report.before?.credibleMissingNamed ?? null,
      credibleMissingAfter: report.after?.credibleMissingNamed ?? null,
      acvnuApplied: report.acvnuPersist?.writeApplied === true,
      factualApplied: report.factualPersist?.writeApplied === true,
      storeShaBefore: report.storeShaBefore ?? null,
      storeShaAfter: report.storeShaAfter ?? null,
      paidAuthorized: false,
      approvalTokenRequired: FINAL_REMEDIATION_APPROVAL_TOKEN,
      reportDir: "reports/gdi-bethesda-88-final-full-e2e-remediation-v1",
      code: report.code || null,
    },
    null,
    2
  )
);

if (report.ok !== true) process.exit(1);
