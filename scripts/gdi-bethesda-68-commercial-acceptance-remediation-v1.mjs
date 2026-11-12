#!/usr/bin/env node
/**
 * Apply Bethesda 68 commercial-acceptance factual remediations.
 * Default: dry-run. Live write requires:
 *   --apply --founder-authorize --token FOUNDER_APPROVE_BETHESDA_68_COMMERCIAL_ACCEPTANCE_REMEDIATION_20261112
 */
import {
  applyAcceptanceRemediationAtomic,
  ACCEPTANCE_REMEDIATION_APPROVAL_TOKEN,
  buildAcceptanceRemediationDryRun,
} from "../lib/group-demand-intelligence/research/bethesda-68-commercial-acceptance-remediation-v1.js";

const args = process.argv.slice(2);
const apply = args.includes("--apply");
const founderAuthorize = args.includes("--founder-authorize");
const tokenIdx = args.indexOf("--token");
const token = tokenIdx >= 0 ? args[tokenIdx + 1] : null;

if (!apply) {
  const dry = buildAcceptanceRemediationDryRun();
  console.log(JSON.stringify({ mode: "dry-run-preview", ...dry }, null, 2));
  process.exit(0);
}

const result = applyAcceptanceRemediationAtomic({
  founderAuthorization: founderAuthorize,
  approvalToken: token || ACCEPTANCE_REMEDIATION_APPROVAL_TOKEN,
  dryRun: false,
});
console.log(JSON.stringify(result, null, 2));
if (!result.ok || !result.writeApplied) process.exit(2);
