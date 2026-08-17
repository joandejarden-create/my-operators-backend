#!/usr/bin/env node
/**
 * Apply square companyLogo attachments to Operator Setup Profile.
 *
 *   npm run operator-setup-company-logo-apply -- --dry-run
 *   npm run operator-setup-company-logo-apply -- --apply --approve-operator-setup-company-logo-apply
 */
import "dotenv/config";
import {
  runOperatorSetupCompanyLogoApply,
  writeOperatorSetupCompanyLogoApplyReports,
  OPERATOR_SETUP_COMPANY_LOGO_APPLY_VERSION,
} from "../lib/partner-intelligence/operator-setup-company-logo-apply.js";

function parseArgs(argv) {
  const out = { apply: false, approve: false, operators: null };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--apply") out.apply = true;
    else if (a === "--dry-run") out.apply = false;
    else if (a === "--approve-operator-setup-company-logo-apply") out.approve = true;
    else if (a === "--operators" && argv[i + 1]) {
      out.operators = argv[++i].split(",").map((s) => s.trim()).filter(Boolean);
    }
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  console.log(
    `[${OPERATOR_SETUP_COMPANY_LOGO_APPLY_VERSION}] dryRun=${!args.apply}`
  );
  const report = await runOperatorSetupCompanyLogoApply({
    operators: args.operators || undefined,
    apply: args.apply,
    approveApply: args.approve,
  });
  const { jsonPath, mdPath } = writeOperatorSetupCompanyLogoApplyReports(report);
  console.log(`Wrote ${jsonPath}`);
  console.log(`Wrote ${mdPath}`);
  for (const r of report.results) {
    console.log(
      `  ${r.operatorSlug}: ${r.status}${r.probe ? ` ${r.probe.bytes || 0}B` : ""}${r.error ? ` err=${r.error}` : ""}`
    );
  }
  if (report.summary.errors) process.exitCode = 2;
}

main().catch((err) => {
  console.error(err?.stack || err?.message || err);
  process.exit(1);
});
