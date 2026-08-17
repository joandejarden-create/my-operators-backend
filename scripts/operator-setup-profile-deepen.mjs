#!/usr/bin/env node
/**
 * Deepen Operator Setup Profile & Positioning toward Arbor/HE coverage.
 *
 *   npm run operator-setup-profile-deepen -- --dry-run
 *   npm run operator-setup-profile-deepen -- --apply --approve-operator-setup-profile-deepen --operators aimbridge-latam,tafer-hotels-resorts
 */
import "dotenv/config";
import {
  runOperatorSetupProfileDeepen,
  writeOperatorSetupProfileDeepenReports,
  OPERATOR_SETUP_PROFILE_DEEPEN_VERSION,
} from "../lib/partner-intelligence/operator-setup-profile-deepen.js";

function parseArgs(argv) {
  const out = { apply: false, approve: false, operators: null };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--apply") out.apply = true;
    else if (a === "--dry-run") out.apply = false;
    else if (a === "--approve-operator-setup-profile-deepen") out.approve = true;
    else if (a === "--operators" && argv[i + 1]) {
      out.operators = argv[++i].split(",").map((s) => s.trim()).filter(Boolean);
    }
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  console.log(
    `[${OPERATOR_SETUP_PROFILE_DEEPEN_VERSION}] dryRun=${!args.apply} operators=${(args.operators || ["(all packs)"]).join(",")}`
  );
  const report = await runOperatorSetupProfileDeepen({
    operators: args.operators || undefined,
    apply: args.apply,
    approveApply: args.approve,
  });
  const { jsonPath, mdPath } = writeOperatorSetupProfileDeepenReports(report);
  console.log(`Wrote ${jsonPath}`);
  console.log(`Wrote ${mdPath}`);
  console.log(
    `operators=${report.summary.operators} avgFields=${report.summary.avgFieldCount} errors=${report.summary.errors}`
  );
  for (const r of report.results) {
    console.log(`  ${r.operatorSlug}: ${r.status} fields=${r.fieldCount ?? 0}${r.error ? ` err=${r.error}` : ""}`);
  }
  if (report.summary.errors) process.exitCode = 2;
}

main().catch((err) => {
  console.error(err?.stack || err?.message || err);
  process.exit(1);
});
