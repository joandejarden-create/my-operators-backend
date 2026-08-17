#!/usr/bin/env node
/**
 * Apply official-website content into Operator Setup linked tabs.
 * Bootstraps missing 1:1 links when --bootstrap-if-missing is set.
 *
 *   npm run operator-setup-website-content-apply -- --dry-run --bootstrap-if-missing
 *   npm run operator-setup-website-content-apply -- --apply --approve-operator-setup-website-content-apply --bootstrap-if-missing --operators tafer-hotels-resorts
 */
import "dotenv/config";
import {
  runOperatorSetupWebsiteContentApply,
  writeOperatorSetupWebsiteContentApplyReports,
  OPERATOR_SETUP_WEBSITE_CONTENT_APPLY_VERSION,
} from "../lib/partner-intelligence/operator-setup-website-content-apply.js";

function parseArgs(argv) {
  const out = {
    apply: false,
    approveApply: false,
    bootstrapIfMissing: false,
    operators: null,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--apply") out.apply = true;
    else if (a === "--dry-run") out.apply = false;
    else if (a === "--approve-operator-setup-website-content-apply") out.approveApply = true;
    else if (a === "--bootstrap-if-missing") out.bootstrapIfMissing = true;
    else if (a === "--operators" && argv[i + 1]) {
      out.operators = argv[++i].split(",").map((s) => s.trim()).filter(Boolean);
    }
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  console.log(
    `[${OPERATOR_SETUP_WEBSITE_CONTENT_APPLY_VERSION}] dryRun=${!args.apply} bootstrap=${args.bootstrapIfMissing}`
  );
  const report = await runOperatorSetupWebsiteContentApply({
    operators: args.operators || undefined,
    apply: args.apply,
    approveApply: args.approveApply,
    bootstrapIfMissing: args.bootstrapIfMissing,
    approveBootstrap: args.approveApply,
  });
  const paths = writeOperatorSetupWebsiteContentApplyReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `operators=${report.summary.operators} tables=${report.summary.tablesTouched} errors=${report.summary.errors}`
  );
  for (const r of report.results) {
    const statuses = (r.tables || []).map((t) => `${t.tableName.split(" - ").pop()}:${t.status}`).join("; ");
    console.log(`  ${r.operatorSlug}: ${statuses || r.error || "—"}`);
  }
  if (report.summary.errors > 0) process.exit(2);
}

main().catch((err) => {
  console.error(err?.stack || err?.message || err);
  process.exit(1);
});
