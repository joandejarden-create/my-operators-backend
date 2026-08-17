#!/usr/bin/env node
/**
 * Operator Explorer source provenance by tab (dry-run).
 *
 *   npm run operator-explorer-source-provenance-by-tab -- --source=fixtures --dry-run
 *   npm run operator-explorer-source-provenance-by-tab -- --source=merged --dry-run
 */
import "dotenv/config";
import {
  OPERATOR_SOURCE_PROVENANCE_VERSION,
  runOperatorSourceProvenanceAudit,
  writeOperatorSourceProvenanceReports,
} from "../lib/partner-intelligence/operator-explorer-source-provenance-by-tab-audit.js";
import { OPERATOR_QUALITY_BASELINE_OPERATORS } from "../lib/partner-intelligence/operator-explorer-quality-baseline.js";

function parseArgs(argv) {
  const out = { operators: null, source: "fixtures" };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--operators" && argv[i + 1]) {
      out.operators = argv[++i].split(",").map((s) => s.trim()).filter(Boolean);
    } else if (a.startsWith("--operators=")) {
      out.operators = a.slice("--operators=".length).split(",").map((s) => s.trim()).filter(Boolean);
    } else if (a === "--source" && argv[i + 1]) {
      out.source = String(argv[++i]).trim().toLowerCase();
    } else if (a.startsWith("--source=")) {
      out.source = a.slice("--source=".length).trim().toLowerCase();
    }
  }
  if (!["fixtures", "live", "merged"].includes(out.source)) {
    throw new Error(`Invalid --source=${out.source}`);
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const operators =
    args.operators || OPERATOR_QUALITY_BASELINE_OPERATORS.map((o) => o.slug);
  console.log(`[${OPERATOR_SOURCE_PROVENANCE_VERSION}] source provenance by tab`);
  console.log(`  source: ${args.source}`);
  console.log(`  operators: ${operators.join(", ")}`);

  const report = await runOperatorSourceProvenanceAudit({
    operators,
    source: args.source,
  });
  const paths = writeOperatorSourceProvenanceReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: pass=${report.summary.pass} fail=${report.summary.fail} failing=${report.summary.failingSlugs.join(",") || "none"}`
  );
  for (const o of report.operatorResults) {
    console.log(
      `  ${o.operatorSlug}: pass=${o.pass} sources=${o.sourceCount} failures=${(o.failures || []).join(",") || "—"}`
    );
  }
  if (!report.auditPass) process.exitCode = 3;
}

main().catch((err) => {
  console.error(err?.stack || err?.message || err);
  process.exit(1);
});
