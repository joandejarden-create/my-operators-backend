#!/usr/bin/env node
import "dotenv/config";
import {
  runTabFactoryAudit,
  writeTabFactoryAuditReports,
  AUDIT_VERSION,
} from "../lib/partner-intelligence/brand-explorer-tab-factory-audit.js";
import { TAB_FACTORY_TARGET_BRANDS } from "../lib/partner-intelligence/brand-explorer-tab-contracts.js";

function parseBrands(argv) {
  const idx = argv.indexOf("--brands");
  if (idx >= 0 && argv[idx + 1]) {
    return argv[idx + 1].split(",").map((s) => s.trim()).filter(Boolean);
  }
  return [...TAB_FACTORY_TARGET_BRANDS];
}

async function main() {
  const brands = parseBrands(process.argv.slice(2));
  console.log(`[${AUDIT_VERSION}] tab-factory audit dry-run`);
  console.log(`  brands: ${brands.join(", ")}`);
  const report = await runTabFactoryAudit({ brands, includeBenchmarks: false });
  const paths = writeTabFactoryAuditReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: failFindings=${report.summary.totalFailFindings} empty=${report.summary.totalEmptyRenderFails} patchPlanComplete=${report.patchPlanComplete} auditPass=${report.auditPass}`
  );
  for (const b of report.brandResults) {
    console.log(
      `  ${b.brandSlug}: auditPass=${b.auditPass} fails=${b.failFindings} empty=${b.emptyRenderFailFindings} decision=${b.releaseQualityDecision}`
    );
  }
  if (!report.patchPlanComplete) process.exit(2);
  if (!report.auditPass) process.exit(3);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
