#!/usr/bin/env node
/**
 * Existing Hotel ADP property certification (read-only, 0 LLM).
 *
 *   npm run audit:adp-property -- <propertyId>
 *   npm run audit:adp-property -- adp_waterstone_boca_raton
 *   npm run audit:adp-property -- --portfolio
 *   npm run audit:adp-property -- --all
 *
 * Does NOT change methodology. Escalates methodology conflicts via gate FAIL + issueClass=METHODOLOGY.
 */

import { runPropertyCertification } from "../lib/ai-demand-positioning/certification/run-property-certification.js";
import { writeCertificationReport } from "../lib/ai-demand-positioning/certification/write-certification-report.js";
import {
  runPortfolioConsistencyAudit,
  writePortfolioConsistencyAudit,
} from "../lib/ai-demand-positioning/certification/run-portfolio-consistency-audit.js";
import { LIVE_EXISTING_HOTEL_PROPERTY_IDS } from "../lib/ai-demand-positioning/certification/certification-status.js";

function usage() {
  console.log(`Usage:
  npm run audit:adp-property -- <propertyId>
  npm run audit:adp-property -- --all
  npm run audit:adp-property -- --portfolio

Live property IDs:
  ${LIVE_EXISTING_HOTEL_PROPERTY_IDS.join("\n  ")}
`);
}

async function main() {
  const args = process.argv.slice(2).filter((a) => a !== "--");
  if (!args.length || args.includes("-h") || args.includes("--help")) {
    usage();
    process.exit(args.length ? 0 : 1);
  }

  const portfolioOnly = args.includes("--portfolio");
  const runAll = args.includes("--all") || portfolioOnly;
  const propertyIds = runAll
    ? [...LIVE_EXISTING_HOTEL_PROPERTY_IDS]
    : args.filter((a) => !a.startsWith("--"));

  if (!propertyIds.length && !portfolioOnly) {
    usage();
    process.exit(1);
  }

  const reports = [];
  for (const propertyId of propertyIds) {
    console.log(`\n=== Certifying ${propertyId} ===`);
    const report = await runPropertyCertification(propertyId);
    if (report.ok === false) {
      console.error(`FAILED: ${report.error || "unknown"}`);
      process.exitCode = 1;
      continue;
    }
    const paths = writeCertificationReport(report);
    reports.push(report);
    console.log(`Status: ${report.status}`);
    console.log(`Material fails: ${report.materialFailCount}; disclosures: ${report.disclosureCount}`);
    console.log(`Wrote: ${paths.md}`);
    console.log(`Wrote: ${paths.json}`);
    const fails = (report.gates || []).filter((g) => g.status === "FAIL");
    if (fails.length) {
      for (const g of fails) {
        console.log(`  FAIL [${g.material ? "MATERIAL" : "soft"}] ${g.gateId}: ${g.summary}`);
      }
    }
  }

  if (portfolioOnly || runAll) {
    console.log(`\n=== Portfolio consistency ===`);
    const audit = await runPortfolioConsistencyAudit({ reports });
    const paths = writePortfolioConsistencyAudit(audit);
    console.log(`Verdict: ${audit.methodologyVerdict}`);
    console.log(`Inconsistent fields: ${audit.inconsistentCount}`);
    console.log(`Wrote: ${paths.md}`);
    console.log(`Wrote: ${paths.json}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
