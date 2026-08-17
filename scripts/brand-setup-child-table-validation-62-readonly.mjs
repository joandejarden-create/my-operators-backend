#!/usr/bin/env node
/**
 * Brand Setup child-table validation — Active-62 read-only.
 *
 *   npm run brand-setup-child-table-validation-62-readonly -- --dry-run
 */
import "../load-env.js";
import {
  VALIDATION_VERSION,
  runBrandSetupChildTableValidation62,
  writeBrandSetupChildTableValidationReports,
} from "../lib/partner-intelligence/brand-setup-child-table-validation-62.js";

async function main() {
  const argv = process.argv.slice(2);
  if (argv.includes("--apply")) {
    console.error("Refusing --apply. Child-table validation is read-only.");
    process.exit(2);
  }
  if (!argv.includes("--dry-run")) {
    console.error("Require --dry-run (read-only validation; no writes).");
    process.exit(2);
  }

  console.log(`[${VALIDATION_VERSION}] starting (dry-run, no Airtable writes)`);
  const report = await runBrandSetupChildTableValidation62();
  const paths = writeBrandSetupChildTableValidationReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.docsPath}`);
  console.log(`Status: ${report.status}`);
  console.log(`Summary: ${JSON.stringify(report.summary)}`);
  console.log(
    `Writes: airtable=${report.airtableWrites} be=${report.brandExplorerWrites} setup=${report.brandSetupWrites} census=${report.censusWrites}`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
