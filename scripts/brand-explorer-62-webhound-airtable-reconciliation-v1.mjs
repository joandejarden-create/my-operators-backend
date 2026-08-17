#!/usr/bin/env node
/**
 * Brand Explorer 62 — Webhound ↔ Airtable reconciliation (read-only).
 *
 *   npm run brand-explorer-62-webhound-airtable-reconciliation-v1 -- --dry-run
 */
import "../load-env.js";
import {
  RECON_VERSION,
  runWebhoundAirtableReconciliation,
  writeReconciliationArtifacts,
} from "../lib/partner-intelligence/brand-explorer-62-webhound-airtable-reconciliation.js";

async function main() {
  const argv = process.argv.slice(2);
  if (argv.includes("--apply")) {
    console.error("Refusing --apply. Reconciliation is read-only (no patch writes).");
    process.exit(2);
  }
  if (!argv.includes("--dry-run")) {
    console.error("Require --dry-run (read-only; no Airtable writes).");
    process.exit(2);
  }

  console.log(`[${RECON_VERSION}] reconcile Webhound claims vs live Airtable (read-only)`);
  const report = await runWebhoundAirtableReconciliation();
  const paths = writeReconciliationArtifacts(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.docsPath}`);
  console.log(`Status: ${report.status}`);
  console.log(`Summary: ${JSON.stringify(report.summary)}`);
  console.log(
    `Writes: airtable=${report.airtableWrites} be=${report.brandExplorerWrites} setup=${report.brandSetupWrites} momentum=${report.recentMomentumWrites}`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
