#!/usr/bin/env node
/**
 * Brand Explorer — AI-Assisted Profile footnote standardization CLI.
 *
 *   --dry-run   Audit + confirm code path (default)
 *   --audit     Raw + enriched audit reports
 *   --apply     Confirm global rendering (requires approval flags; no Airtable writes)
 */
import "dotenv/config";
import {
  APPLY_FLAGS,
  STANDARDIZATION_VERSION,
  runFootnoteAudit,
  runStandardization,
} from "../lib/partner-intelligence/brand-explorer-ai-assisted-footnote-standardization.js";

function parseArgs(argv) {
  return {
    dryRun: argv.includes("--dry-run") || (!argv.includes("--apply") && !argv.includes("--audit")),
    audit: argv.includes("--audit"),
    apply: argv.includes("--apply"),
    argv,
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${STANDARDIZATION_VERSION}] AI-Assisted Profile footnote standardization`);

  if (opts.audit && !opts.apply) {
    const raw = await runFootnoteAudit({ mode: "raw", includeFactoryPreview: true });
    console.log(`Raw audit: pass=${raw.report.summary.pass} fail=${raw.report.summary.fail}`);
    console.log(`Wrote ${raw.paths.jsonPath}`);
    console.log(`Wrote ${raw.paths.mdPath}`);
    const enriched = await runFootnoteAudit({ mode: "enriched", includeFactoryPreview: true });
    console.log(
      `Enriched audit: pass=${enriched.report.summary.pass} fail=${enriched.report.summary.fail}`
    );
    console.log(`Wrote ${enriched.paths.jsonPath}`);
    if (enriched.report.summary.fail > 0) process.exitCode = 2;
    return;
  }

  if (opts.apply) {
    const missing = APPLY_FLAGS.filter((f) => !opts.argv.includes(f));
    if (missing.length) {
      console.error("Missing apply flags:\n" + missing.map((f) => `  ${f}`).join("\n"));
      process.exitCode = 1;
      return;
    }
  }

  const result = await runStandardization({
    argv: opts.argv,
    dryRun: !opts.apply,
    apply: opts.apply,
  });

  if (result.error === "missing_apply_flags") {
    console.error("Missing apply flags:\n" + (result.missingFlags || []).map((f) => `  ${f}`).join("\n"));
    process.exitCode = 1;
    return;
  }

  console.log(`Ready: ${result.report.readyState}`);
  console.log(`Airtable writes: ${result.report.airtableWrites}`);
  console.log(`Wrote ${result.paths.jsonPath}`);
  console.log(`Wrote ${result.paths.mdPath}`);
  console.log(`Wrote ${result.paths.docPath}`);
  if (!result.ok) process.exitCode = 2;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
