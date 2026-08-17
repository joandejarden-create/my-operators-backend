/**
 * Generate Scout opportunity signals report (read-only).
 *
 * Usage:
 *   node scripts/generate-scout-opportunity-signals-report.mjs
 *   node scripts/generate-scout-opportunity-signals-report.mjs --country=Mexico --limit=200
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildOpportunitySignalsReport } from "../lib/scout/opportunity-signals.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");

function parseArgs() {
  const query = { includePipeline: "1", limit: "200" };
  for (const arg of process.argv.slice(2)) {
    if (arg.startsWith("--")) {
      const [key, ...rest] = arg.slice(2).split("=");
      query[key] = rest.join("=") || "1";
    }
  }
  return query;
}

async function main() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID_ALT) {
    throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");
  }

  const query = parseArgs();
  const report = await buildOpportunitySignalsReport(query);
  if (!report.ok) throw new Error(report.error);

  mkdirSync(REPORTS, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outPath = join(REPORTS, `scout-opportunity-signals-${stamp}.json`);

  writeFileSync(
    outPath,
    JSON.stringify(
      {
        generatedAt: report.source.generatedAt,
        query,
        summary: report.summary,
        warnings: report.warnings,
        source: report.source,
        signals: report.signals,
      },
      null,
      2
    )
  );

  console.log("Scout opportunity signals report written:", outPath);
  console.log("signalsReturned:", report.summary.signalsReturned);
  console.log("bySignalType:", report.summary.bySignalType);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
