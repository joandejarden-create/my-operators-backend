/**
 * Smoke: extraction run → list Pending facts (Arbor platforms source).
 * Usage: node scripts/partner-intelligence-extract-smoke.mjs [--apply] [--source rec…]
 */
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, "..", ".env.local") });
dotenv.config({ path: path.join(__dirname, "..", ".env") });

const apply = process.argv.includes("--apply");
const sourceArg = process.argv.find((a) => a.startsWith("--source="));
const sourceId = sourceArg ? sourceArg.split("=")[1] : "recM4HuV9r5Gz35P7";

process.env.PARTNER_INTELLIGENCE_EXTRACTION_ENABLED = "1";

const { runPartnerSourceExtraction } = await import("../lib/partner-intelligence/run-extraction.js");
const { listPartnerFacts } = await import("../lib/partner-intelligence/airtable-facts.js");

if (!apply) {
  console.log("Dry run — pass --apply to create facts in Airtable.");
  console.log("Source:", sourceId);
  process.exit(0);
}

const result = await runPartnerSourceExtraction(sourceId, { force: true });
console.log("Extraction:", {
  runId: result.runId,
  factsCreated: result.factsCreated,
  documentKind: result.documentKind,
});

const pending = await listPartnerFacts({
  operatorId: "recF5Z87OAqFgndoq",
  humanReviewStatus: "Pending",
  extractionRunId: result.runId,
});
console.log(
  "Pending facts this run:",
  pending.facts.map((f) => ({ id: f.id, field: f.fieldName, value: f.extractedValue?.slice(0, 80) }))
);
