/**
 * Patch Case Summary columns on existing Radisson materials.caseStudy rows.
 * Usage: node scripts/patch-radisson-case-study-summaries.mjs [--dry-run]
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const TABLE = "Brand Setup - Brand Explorer Presentation";
const FIXTURE = path.join(ROOT, "fixtures/brand-explorer-presentation-radisson-case-studies.json");

const RECORD_IDS_BY_TITLE = {
  "Radisson Riviera Panama": "recxGsxZJ0KTER24c",
  "Radisson Puebla Angelópolis": "recTI61RFUL3wahyD",
  "Radisson San Luis Potosí, Aeropuerto": "recW2WqF79tFyzF0w",
};

function summaryFields(row) {
  return {
    "Case Summary Overview": String(row.caseSummaryOverview || "").trim(),
    "Case Summary Owner Objective": String(row.caseSummaryOwnerObjective || "").trim(),
    "Case Summary Brand Relevance": String(row.caseSummaryBrandRelevance || "").trim(),
    "Case Summary Interpretation": String(row.caseSummaryInterpretation || "").trim(),
    "Case Summary Tags": String(row.caseSummaryTags || "").trim(),
  };
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const data = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

  for (const row of data.rows) {
    const title = String(row.title || "").trim();
    const id = RECORD_IDS_BY_TITLE[title];
    if (!id) {
      console.error("No record id for title:", title);
      process.exit(1);
    }
    const fields = summaryFields(row);
    console.log(dryRun ? "Would update" : "Updating", id, title);
    if (!dryRun) {
      await base(TABLE).update(id, fields);
    }
  }
  console.log(dryRun ? "Dry run complete." : "Patched 3 row(s).");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
