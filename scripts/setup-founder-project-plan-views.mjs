/**
 * Generate manual setup guide for Founder Project Plan daily & phase views.
 *
 *   node scripts/setup-founder-project-plan-views.mjs
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getGtmConfig, fetchAllTables } from "../lib/dealality-master-todo/master-todo-airtable-io.js";
import { MASTER_TODO_DEFAULT_TABLE_ID } from "../lib/dealality-master-todo/master-todo-field-map.js";
import { buildFppViewsManualMarkdown } from "../lib/dealality-master-todo/founder-project-plan-view-config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORT_MD = path.resolve(ROOT, "reports/founder-project-plan-daily-views-manual.md");

async function main() {
  const { baseId, token } = getGtmConfig();
  const tables = await fetchAllTables(baseId, token);
  const table = tables.find((t) => t.id === MASTER_TODO_DEFAULT_TABLE_ID);
  const md = buildFppViewsManualMarkdown({
    baseId,
    tableId: MASTER_TODO_DEFAULT_TABLE_ID,
    tableName: table?.name || "Founder Project Plan",
  });
  fs.mkdirSync(path.dirname(REPORT_MD), { recursive: true });
  fs.writeFileSync(REPORT_MD, md);
  console.log(`\nView setup guide written to:\n  ${REPORT_MD}`);
  console.log("\nAirtable API cannot create views here — follow the guide in the Airtable UI.");
}

main().catch((err) => {
  console.error("[setup-founder-project-plan-views]", err.message || err);
  process.exit(1);
});
