#!/usr/bin/env node
/**
 * Ensure GDI Hotel Research Opportunities table (founder-approved).
 * Usage:
 *   node scripts/ensure-gdi-hotel-research-opportunities-schema-v1.mjs
 *   node scripts/ensure-gdi-hotel-research-opportunities-schema-v1.mjs --apply
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { ensureHotelResearchOpportunitiesSchema } from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-opportunities-schema-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const APPLY = process.argv.includes("--apply");
const OUT = path.join(
  ROOT,
  "reports/gdi-airtable-research-table-and-initial-backfill-v1",
);

const result = await ensureHotelResearchOpportunitiesSchema({ apply: APPLY });
fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(
  path.join(OUT, "SCHEMA_CREATION_RESULTS.json"),
  `${JSON.stringify(result, null, 2)}\n`,
);

const md = `# Schema creation results

- Mode: **${result.report.mode}**
- OK: **${result.ok}**
- Base: \`${result.report.baseId}\`
- Table: **${result.report.tableName}**
- Table ID: \`${result.report.tableId || "pending"}\`
- Created table: ${result.report.createdTable}
- Would create: ${result.report.wouldCreateTable}

## Field IDs

| Field | ID | Type |
|-------|----|------|
${(result.report.fields || [])
  .map((f) => `| ${f.name} | \`${f.id || ""}\` | ${f.type || ""} |`)
  .join("\n")}

## Errors

${
  result.report.errors?.length
    ? result.report.errors.map((e) => `- ${JSON.stringify(e)}`).join("\n")
    : "_None_"
}
`;
fs.writeFileSync(path.join(OUT, "SCHEMA_CREATION_RESULTS.md"), md);

console.log(JSON.stringify({ ok: result.ok, tableId: result.report.tableId, mode: result.report.mode }, null, 2));
if (!result.ok) process.exitCode = 1;
