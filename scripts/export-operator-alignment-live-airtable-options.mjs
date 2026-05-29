#!/usr/bin/env node
/**
 * Export live Airtable field options for Operator Alignment fields.
 *   node scripts/export-operator-alignment-live-airtable-options.mjs
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  fetchLiveAirtableTables,
  buildLiveOptionsIndex,
  comparePlannedToLive,
  LIVE_OPTIONS_JSON,
} from "../lib/operator-alignment-airtable-options-loader.js";
import { OAS_AUDIT_FIELD_SPECS, OAS_AUDIT_TABLES } from "../lib/operator-alignment-airtable-options-registry.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE credentials required");

  const tables = await fetchLiveAirtableTables(baseId, apiKey);
  const index = buildLiveOptionsIndex(tables);

  if (!fs.existsSync(REPORTS)) fs.mkdirSync(REPORTS, { recursive: true });

  const jsonPath = LIVE_OPTIONS_JSON;
  fs.writeFileSync(jsonPath, JSON.stringify(index, null, 2));

  const rows = [];
  for (const spec of OAS_AUDIT_FIELD_SPECS) {
    const key = `${spec.tableKey}::${spec.fieldName}`;
    const entry = index.fields[key] || {};
    const cmp = comparePlannedToLive(spec.plannedOptions || [], entry.liveOptions || []);
    rows.push({
      table: OAS_AUDIT_TABLES[spec.tableKey],
      tableKey: spec.tableKey,
      fieldName: spec.fieldName,
      fieldType: entry.fieldType || entry.status || "",
      status: entry.status || "unknown",
      plannedCount: (spec.plannedOptions || []).length,
      liveCount: (entry.liveOptions || []).length,
      matchStatus: cmp.matchStatus,
      plannedMissingFromLive: cmp.missing.join(" | "),
      liveExtraNotPlanned: cmp.extra.join(" | "),
      liveOptions: (entry.liveOptions || []).join(" | "),
    });
  }

  const csvPath = path.join(REPORTS, "operator-alignment-live-airtable-options.csv");
  const headers = Object.keys(rows[0] || {});
  const csvLines = [
    headers.join(","),
    ...rows.map((r) =>
      headers
        .map((h) => {
          const v = String(r[h] ?? "").replace(/"/g, '""');
          return v.includes(",") || v.includes('"') ? `"${v}"` : v;
        })
        .join(",")
    ),
  ];
  fs.writeFileSync(csvPath, csvLines.join("\n"));

  console.log("Exported", rows.length, "fields");
  console.log("JSON:", jsonPath);
  console.log("CSV:", csvPath);
  const partial = rows.filter((r) => r.matchStatus !== "Exact" && r.status === "ok");
  console.log("Non-exact matches:", partial.length);
  for (const r of partial.slice(0, 15)) {
    console.log(`  ${r.matchStatus}\t${r.fieldName}`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
