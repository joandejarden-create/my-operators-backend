/**
 * Step 1 — Inventory Scout demand overlay source tables (read-only).
 *
 * Usage: node scripts/inventory-scout-demand-overlay-sources.mjs
 *
 * Output:
 *   reports/scout-demand-overlay-source-inventory.json
 *   reports/scout-demand-overlay-source-inventory.csv
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { inspectOverlaySourceTables } from "../lib/scout/demand-overlays.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const JSON_OUT = join(REPORTS, "scout-demand-overlay-source-inventory.json");
const CSV_OUT = join(REPORTS, "scout-demand-overlay-source-inventory.csv");

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function toCsvRows(inventory) {
  const rows = [
    [
      "tableLabel",
      "tableName",
      "tableId",
      "fieldName",
      "fieldType",
      "fieldRole",
      "inRecommendedMapping",
    ].join(","),
  ];

  for (const table of inventory.tables || []) {
    const mappingValues = new Set(
      Object.values(table.recommendedMapping || {}).filter(
        (v) => typeof v === "string" && v
      )
    );
    for (const field of table.fields || []) {
      rows.push(
        [
          csvEscape(table.label),
          csvEscape(table.tableName),
          csvEscape(table.tableId),
          csvEscape(field.name),
          csvEscape(field.type),
          csvEscape(field.role),
          csvEscape(mappingValues.has(field.name) ? "yes" : "no"),
        ].join(",")
      );
    }
  }
  return rows.join("\n");
}

async function main() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) {
    throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");
  }

  console.log("=== Scout demand overlay source inventory (read-only) ===\n");
  console.log("Base:", baseId);

  const result = await inspectOverlaySourceTables({ apiKey, baseId });
  if (!result.ok) throw new Error(result.error);

  const payload = {
    generatedAt: new Date().toISOString(),
    baseId,
    tablesFound: result.tablesFound,
    tablesMissing: result.tablesMissing,
    allTableNames: result.allTableNames,
    tables: result.tables,
    usableCoordinateFields: result.tables.flatMap((t) =>
      (t.coordinateFields || []).map((f) => ({ table: t.label, field: f }))
    ),
    usableGeographyFields: result.tables.flatMap((t) =>
      (t.geographyFields || []).map((entry) => {
        const [role, field] = entry.split(":");
        return { table: t.label, role, field };
      })
    ),
    recommendedOverlayMappings: result.tables.map((t) => ({
      table: t.label,
      tableName: t.tableName,
      mapping: t.recommendedMapping,
      enabledInScoutPhase5A: t.recommendedMapping?.enabledInScoutPhase5A === true,
    })),
  };

  mkdirSync(REPORTS, { recursive: true });
  writeFileSync(JSON_OUT, JSON.stringify(payload, null, 2), "utf8");
  writeFileSync(CSV_OUT, toCsvRows(result), "utf8");

  console.log("\nTables found:", result.tablesFound.join(", ") || "(none)");
  console.log("Tables missing:", result.tablesMissing.join(", ") || "(none)");
  console.log("\nWrote:", JSON_OUT);
  console.log("Wrote:", CSV_OUT);

  for (const t of result.tables) {
    console.log(`\n${t.label} (${t.tableName})`);
    console.log("  coordinates:", t.coordinateFields.join(", ") || "none");
    console.log("  geography:", t.geographyFields.join(", ") || "none");
    console.log("  Scout 5A:", t.recommendedMapping?.enabledInScoutPhase5A ? "enabled" : "inventory only");
  }
}

main().catch((err) => {
  console.error("\nFAIL:", err.message);
  process.exit(1);
});
