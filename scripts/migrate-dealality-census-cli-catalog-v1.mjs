#!/usr/bin/env node
/**
 * Build CLI migration catalog from Phase-1 inventory + disposition classifier.
 * No Airtable writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import inv from "../reports/hotel-explorer/global-census-retirement/DEALALITY-LEGACY-HOTEL-CENSUS-INVENTORY-V1.json" with { type: "json" };
import {
  classifyCensusCliPath,
  CLI_DISPOSITION,
  DEALALITY_CENSUS_CLI_MIGRATION_V1,
  HPC_CLI_TABLE_ID,
  HPC_CLI_TABLE_NAME,
} from "../lib/hotel-census/dealality-census-cli-migration-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const outDir = path.join(root, "reports/hotel-explorer/global-census-retirement");
fs.mkdirSync(outDir, { recursive: true });

const cliRows = (inv.rows || []).filter((r) => r.migrationStatus === "MIGRATION_REQUIRED_CLI");

const catalog = cliRows.map((r) => {
  const file = String(r.file || "").replace(/^\.\\/, "").replace(/\\/g, "/");
  const disposition = classifyCensusCliPath(file);
  return {
    file,
    operationClass: r.operationClass || "IMPORT",
    product: r.product || "census-enrichment-cli",
    priorMigrationStatus: r.migrationStatus,
    disposition,
    targetTable: HPC_CLI_TABLE_NAME,
    targetTableId: HPC_CLI_TABLE_ID,
    legacyWritesBlocked: true,
    sharedPath: "lib/hotel-census/fields.js → Hotel Property Census + HPC field names",
    reuse: disposition.includes("AUTOPILOT")
      ? "npm run census:autopilot (HPC)"
      : disposition.includes("UPSERT") || disposition.includes("NULL_FILL")
        ? "hpc-shared-upsert-v1 / applyHpcNullFill"
        : "disabled until retarget complete",
  };
});

const byDisposition = {};
for (const row of catalog) {
  byDisposition[row.disposition] = (byDisposition[row.disposition] || 0) + 1;
}

const report = {
  contract: DEALALITY_CENSUS_CLI_MIGRATION_V1,
  generatedAt: new Date().toISOString(),
  applyAuthorized: false,
  note:
    "fields.js HOTEL_CENSUS_TABLE + CENSUS_FIELDS now resolve to HPC. platform-base blocks legacy writes. Individual brand CLIs still need payload QA against HPC schema before live --apply.",
  counts: {
    total: catalog.length,
    byDisposition,
  },
  dispositions: CLI_DISPOSITION,
  catalog,
};

fs.writeFileSync(
  path.join(outDir, "DEALALITY-CENSUS-CLI-MIGRATION-CATALOG-V1.json"),
  JSON.stringify(report, null, 2)
);

const md = [];
md.push("# Dealality Census CLI Migration Catalog v1");
md.push("");
md.push(`Generated: ${report.generatedAt}`);
md.push("");
md.push(`Active CLI table: **${HPC_CLI_TABLE_NAME}** (\`${HPC_CLI_TABLE_ID}\`)`);
md.push("");
md.push("## Counts by disposition");
md.push("");
md.push("| Disposition | Count |");
md.push("| --- | ---: |");
for (const [k, v] of Object.entries(byDisposition).sort((a, b) => b[1] - a[1])) {
  md.push(`| ${k} | ${v} |`);
}
md.push("");
md.push("## Shared retarget");
md.push("");
md.push("- `lib/hotel-census/fields.js` — `HOTEL_CENSUS_TABLE` → HPC; `CENSUS_FIELDS` → HPC names");
md.push("- `lib/hotel-census/platform-base.js` — legacy write Proxy block");
md.push("- `lib/hotel-census/dealality-census-cli-migration-v1.js` — field map + enforce helpers");
md.push("");
md.push(`Total CLIs catalogued: **${catalog.length}**`);
md.push("");
md.push("Live Airtable apply for brand enrichment batches remains founder-gated; dry-run is the default.");

fs.writeFileSync(path.join(outDir, "DEALALITY-CENSUS-CLI-MIGRATION-CATALOG-V1.md"), md.join("\n"));
console.log(JSON.stringify(report.counts, null, 2));
console.log("Wrote DEALALITY-CENSUS-CLI-MIGRATION-CATALOG-V1.*");
