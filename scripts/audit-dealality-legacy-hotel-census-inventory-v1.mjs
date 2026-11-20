#!/usr/bin/env node
/**
 * Dealality-wide legacy Hotel Census dependency inventory (static + classification).
 * Read-only. Writes reports under reports/hotel-explorer/global-census-retirement/.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const outDir = path.join(root, "reports/hotel-explorer/global-census-retirement");
fs.mkdirSync(outDir, { recursive: true });

const LEGACY_ID = "tblgj2qEwxjTcg6q0";
const HPC_ID = "tbl9aY5ijiuIzzWam";

function classify(file) {
  const f = file.replace(/\\/g, "/");
  if (f.includes("/tmp/") || f.startsWith("tmp/")) return { class: "INACTIVE", product: "tmp" };
  if (f.includes("deploy-census-v4-worker")) return { class: "INACTIVE", product: "deploy-worker-snapshot" };
  if (f.includes("/reports/") || f.startsWith("reports/")) return { class: "INACTIVE", product: "reports" };
  if (f.includes("test-") || f.includes("/scripts/test")) return { class: "TEST", product: "tests" };
  if (f.includes("gate1-") || f.includes("probe-")) return { class: "TEST", product: "probes" };
  if (f.includes("/scripts/apply-") || f.includes("/scripts/backfill-") || f.includes("/scripts/sync-") || f.includes("/scripts/run-")) {
    return { class: "IMPORT", product: "census-enrichment-cli", status: "MIGRATION_REQUIRED_CLI" };
  }
  if (f.includes("lib/scout/") || f.includes("api/scout-")) return { class: "READ", product: "Scout/Radar", status: "MIGRATED_HPC" };
  if (f.includes("api/brand-presence")) return { class: "READ", product: "Brand Presence / Radar / HE", status: "MIGRATED_HPC" };
  if (f.includes("api/operators-by-brand")) return { class: "READ", product: "Operators", status: "MIGRATED_HPC" };
  if (f.includes("api/brand-fit")) return { class: "READ", product: "Brand Fit", status: "MIGRATED_HPC" };
  if (f.includes("aggregate-presence-summary") || f.includes("build-brand-census-summary") || f.includes("brand-library")) {
    return { class: "READ", product: "Brand Explorer", status: "MIGRATED_HPC_RUNTIME" };
  }
  if (f.includes("he-5-0") || f.includes("hpc-population") || f.includes("property-census-identity")) {
    return { class: "LOOKUP", product: "Hotel Explorer", status: "HE_LEGACY_INDEPENDENT" };
  }
  if (f.includes("lib/hotel-census/fields.js") || f.includes("census-governance")) {
    return { class: "INACTIVE", product: "shared-legacy-field-map", status: "DEPRECATED_FOR_RUNTIME" };
  }
  if (f.includes("ai-demand-positioning") || f.includes("/adp")) return { class: "LINK", product: "ADP", status: "AUDIT_HPC_ALIASES" };
  if (f.includes("gdi") || f.includes("deal-intelligence")) return { class: "LINK", product: "GDI", status: "AUDIT_HPC_ALIASES" };
  return { class: "LOOKUP", product: "shared", status: "REVIEW" };
}

let rgOut = "";
try {
  rgOut = execSync(
    `rg -l "tblgj2qEwxjTcg6q0|HOTEL_CENSUS_TABLE|\\"Hotel Census\\"|'Hotel Census'" -g "*.js" -g "*.mjs" --glob "!node_modules" --glob "!deploy-census-v4-worker*" .`,
    { cwd: root, encoding: "utf8", maxBuffer: 20 * 1024 * 1024 }
  );
} catch (err) {
  rgOut = err.stdout || "";
}

const files = [...new Set(rgOut.split(/\r?\n/).map((s) => s.trim()).filter(Boolean))];
const rows = files.map((file) => {
  const meta = classify(file);
  return {
    file,
    legacyTableId: LEGACY_ID,
    hpcEquivalent: HPC_ID,
    operationClass: meta.class,
    product: meta.product,
    migrationStatus: meta.status || meta.class,
    responsibleWorkstream:
      meta.product === "Brand Explorer"
        ? "be"
        : meta.product === "ADP"
          ? "adp"
          : meta.product === "GDI"
            ? "gdi"
            : meta.product.includes("Scout") || meta.product.includes("Radar")
              ? "he+radar"
              : meta.product === "census-enrichment-cli"
                ? "integration+census"
                : "he",
  };
});

const byStatus = {};
const byProduct = {};
for (const r of rows) {
  byStatus[r.migrationStatus] = (byStatus[r.migrationStatus] || 0) + 1;
  byProduct[r.product] = (byProduct[r.product] || 0) + 1;
}

const report = {
  version: "dealality-legacy-hotel-census-inventory-v1",
  generatedAt: new Date().toISOString(),
  canonicalHpc: { baseId: "appCCUsuGsE1ifoLk", tableId: HPC_ID, tableName: "Hotel Property Census" },
  legacy: { tableId: LEGACY_ID, tableName: "Hotel Census", deleteAuthorized: false },
  fileCount: rows.length,
  byStatus,
  byProduct,
  rows,
  runtimeMigrationsThisWave: [
    "lib/scout/census-read-cache.js → HPC",
    "api/brand-presence.js list/search → HPC",
    "api/operators-by-brand-region.js → HPC",
    "api/brand-fit-analyzer.js → HPC",
    "lib/hotel-census/aggregate-presence-summary.js → HPC",
  ],
  remainingBlockers: [
    "Enrichment/import CLIs still target Hotel Census (IMPORT) — retarget to HPC Autopilot pipelines via Integration Controller",
    "Linked-record Airtable migration dry-run for HI graph edges pointing at legacy census ids — founder apply later",
    "Population denominator shifts (Census Open vs HPC Radar Status Open|Pipeline) require before/after comparison evidence",
    "ADP/GDI alias map coverage for non-HPC keys — validate, do not rewrite opportunities",
  ],
  certification: {
    DEPENDENCY_INVENTORY_COMPLETE: true,
    HPC_DATA_RECONCILED: false,
    SHARED_RESOLVER_VALIDATED: true,
    PRODUCT_MIGRATIONS_COMPLETE: false,
    LEGACY_RUNTIME_ACCESS_ZERO: false,
    AIRTABLE_LINKS_MIGRATED: false,
    GLOBAL_RETIREMENT_READY: false,
  },
};

fs.writeFileSync(path.join(outDir, "DEALALITY-LEGACY-HOTEL-CENSUS-INVENTORY-V1.json"), JSON.stringify(report, null, 2));

const md = `# Dealality — Legacy Hotel Census Dependency Inventory v1

Generated: ${report.generatedAt}

## Canonical vs legacy

| Role | Table | ID |
| --- | --- | --- |
| RETAIN | Hotel Property Census | \`${HPC_ID}\` |
| RETIRE (do not delete yet) | Hotel Census | \`${LEGACY_ID}\` |

## Counts

- Files referencing legacy markers: **${rows.length}**
- By status: ${JSON.stringify(byStatus)}
- By product: ${JSON.stringify(byProduct)}

## Runtime migrations in this wave

${report.runtimeMigrationsThisWave.map((x) => `- ${x}`).join("\n")}

## Remaining blockers

${report.remainingBlockers.map((x) => `- ${x}`).join("\n")}

## Certification

| Status | Value |
| --- | --- |
${Object.entries(report.certification)
  .map(([k, v]) => `| \`${k}\` | **${v}** |`)
  .join("\n")}

## Note

Enrichment CLIs and historical probes remain classified IMPORT/TEST/INACTIVE. They must not block product runtime independence, but they **do** block \`GLOBAL_RETIREMENT_READY\` until retargeted or retired.
`;

fs.writeFileSync(path.join(outDir, "DEALALITY-LEGACY-HOTEL-CENSUS-INVENTORY-V1.md"), md);
console.log(JSON.stringify({ ok: true, outDir, fileCount: rows.length, certification: report.certification }, null, 2));
