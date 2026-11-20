#!/usr/bin/env node
/**
 * Schema/code dependency scan for legacy Hotel Census *links* — does NOT read
 * legacy hotel record contents (founder correction 2026-11-20).
 *
 * Identifies code/schema references that still mention the legacy table id/name.
 * Reconstructing relationships requires independent HPC identifiers — never
 * by fetching Hotel Census rows.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { LEGACY_HOTEL_CENSUS_TABLE_ID, LEGACY_HOTEL_CENSUS_TABLE_NAME } from "../lib/hotel-intelligence/hpc-population/legacy-hotel-census-access-guard-v1.js";
import { HPC_CLI_TABLE_ID } from "../lib/hotel-census/dealality-census-cli-migration-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const outDir = path.join(root, "reports/hotel-explorer/global-census-retirement");
fs.mkdirSync(outDir, { recursive: true });

const SCAN_ROOTS = ["api", "lib", "scripts", "public/js"];
const SKIP = new Set(["node_modules", ".git", "tmp", "data"]);

function walk(dir, out = []) {
  let entries;
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return out;
  }
  for (const ent of entries) {
    if (SKIP.has(ent.name)) continue;
    const p = path.join(dir, ent.name);
    if (ent.isDirectory()) walk(p, out);
    else if (/\.(js|mjs|cjs|ts|json|md)$/i.test(ent.name)) out.push(p);
  }
  return out;
}

const hits = [];
for (const rootName of SCAN_ROOTS) {
  const abs = path.join(root, rootName);
  for (const file of walk(abs)) {
    const rel = path.relative(root, file).replace(/\\/g, "/");
    // Skip disabled recon scripts and deprecated reports (audit history)
    if (/reconcile-legacy-census|dealality-legacy-hpc-reconciliation|compare-legacy-hpc-population|prepare-legacy-to-hpc-mutation/i.test(rel)) {
      continue;
    }
    if (/global-census-retirement\//i.test(rel) && /RECONCILIATION|MUTATION-PACKAGE|POPULATION-COMPARE/i.test(rel)) {
      continue;
    }
    let text = "";
    try {
      text = fs.readFileSync(file, "utf8");
    } catch {
      continue;
    }
    if (
      text.includes(LEGACY_HOTEL_CENSUS_TABLE_ID) ||
      /Hotel Census/.test(text) ||
      /LEGACY_HOTEL_CENSUS/.test(text)
    ) {
      const role = /assertLegacyHotelCensusAccess|isLegacyHotelCensusTableRef|LEGACY_HOTEL_CENSUS_DATA_ACCESS_PROHIBITED|ACCESS_BLOCKED|PROHIBITED/.test(
        text
      )
        ? "GUARD_OR_BLOCK"
        : /deprecated|DISABLED|PROHIBITED LEGACY|do not (read|use|reconcile)/i.test(text)
          ? "DEPRECATED_DOCS"
          : "REVIEW_POSSIBLE_DEPENDENCY";
      hits.push({ file: rel, role });
    }
  }
}

const byRole = {};
for (const h of hits) byRole[h.role] = (byRole[h.role] || 0) + 1;

const report = {
  contract: "DEALALITY_LEGACY_LINK_DEPENDENCY_SCAN_V1",
  generatedAt: new Date().toISOString(),
  policy: "HPC_ONLY_NO_LEGACY_RECORD_READS",
  banner: "Does not fetch Hotel Census record contents",
  legacyTableId: LEGACY_HOTEL_CENSUS_TABLE_ID,
  legacyTableName: LEGACY_HOTEL_CENSUS_TABLE_NAME,
  hpcTableId: HPC_CLI_TABLE_ID,
  airtableLinksMigrated: false,
  legacyLinkDependenciesRemoved: false,
  note:
    "Reestablish relationships only via independent HPC identifiers / evidence. Never assume a legacy linked-record id is an HPC hotel.",
  counts: { filesScannedHits: hits.length, byRole },
  hits,
  proposedNextSteps: [
    "For REVIEW_POSSIBLE_DEPENDENCY files: remove runtime reads/writes or convert to GUARD_OR_BLOCK.",
    "Airtable linked fields still pointing at Hotel Census require separate founder-authorized schema/link rewrite dry-run — without reading legacy row contents.",
    "Do not import legacy relationship graphs into HPC.",
  ],
};

fs.writeFileSync(
  path.join(outDir, "LEGACY-CENSUS-LINK-DEPENDENCY-SCAN-V1.json"),
  JSON.stringify(report, null, 2)
);

const md = [];
md.push("# Legacy Census link dependency scan (no record reads)");
md.push("");
md.push(`Generated: ${report.generatedAt}`);
md.push("**Does not query Hotel Census record contents.**");
md.push("");
md.push(`Hits: **${hits.length}**`);
md.push("");
md.push("| Role | Count |");
md.push("| --- | ---: |");
for (const [k, v] of Object.entries(byRole).sort((a, b) => b[1] - a[1])) {
  md.push(`| ${k} | ${v} |`);
}
md.push("");
md.push("`AIRTABLE_LINKS_MIGRATED` / `LEGACY_LINK_DEPENDENCIES_REMOVED` remain **false** until founder-authorized reconstruction without legacy data.");

fs.writeFileSync(path.join(outDir, "LEGACY-CENSUS-LINK-DEPENDENCY-SCAN-V1.md"), md.join("\n"));
console.log(JSON.stringify(report.counts, null, 2));
console.log("Wrote LEGACY-CENSUS-LINK-DEPENDENCY-SCAN-V1.*");
