#!/usr/bin/env node
/**
 * Import reviewed hotel-facing prospect packages into GDI stores.
 *
 * Usage:
 *   node scripts/gdi-import-reviewed-prospects.mjs --hotel recLuxvwwxID7U2B8 --dry-run
 *   node scripts/gdi-import-reviewed-prospects.mjs --hotel rec0eTEnCqlsGJJt9 --apply --data-root <path>
 *   node scripts/gdi-import-reviewed-prospects.mjs --both --apply --data-root <path>
 *   node scripts/gdi-import-reviewed-prospects.mjs --both --dry-run --adapter airtable
 *
 * Persistence follows GDI_OPPORTUNITIES_PERSISTENCE (or Airtable when configured).
 * Prefer an isolated --data-root + --adapter filesystem for local proof.
 * Production Airtable apply: omit --data-root, use --adapter airtable, then --apply.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  importReviewedProspectsForHotel,
  importReviewedProspectsBothHotels,
  loadReviewedProspectPackage,
} from "../lib/group-demand-intelligence/reviewed-prospect-import-v1.js";
import { REVIEWED_PROSPECT_HOTEL_IDS } from "../lib/group-demand-intelligence/reviewed-prospect-publication-v1.js";
import { clearGdiRepositoryReadCache } from "../lib/group-demand-intelligence/repository.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i < 0) return fallback;
  return process.argv[i + 1] || fallback;
}

function has(flag) {
  return process.argv.includes(flag);
}

const dryRun = !has("--apply");
const both = has("--both");
const hotelId = arg("--hotel");
const dataRoot = arg("--data-root");
const adapter = String(arg("--adapter", "") || "")
  .trim()
  .toLowerCase();
const packageDir =
  arg("--package-dir") ||
  path.join(ROOT, "reports/gdi-bethesda-process-replication/hotel-review");
const outDir =
  arg("--out-dir") ||
  path.join(ROOT, "reports/gdi-prospect-publication-implementation");
const backupDir =
  arg("--backup-dir") || path.join(outDir, "backups");

if (dataRoot) {
  process.env.GDI_DATA_ROOT = path.resolve(dataRoot);
}
if (adapter === "filesystem" || adapter === "fs") {
  process.env.GDI_OPPORTUNITIES_PERSISTENCE = "filesystem";
} else if (adapter === "airtable" || adapter === "at") {
  process.env.GDI_OPPORTUNITIES_PERSISTENCE = "airtable";
  // Production Airtable path must not inherit a leftover isolated data root.
  if (!dataRoot) delete process.env.GDI_DATA_ROOT;
} else if (!process.env.GDI_OPPORTUNITIES_PERSISTENCE && !dataRoot) {
  // Default CLI: if Airtable is configured and no isolated root, prefer airtable.
  process.env.GDI_OPPORTUNITIES_PERSISTENCE = "airtable";
}
process.env.GDI_REVIEWED_PROSPECT_PUBLICATION_V1 =
  process.env.GDI_REVIEWED_PROSPECT_PUBLICATION_V1 || "1";

clearGdiRepositoryReadCache();
for (const id of REVIEWED_PROSPECT_HOTEL_IDS) {
  invalidateGdiHotelReadCache(id);
}
fs.mkdirSync(outDir, { recursive: true });

const bundle = loadReviewedProspectPackage(packageDir);
console.log(
  JSON.stringify(
    {
      mode: dryRun ? "dry-run" : "apply",
      dataRoot: process.env.GDI_DATA_ROOT || "(default)",
      packageDir,
      packageVersion: bundle.packageVersion,
      registerHash: bundle.registerHash,
      packagesHash: bundle.packagesHash,
    },
    null,
    2
  )
);

const results = both
  ? await importReviewedProspectsBothHotels({
      packageDir,
      dryRun,
      backupDir: dryRun ? null : backupDir,
      hotelIds: REVIEWED_PROSPECT_HOTEL_IDS,
    })
  : [
      await importReviewedProspectsForHotel({
        hotelId: hotelId || REVIEWED_PROSPECT_HOTEL_IDS[0],
        packageDir,
        dryRun,
        backupDir: dryRun ? null : backupDir,
      }),
    ];

const stamp = new Date().toISOString().replace(/[:.]/g, "-");
const resultPath = path.join(
  outDir,
  `import-result-${dryRun ? "dryrun" : "apply"}-${stamp}.json`
);
fs.writeFileSync(resultPath, JSON.stringify({ results }, null, 2) + "\n");

// Flatten manifest CSV
const rows = results.flatMap((r) =>
  (r.manifest || []).map((m) => ({
    ...m,
    dryRun: r.dryRun,
    persistenceMode: r.persistenceMode,
    dataRoot: r.dataRoot,
    registerHash: r.registerHash,
    packagesHash: r.packagesHash,
  }))
);
const headers = [
  "hotelId",
  "cardId",
  "section",
  "account",
  "opportunityId",
  "action",
  "matchType",
  "hotelFacing",
  "enrichmentQueue",
  "eligibility",
  "contactGroup",
  "person",
  "contactMethod",
  "businessPhone",
  "phoneType",
  "dryRun",
  "persistenceMode",
  "dataRoot",
  "registerHash",
  "packagesHash",
];
const esc = (v) => {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};
const csv =
  headers.join(",") +
  "\n" +
  rows.map((r) => headers.map((h) => esc(r[h])).join(",")).join("\n") +
  "\n";
const manifestPath = path.join(
  outDir,
  dryRun ? "import-manifest-dryrun.csv" : "import-manifest.csv"
);
fs.writeFileSync(manifestPath, csv);

console.log(
  JSON.stringify(
    {
      resultPath,
      manifestPath,
      summaries: results.map((r) => ({
        hotelId: r.hotelId,
        dryRun: r.dryRun,
        persistenceMode: r.persistenceMode,
        savePersistence: r.savePersistence,
        counts: r.counts,
        conflicts: r.conflicts?.length || 0,
        quarantined: r.quarantined?.length || 0,
        profileInit: r.profileInit,
      })),
    },
    null,
    2
  )
);
