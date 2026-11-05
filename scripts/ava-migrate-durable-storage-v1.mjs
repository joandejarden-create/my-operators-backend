/**
 * Idempotent migration of the six AVA seed audits into AVA_STORAGE_ROOT.
 *
 * Never overwrites a newer durable runtime record with older packaged seed.
 *
 *   AVA_STORAGE_ROOT=... AVA_STORAGE_MODE=local node scripts/ava-migrate-durable-storage-v1.mjs
 *   node scripts/ava-migrate-durable-storage-v1.mjs --dry-run
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  AVA_PRODUCTION_ARCHIVE_ROOT,
  AVA_PRODUCTION_STORE_ROOT,
} from "../lib/ai-visibility-audit-action-plan/certified-baseline-paths-v1.js";
import {
  assertAvaDurableWritable,
  ensureAvaDurableStorage,
  resolveAvaStorageMode,
  AVA_STORAGE_MODE,
  RECOMMENDED_AVA_VOLUME_MOUNT,
} from "../lib/ai-visibility-audit-action-plan/resolve-ava-storage-root-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const DRY = process.argv.includes("--dry-run");

const COHORT = [
  "adp_spice_island_beach_resort",
  "adp_wyndham_san_diego_bayside",
  "adp_waterstone_boca_raton",
  "adp_bethesda_marriott",
  "adp_castillo_hotel_son_vida",
  "adp_sheraton_mallorca_arabella_golf",
];

function sha256File(abs) {
  return crypto.createHash("sha256").update(fs.readFileSync(abs)).digest("hex");
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function mtimeMs(p) {
  try {
    return fs.statSync(p).mtimeMs;
  } catch {
    return 0;
  }
}

function copyIfNewerOrMissing(src, dest) {
  if (!fs.existsSync(src)) return { action: "skip_missing_src" };
  ensureDir(path.dirname(dest));
  if (fs.existsSync(dest)) {
    const srcM = mtimeMs(src);
    const destM = mtimeMs(dest);
    const srcSha = sha256File(src);
    const destSha = sha256File(dest);
    if (srcSha === destSha) return { action: "unchanged", sha256: destSha };
    // Do not overwrite newer durable with older seed
    if (destM > srcM) {
      return {
        action: "kept_newer_runtime",
        srcSha,
        destSha,
        srcM,
        destM,
      };
    }
  }
  if (DRY) return { action: "would_copy", bytes: fs.statSync(src).size };
  fs.copyFileSync(src, dest);
  return { action: "copied", sha256: sha256File(dest) };
}

function copyDirPreferNewer(srcDir, destDir) {
  if (!fs.existsSync(srcDir)) return { files: 0, results: [] };
  ensureDir(destDir);
  const results = [];
  let files = 0;
  for (const ent of fs.readdirSync(srcDir, { withFileTypes: true })) {
    const from = path.join(srcDir, ent.name);
    const to = path.join(destDir, ent.name);
    if (ent.isDirectory()) {
      const nested = copyDirPreferNewer(from, to);
      files += nested.files;
      results.push(...nested.results);
    } else {
      const r = copyIfNewerOrMissing(from, to);
      files += 1;
      results.push({ file: ent.name, ...r });
    }
  }
  return { files, results };
}

function migrateProperty(propertyId, layout) {
  const seedStore = path.join(AVA_PRODUCTION_STORE_ROOT, propertyId);
  const seedPdf = path.join(AVA_PRODUCTION_ARCHIVE_ROOT, propertyId);
  const destAudit = path.join(layout.audits, propertyId);
  const destPdf = path.join(layout.pdfs, propertyId);

  const store = copyDirPreferNewer(seedStore, destAudit);

  // PDF: prefer archive current.pdf
  const pdfResults = [];
  const seedCurrent = path.join(seedPdf, "current.pdf");
  const destCurrent = path.join(destPdf, "current.pdf");
  if (fs.existsSync(seedCurrent)) {
    pdfResults.push({
      file: "current.pdf",
      ...copyIfNewerOrMissing(seedCurrent, destCurrent),
    });
  }
  if (fs.existsSync(seedPdf)) {
    for (const ent of fs.readdirSync(seedPdf, { withFileTypes: true })) {
      if (!ent.isFile()) continue;
      if (ent.name === "current.pdf") continue;
      pdfResults.push({
        file: ent.name,
        ...copyIfNewerOrMissing(
          path.join(seedPdf, ent.name),
          path.join(destPdf, ent.name)
        ),
      });
    }
  }

  // Rewrite meta pdfRelativePath to durable layout when migrating
  const metaPath = path.join(destAudit, "AUDIT_META.json");
  let metaPatch = null;
  if (fs.existsSync(metaPath) && fs.existsSync(destCurrent) && !DRY) {
    const meta = JSON.parse(fs.readFileSync(metaPath, "utf8"));
    const sha = sha256File(destCurrent);
    const next = {
      ...meta,
      pdfRelativePath: `pdfs/${propertyId}/current.pdf`,
      pdfSha256: sha,
      durableMigratedAt: new Date().toISOString(),
      durableStorageRootHint: RECOMMENDED_AVA_VOLUME_MOUNT,
    };
    // Annotate historical decision-induced versions (do not delete)
    if (Array.isArray(next.versions)) {
      next.versions = next.versions.map((v) => {
        const id = String(v.versionId || "");
        if (id.includes("_decision") && !v.kind) {
          return {
            ...v,
            kind: "DECISION_INDUCED_VERSION_LEGACY",
            note: "Preserved historical decision snapshot; new decisions use events only.",
          };
        }
        if (!v.kind) return { ...v, kind: "REPORT_VERSION" };
        return v;
      });
    }
    fs.writeFileSync(metaPath, `${JSON.stringify(next, null, 2)}\n`);
    metaPatch = { pdfRelativePath: next.pdfRelativePath, pdfSha256: sha };
  }

  return {
    propertyId,
    storeFiles: store.files,
    storeSample: store.results.slice(0, 5),
    pdfResults,
    metaPatch,
  };
}

function main() {
  if (resolveAvaStorageMode() === AVA_STORAGE_MODE.OFF) {
    // Default this migration to local if mode unset
    process.env.AVA_STORAGE_MODE = "local";
    if (!process.env.AVA_STORAGE_ROOT) {
      process.env.AVA_STORAGE_ROOT = path.join(ROOT, "data/ava-data");
    }
  }

  const state = ensureAvaDurableStorage();
  if (!state.ok) {
    console.error(JSON.stringify({ ok: false, ...state }, null, 2));
    process.exit(1);
  }
  assertAvaDurableWritable();

  const results = COHORT.map((id) => migrateProperty(id, state.layout));
  const out = {
    ok: true,
    dryRun: DRY,
    mode: state.mode,
    root: state.root,
    layout: state.layout,
    cohort: COHORT,
    results,
    policy: {
      neverOverwriteNewerRuntime: true,
      seedBaselinesRemainInArtifact: true,
      recommendedMount: RECOMMENDED_AVA_VOLUME_MOUNT,
    },
  };
  const reportDir = path.join(
    ROOT,
    "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
  );
  ensureDir(reportDir);
  const reportPath = path.join(reportDir, "AVA_DURABLE_STORAGE_MIGRATION.json");
  fs.writeFileSync(reportPath, `${JSON.stringify(out, null, 2)}\n`);
  console.log(JSON.stringify({ ok: true, reportPath, root: state.root, dryRun: DRY }, null, 2));
}

main();
