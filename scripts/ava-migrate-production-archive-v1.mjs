/**
 * Migrate the six Phase 8 AVA audits into the production-durable archive + store.
 * Idempotent. Does not change founder approval or publish.
 *
 *   node scripts/ava-migrate-production-archive-v1.mjs
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  AVA_PRODUCTION_ARCHIVE_INDEX,
  AVA_PRODUCTION_ARCHIVE_ROOT,
  AVA_PRODUCTION_STORE_ROOT,
  archivePropertyDir,
} from "../lib/ai-visibility-audit-action-plan/certified-baseline-paths-v1.js";
import { loadAdpBaselineForAudit } from "../lib/ai-visibility-audit-action-plan/adp-baseline-loader-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const LEGACY_STORE = path.join(ROOT, "data/ai-visibility-audit-action-plan/store");

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

function copyFileIdempotent(src, dest) {
  ensureDir(path.dirname(dest));
  const buf = fs.readFileSync(src);
  if (fs.existsSync(dest)) {
    const existing = fs.readFileSync(dest);
    if (Buffer.compare(buf, existing) === 0) return { copied: false, bytes: buf.length };
  }
  fs.writeFileSync(dest, buf);
  return { copied: true, bytes: buf.length };
}

function copyDirRecursive(src, dest) {
  ensureDir(dest);
  let files = 0;
  let bytes = 0;
  for (const ent of fs.readdirSync(src, { withFileTypes: true })) {
    const from = path.join(src, ent.name);
    const to = path.join(dest, ent.name);
    if (ent.isDirectory()) {
      const nested = copyDirRecursive(from, to);
      files += nested.files;
      bytes += nested.bytes;
    } else {
      const r = copyFileIdempotent(from, to);
      files += 1;
      bytes += r.bytes;
    }
  }
  return { files, bytes };
}

function pickLatestPdf(propertyId) {
  const slug = propertyId.replace(/^adp_/, "").replace(/_/g, "-");
  const dir = path.join(
    ROOT,
    "reports/dealality-ai-visibility-audit-action-plan-pilot-v1",
    slug,
    "pdf-staged"
  );
  if (!fs.existsSync(dir)) return null;
  const pdfs = fs
    .readdirSync(dir)
    .filter((f) => f.endsWith(".pdf") && !/pre-/i.test(f) && !/phase[56]/i.test(f))
    .map((f) => ({ f, t: fs.statSync(path.join(dir, f)).mtimeMs }))
    .sort((a, b) => b.t - a.t);
  // Prefer *-final.pdf then NOVEMBER2026.pdf without phase suffix
  const final = pdfs.find((p) => /final\.pdf$/i.test(p.f));
  if (final) return path.join(dir, final.f);
  const plain = pdfs.find((p) => /NOVEMBER2026\.pdf$/i.test(p.f));
  if (plain) return path.join(dir, plain.f);
  return pdfs[0] ? path.join(dir, pdfs[0].f) : null;
}

function migrateOne(propertyId) {
  const legacyDir = path.join(LEGACY_STORE, propertyId);
  if (!fs.existsSync(legacyDir)) {
    return { propertyId, ok: false, error: "LEGACY_STORE_MISSING" };
  }
  const metaPath = path.join(legacyDir, "AUDIT_META.json");
  const meta = JSON.parse(fs.readFileSync(metaPath, "utf8"));
  const baseline = loadAdpBaselineForAudit(propertyId, {
    periodId: meta.adpBaselinePeriodId || undefined,
  });

  const prodDir = path.join(AVA_PRODUCTION_STORE_ROOT, propertyId);
  const storeCopy = copyDirRecursive(legacyDir, prodDir);

  const pdfSrc = pickLatestPdf(propertyId);
  if (!pdfSrc) return { propertyId, ok: false, error: "PDF_MISSING" };
  const pdfSha = sha256File(pdfSrc);
  const archDir = archivePropertyDir(propertyId);
  const pdfDest = path.join(archDir, "current.pdf");
  const pdfCopy = copyFileIdempotent(pdfSrc, pdfDest);
  // Also keep named copy for clarity
  const namedDest = path.join(archDir, path.basename(pdfSrc));
  copyFileIdempotent(pdfSrc, namedDest);

  const pdfRelativePath = path
    .relative(ROOT, pdfDest)
    .replace(/\\/g, "/");

  // Patch production meta PDF pointer + baseline pin (do not change approval fields)
  const prodMetaPath = path.join(prodDir, "AUDIT_META.json");
  const prodMeta = JSON.parse(fs.readFileSync(prodMetaPath, "utf8"));
  const preservedStatus = prodMeta.status;
  const preservedCustomer = prodMeta.customerReadinessStatus || "NOT_CUSTOMER_READY";
  const preservedPub = prodMeta.publicationAuthorizationStatus || "NOT_AUTHORIZED";
  const preservedFounder = prodMeta.founderApprovedForCustomer === true;

  prodMeta.pdfRelativePath = pdfRelativePath;
  prodMeta.pdfStatus = "STAGED";
  prodMeta.pdfSha256 = pdfSha;
  prodMeta.adpBaselinePeriodId =
    prodMeta.adpBaselinePeriodId || baseline.baseline?.periodId || null;
  prodMeta.adpBaselineSha256 = baseline.baseline?.baselineSha256 || null;
  prodMeta.adpBaselinePath = baseline.baseline?.leanBaselinePath || null;
  prodMeta.productionArchiveMigratedAt = new Date().toISOString();
  prodMeta.customerReadinessStatus = preservedCustomer;
  prodMeta.publicationAuthorizationStatus = preservedPub;
  prodMeta.founderApprovedForCustomer = preservedFounder;
  prodMeta.status = preservedStatus;
  if (Array.isArray(prodMeta.versions)) {
    prodMeta.versions = prodMeta.versions.map((v) => ({
      ...v,
      pdfRelativePath: v.pdfRelativePath || pdfRelativePath,
    }));
  }
  fs.writeFileSync(prodMetaPath, `${JSON.stringify(prodMeta, null, 2)}\n`);

  // Patch latest version record pdf path
  if (prodMeta.latestVersionId) {
    const vFile = path.join(prodDir, "versions", `${prodMeta.latestVersionId}.json`);
    if (fs.existsSync(vFile)) {
      const ver = JSON.parse(fs.readFileSync(vFile, "utf8"));
      ver.pdfRelativePath = pdfRelativePath;
      if (ver.payload) {
        ver.payload.reporting = ver.payload.reporting || {};
        ver.payload.reporting.adpPeriodId =
          ver.payload.reporting.adpPeriodId || prodMeta.adpBaselinePeriodId;
        ver.payload.reporting.adpBaselineSha256 = prodMeta.adpBaselineSha256;
      }
      fs.writeFileSync(vFile, `${JSON.stringify(ver, null, 2)}\n`);
    }
  }

  const checksums = {
    propertyId,
    pdfSha256: pdfSha,
    pdfRelativePath,
    storeFiles: storeCopy.files,
    storeBytes: storeCopy.bytes,
    metaStatus: prodMeta.status,
    evidenceReviewStatus: prodMeta.evidenceReviewStatus,
    customerReadinessStatus: prodMeta.customerReadinessStatus,
    publicationAuthorizationStatus: prodMeta.publicationAuthorizationStatus,
    founderApprovedForCustomer: prodMeta.founderApprovedForCustomer === true,
    actionCount: prodMeta.acceptedRecommendationCount || 0,
    versionCount: Array.isArray(prodMeta.versions) ? prodMeta.versions.length : 0,
    adpBaselinePeriodId: prodMeta.adpBaselinePeriodId,
    adpBaselineSha256: prodMeta.adpBaselineSha256,
    migratedAt: prodMeta.productionArchiveMigratedAt,
  };
  fs.writeFileSync(
    path.join(archDir, "checksums.json"),
    `${JSON.stringify(checksums, null, 2)}\n`
  );

  return {
    propertyId,
    ok: true,
    name: prodMeta.name,
    status: prodMeta.status,
    pdfSha256: pdfSha,
    pdfCopied: pdfCopy.copied,
    storeFiles: storeCopy.files,
    storeBytes: storeCopy.bytes,
    checksums,
  };
}

function main() {
  ensureDir(AVA_PRODUCTION_STORE_ROOT);
  ensureDir(AVA_PRODUCTION_ARCHIVE_ROOT);
  const results = COHORT.map(migrateOne);
  const failed = results.filter((r) => !r.ok);
  const index = {
    contractVersion: "AVA_PRODUCTION_ARCHIVE_INDEX_V1",
    migratedAt: new Date().toISOString(),
    count: results.filter((r) => r.ok).length,
    hotels: results,
  };
  fs.writeFileSync(AVA_PRODUCTION_ARCHIVE_INDEX, `${JSON.stringify(index, null, 2)}\n`);

  // Parity: compare legacy vs production version counts and statuses
  const parity = COHORT.map((propertyId) => {
    const legacyMeta = JSON.parse(
      fs.readFileSync(path.join(LEGACY_STORE, propertyId, "AUDIT_META.json"), "utf8")
    );
    const prodMeta = JSON.parse(
      fs.readFileSync(
        path.join(AVA_PRODUCTION_STORE_ROOT, propertyId, "AUDIT_META.json"),
        "utf8"
      )
    );
    const legacyVersions = fs.readdirSync(
      path.join(LEGACY_STORE, propertyId, "versions")
    ).length;
    const prodVersions = fs.readdirSync(
      path.join(AVA_PRODUCTION_STORE_ROOT, propertyId, "versions")
    ).length;
    return {
      propertyId,
      statusMatch: legacyMeta.status === prodMeta.status,
      versionsMatch: legacyVersions === prodVersions,
      legacyVersions,
      prodVersions,
      founderUnchanged:
        (legacyMeta.founderApprovedForCustomer === true) ===
        (prodMeta.founderApprovedForCustomer === true),
      customerStillBlocked: prodMeta.customerReadinessStatus === "NOT_CUSTOMER_READY",
      publicationStillBlocked:
        prodMeta.publicationAuthorizationStatus === "NOT_AUTHORIZED",
      pdfExists: fs.existsSync(path.join(archivePropertyDir(propertyId), "current.pdf")),
    };
  });

  const out = {
    ok: failed.length === 0 && parity.every((p) => p.statusMatch && p.versionsMatch && p.pdfExists),
    results,
    parity,
    archiveIndex: path.relative(ROOT, AVA_PRODUCTION_ARCHIVE_INDEX).replace(/\\/g, "/"),
    productionStore: path.relative(ROOT, AVA_PRODUCTION_STORE_ROOT).replace(/\\/g, "/"),
  };
  fs.writeFileSync(
    path.join(
      ROOT,
      "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_MIGRATION_PARITY.json"
    ),
    `${JSON.stringify(out, null, 2)}\n`
  );
  console.log(JSON.stringify({ ok: out.ok, failed: failed.length, parity }, null, 2));
  if (!out.ok) process.exit(1);
}

main();
