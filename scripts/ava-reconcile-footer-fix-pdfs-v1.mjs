/**
 * Reconcile footer-fix PDFs into AVA archive + metadata.
 * - Points latest/current at corrected durable bytes
 * - Updates checksums without changing approval / publication statuses
 * - Preserves prior archive current.pdf as a historical version file
 *
 *   node scripts/ava-reconcile-footer-fix-pdfs-v1.mjs
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getAuditMeta, upsertAuditMeta } from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { AVA_PRODUCTION_ARCHIVE_ROOT } from "../lib/ai-visibility-audit-action-plan/certified-baseline-paths-v1.js";
import { resolveAvaPdfOutDir, readAvaPdfChecksums } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const HOTELS = [
  {
    propertyId: "adp_spice_island_beach_resort",
    expectSha: "330a1994992bbd99c98580d40c25cc932f96c602d2f796d56a97483ffae0a4cb",
    filename:
      "Dealality - AI Visibility Audit & Action Plan - Spice Island Beach Resort - NOVEMBER2026.pdf",
  },
  {
    propertyId: "adp_wyndham_san_diego_bayside",
    expectSha: "9fb00ca9d846184326de1a667c98959bf232c0fe5b0e4dfca996312c8da3c96d",
    filename:
      "Dealality - AI Visibility Audit & Action Plan - Wyndham San Diego Bayside - NOVEMBER2026.pdf",
  },
  {
    propertyId: "adp_waterstone_boca_raton",
    expectSha: "beae3cbcd836e61d0c3b3d655b0b1655e788cf52252915887ded3f50172cb33c",
    filename:
      "Dealality - AI Visibility Audit & Action Plan - Waterstone Resort & Marina - NOVEMBER2026.pdf",
  },
  {
    propertyId: "adp_bethesda_marriott",
    expectSha: "897bde170580e99462efdc30d454d4eb01d19ce2e2fd5ff43a3038977fa0f717",
    filename:
      "Dealality - AI Visibility Audit & Action Plan - Bethesda Marriott - NOVEMBER2026.pdf",
  },
  {
    propertyId: "adp_castillo_hotel_son_vida",
    expectSha: "337c3ce0ee04691039951ed8f080504bb8ff53ba6b6766be2e3a08a1e123e75c",
    filename:
      "Dealality - AI Visibility Audit & Action Plan - Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca - NOVEMBER2026.pdf",
  },
  {
    propertyId: "adp_sheraton_mallorca_arabella_golf",
    expectSha: "86ae3e587e55ef261bff7fa67833b34d39f2e0563f0ba07ccf1b7e83493dc124",
    filename:
      "Dealality - AI Visibility Audit & Action Plan - Sheraton Mallorca Arabella Golf Hotel - NOVEMBER2026.pdf",
  },
];

function sha256File(abs) {
  return crypto.createHash("sha256").update(fs.readFileSync(abs)).digest("hex");
}

function atomicCopy(src, dest) {
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  const tmp = `${dest}.${process.pid}.tmp`;
  fs.copyFileSync(src, tmp);
  fs.renameSync(tmp, dest);
}

function atomicWriteJson(filePath, obj) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const tmp = `${filePath}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(obj, null, 2)}\n`, "utf8");
  fs.renameSync(tmp, filePath);
}

function syncLegacyArchive({ propertyId, sourceAbs, versionId, filename, expectSha }) {
  const legacyDir = path.join(AVA_PRODUCTION_ARCHIVE_ROOT, propertyId);
  fs.mkdirSync(legacyDir, { recursive: true });
  const legacyCurrent = path.join(legacyDir, "current.pdf");
  const preserved = [];

  if (fs.existsSync(legacyCurrent)) {
    const priorSha = sha256File(legacyCurrent);
    if (priorSha !== expectSha) {
      const histName = `pre_footer_fix_${priorSha.slice(0, 12)}.pdf`;
      const histAbs = path.join(legacyDir, histName);
      if (!fs.existsSync(histAbs)) {
        atomicCopy(legacyCurrent, histAbs);
        preserved.push({
          file: histName,
          sha256: priorSha,
          note: "prior_legacy_current_preserved",
        });
      }
    }
  }

  atomicCopy(sourceAbs, legacyCurrent);
  atomicCopy(sourceAbs, path.join(legacyDir, `${versionId}.pdf`));
  if (filename) atomicCopy(sourceAbs, path.join(legacyDir, filename));

  const checksumsPath = path.join(legacyDir, "checksums.json");
  let checksums = { propertyId, versions: {} };
  if (fs.existsSync(checksumsPath)) {
    try {
      checksums = JSON.parse(fs.readFileSync(checksumsPath, "utf8"));
    } catch {
      checksums = { propertyId, versions: {} };
    }
  }
  checksums.versions = checksums.versions || {};
  for (const p of preserved) {
    checksums.versions[`historical_${p.sha256.slice(0, 12)}`] = {
      sha256: p.sha256,
      file: p.file,
      stagedAt: new Date().toISOString(),
      preserved: true,
    };
  }
  checksums.versions[versionId] = {
    sha256: expectSha,
    file: `${versionId}.pdf`,
    stagedAt: new Date().toISOString(),
    templateFix: "confidentiality-footer-only-v1",
  };
  checksums.currentSha256 = expectSha;
  checksums.pdfSha256 = expectSha;
  checksums.pdfRelativePath = path
    .relative(ROOT, legacyCurrent)
    .replace(/\\/g, "/");
  checksums.updatedAt = new Date().toISOString();
  atomicWriteJson(checksumsPath, checksums);

  return {
    legacyCurrent: path.relative(ROOT, legacyCurrent).replace(/\\/g, "/"),
    legacySha: sha256File(legacyCurrent),
    preserved,
  };
}

function refreshDurableChecksumTopFields({ propertyId, expectSha, versionId }) {
  const durableDir = resolveAvaPdfOutDir(propertyId);
  const checksumsPath = path.join(durableDir, "checksums.json");
  let checksums = readAvaPdfChecksums(propertyId) || { propertyId, versions: {} };
  checksums.versions = checksums.versions || {};
  if (!checksums.versions[versionId]) {
    checksums.versions[versionId] = {
      sha256: expectSha,
      file: `${versionId}.pdf`,
      stagedAt: new Date().toISOString(),
    };
  }
  checksums.currentSha256 = expectSha;
  checksums.pdfSha256 = expectSha;
  checksums.pdfRelativePath = `pdfs/${propertyId}/current.pdf`;
  checksums.updatedAt = new Date().toISOString();
  atomicWriteJson(checksumsPath, checksums);
  return checksums;
}

function main() {
  const results = [];
  for (const h of HOTELS) {
    const priorMeta = getAuditMeta(h.propertyId);
    if (!priorMeta) throw new Error(`MISSING_META ${h.propertyId}`);
    const preservedStatus = priorMeta.status;
    const versionId = priorMeta.latestVersionId;
    if (!versionId) throw new Error(`MISSING_LATEST_VERSION ${h.propertyId}`);

    const durableCurrent = path.join(
      resolveAvaPdfOutDir(h.propertyId),
      "current.pdf"
    );
    if (!fs.existsSync(durableCurrent)) {
      throw new Error(`MISSING_DURABLE_CURRENT ${h.propertyId}`);
    }
    const durableSha = sha256File(durableCurrent);
    if (durableSha !== h.expectSha) {
      throw new Error(
        `DURABLE_SHA_MISMATCH ${h.propertyId} got=${durableSha} expect=${h.expectSha}`
      );
    }

    const legacy = syncLegacyArchive({
      propertyId: h.propertyId,
      sourceAbs: durableCurrent,
      versionId,
      filename: h.filename,
      expectSha: h.expectSha,
    });
    const durableChecksums = refreshDurableChecksumTopFields({
      propertyId: h.propertyId,
      expectSha: h.expectSha,
      versionId,
    });

    // Refresh meta checksum pointers only — forceStatus preserves lifecycle.
    const nextMeta = upsertAuditMeta(h.propertyId, {
      pdfRelativePath: `pdfs/${h.propertyId}/current.pdf`,
      pdfSha256: h.expectSha,
      pdfStatus: "STAGED",
      forceStatus: true,
      founderApprovedForCustomer: priorMeta.founderApprovedForCustomer === true,
      founderApprovedForCustomerAt: priorMeta.founderApprovedForCustomerAt || null,
      customerReadinessStatus:
        priorMeta.customerReadinessStatus || "NOT_CUSTOMER_READY",
      publicationAuthorizationStatus:
        priorMeta.publicationAuthorizationStatus || "NOT_AUTHORIZED",
      evidenceReviewStatus: priorMeta.evidenceReviewStatus,
    });

    if (nextMeta.status !== preservedStatus) {
      throw new Error(
        `STATUS_CHANGED ${h.propertyId} ${preservedStatus} -> ${nextMeta.status}`
      );
    }

    results.push({
      propertyId: h.propertyId,
      status: nextMeta.status,
      latestVersionId: versionId,
      pdfRelativePath: nextMeta.pdfRelativePath,
      pdfSha256: nextMeta.pdfSha256,
      durableSha,
      legacySha: legacy.legacySha,
      legacyCurrent: legacy.legacyCurrent,
      preservedHistorical: legacy.preserved,
      durableChecksumCurrent: durableChecksums.currentSha256,
      founderApprovedForCustomer: nextMeta.founderApprovedForCustomer === true,
      publicationAuthorizationStatus: nextMeta.publicationAuthorizationStatus,
      customerReadinessStatus: nextMeta.customerReadinessStatus,
    });
  }

  const summary = {
    ok: results.every(
      (r) =>
        r.durableSha === r.pdfSha256 &&
        r.legacySha === r.pdfSha256 &&
        r.publicationAuthorizationStatus === "NOT_AUTHORIZED"
    ),
    contract: "AVA_PDF_FOOTER_FIX_ARCHIVE_RECONCILE_V1",
    generatedAt: new Date().toISOString(),
    publicationAuthorized: false,
    note: "Latest current.pdf + checksums point at confidentiality-footer-only PDFs; statuses unchanged; historical archive bytes preserved.",
    results,
  };
  const outPath = path.join(
    ROOT,
    "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_PDF_FOOTER_FIX_ARCHIVE_RECONCILE.json"
  );
  fs.writeFileSync(outPath, `${JSON.stringify(summary, null, 2)}\n`);
  console.log(JSON.stringify({ ok: summary.ok, count: results.length, outPath }, null, 2));
  if (!summary.ok) process.exit(1);
}

main();
