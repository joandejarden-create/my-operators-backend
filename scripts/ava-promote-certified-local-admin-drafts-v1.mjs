#!/usr/bin/env node
/**
 * Promote certified local V4 portfolio PDFs into the Admin Draft channel.
 *
 * Certified artifact → local Admin Draft → Preview/Download
 *
 *   AVA_STORAGE_MODE=local \
 *   AVA_STORAGE_ROOT=<admin-data/ava-data> \
 *   node scripts/ava-promote-certified-local-admin-drafts-v1.mjs --all-38
 *
 * Does NOT regenerate PDFs, publish, deploy, or modify published.pdf.
 */
import "dotenv/config";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
  loadAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import {
  ensureAvaDurableStorage,
  resolveAvaStorageRoot,
} from "../lib/ai-visibility-audit-action-plan/resolve-ava-storage-root-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const DEFAULT_MANIFEST = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_38_HOTEL_V4_LOCAL_REGENERATION_MANIFEST.json"
);
const DEFAULT_STAGE = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/portfolio-v4-local-staging"
);

function sha256File(abs) {
  return crypto.createHash("sha256").update(fs.readFileSync(abs)).digest("hex");
}

function arg(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

function resolveSourcePdf(hotel, stageRoot) {
  if (hotel.pdfPath) {
    const fromManifest = path.isAbsolute(hotel.pdfPath)
      ? hotel.pdfPath
      : path.join(ROOT, hotel.pdfPath);
    if (fs.existsSync(fromManifest)) return fromManifest;
  }
  const slug = String(hotel.file || "")
    .replace(/-v1\.json$/i, "")
    .trim();
  const dir = path.join(stageRoot, slug);
  if (!fs.existsSync(dir)) return null;
  const pdf = fs.readdirSync(dir).find((f) => f.toLowerCase().endsWith(".pdf"));
  return pdf ? path.join(dir, pdf) : null;
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  const storage = ensureAvaDurableStorage();
  if (!storage.ok || !storage.writable) {
    console.error("AVA storage unavailable:", storage.message || storage.error);
    process.exit(1);
  }

  const manifestPath = path.resolve(arg("--manifest") || DEFAULT_MANIFEST);
  const stageRoot = path.resolve(arg("--stage-root") || DEFAULT_STAGE);
  const all38 = process.argv.includes("--all-38");
  const hotelsArg = arg("--hotels");
  const dryRun = process.argv.includes("--dry-run");

  if (!fs.existsSync(manifestPath)) {
    console.error("Manifest missing:", manifestPath);
    process.exit(1);
  }
  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  let hotels = Array.isArray(manifest.hotels) ? [...manifest.hotels] : [];
  if (!all38) {
    if (!hotelsArg) {
      console.error("Pass --all-38 or --hotels <propertyId,propertyId,...>");
      process.exit(1);
    }
    const set = new Set(hotelsArg.split(",").map((s) => s.trim()));
    hotels = hotels.filter(
      (h) =>
        set.has(h.propertyId) ||
        set.has(h.file) ||
        set.has(String(h.hotelName || ""))
    );
  }

  const results = [];
  const storageRoot = resolveAvaStorageRoot();
  console.log(
    `Promote ${hotels.length} certified V4 PDFs → Admin Draft @ ${storageRoot}` +
      (dryRun ? " (dry-run)" : "")
  );

  for (const hotel of hotels) {
    const propertyId = hotel.propertyId;
    const expectSha = String(hotel.pdfSha256 || "").toLowerCase();
    const sourceAbs = resolveSourcePdf(hotel, stageRoot);
    const publishedAbs = path.join(storageRoot, "pdfs", propertyId, "published.pdf");
    const publishedShaBefore = fs.existsSync(publishedAbs)
      ? sha256File(publishedAbs)
      : null;
    const priorMeta = getAuditMeta(propertyId) || {};
    const priorDraftSha =
      priorMeta.draftPdfSha256 ||
      (fs.existsSync(path.join(storageRoot, "pdfs", propertyId, "draft.pdf"))
        ? sha256File(path.join(storageRoot, "pdfs", propertyId, "draft.pdf"))
        : null);

    if (!sourceAbs || !fs.existsSync(sourceAbs)) {
      results.push({
        propertyId,
        hotelName: hotel.hotelName,
        ok: false,
        error: "SOURCE_PDF_MISSING",
        expectedSha: expectSha,
      });
      continue;
    }
    const sourceSha = sha256File(sourceAbs).toLowerCase();
    if (expectSha && sourceSha !== expectSha) {
      results.push({
        propertyId,
        hotelName: hotel.hotelName,
        ok: false,
        error: "SOURCE_SHA_MISMATCH_VS_MANIFEST",
        sourceSha,
        expectedSha: expectSha,
        sourceAbs,
      });
      continue;
    }

    const versionId =
      hotel.versionId ||
      `ava_v4_local_admin_${propertyId.replace(/^adp_/, "")}_${Date.now().toString(36)}`;
    const filename =
      path.basename(sourceAbs) ||
      `Dealality - AI Visibility Audit & Action Plan V4 - ${hotel.hotelName} - NOVEMBER2026.pdf`;

    if (dryRun) {
      results.push({
        propertyId,
        hotelName: hotel.hotelName,
        ok: true,
        dryRun: true,
        sourceSha,
        previousDraftPdfSha256: priorDraftSha,
        publishedShaBefore,
      });
      continue;
    }

    const staged = stageAvaPdf({
      propertyId,
      generatedAbsPath: sourceAbs,
      versionId,
      filename,
    });

    if (staged.sha256.toLowerCase() !== sourceSha) {
      results.push({
        propertyId,
        hotelName: hotel.hotelName,
        ok: false,
        error: "STAGED_SHA_MISMATCH",
        stagedSha: staged.sha256,
        sourceSha,
      });
      continue;
    }

    upsertAuditMeta(propertyId, {
      previousDraftPdfSha256: priorDraftSha || null,
      previousDraftVersionId: priorMeta.draftVersionId || null,
      draftVersionId: versionId,
      draftPdfRelativePath: staged.draftPdfRelativePath || staged.pdfRelativePath,
      draftPdfSha256: staged.sha256,
      draftUpdatedAt: new Date().toISOString(),
      draftTemplate: hotel.templateVersion || "AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V4",
      draftChannelSource: "CERTIFIED_LOCAL_V4_MANIFEST",
      draftManifestContract:
        manifest.contract || "AVA_38_HOTEL_V4_LOCAL_REGENERATION_MANIFEST_V1",
      pdfRelativePath: staged.draftPdfRelativePath || staged.pdfRelativePath,
      pdfSha256: staged.sha256,
      latestVersionId: versionId,
      pdfStatus: "STAGED",
      forceStatus: true,
    });

    if (!loadAuditVersion(propertyId, versionId)) {
      // Lightweight version stub — payload may live in source worktree only.
      saveAuditVersion(
        propertyId,
        versionId,
        {
          property: {
            propertyId,
            name: hotel.hotelName,
          },
          reporting: {
            reportMonthLabel: "November 2026",
            templateVersion: hotel.templateVersion || "AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V4",
          },
          _promotedFromCertifiedLocalManifest: true,
          _sourcePdfSha256: sourceSha,
        },
        {
          status: "AUDIT_VALIDATED",
          metaStatus: "AUDIT_VALIDATED",
          forceStatus: true,
          kind: "REPORT_VERSION_V4_LOCAL_ADMIN_PROMOTE",
          pdfRelativePath: staged.draftPdfRelativePath || staged.pdfRelativePath,
          channel: "draft",
          template: hotel.templateVersion || "AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V4",
        }
      );
    }

    const publishedShaAfter = fs.existsSync(publishedAbs)
      ? sha256File(publishedAbs)
      : null;
    const publishedUntouched =
      publishedShaBefore == null || publishedShaBefore === publishedShaAfter;

    results.push({
      propertyId,
      hotelName: hotel.hotelName,
      ok: true,
      draftPdfSha256: staged.sha256,
      previousDraftPdfSha256: priorDraftSha,
      draftVersionId: versionId,
      draftPdfRelativePath: staged.draftPdfRelativePath || staged.pdfRelativePath,
      publishedUntouched,
      publishedSha256: publishedShaAfter,
    });
    console.log(
      `  OK ${hotel.hotelName} draft=${staged.sha256.slice(0, 12)}… publishedUntouched=${publishedUntouched}`
    );
  }

  const okCount = results.filter((r) => r.ok).length;
  const fail = results.filter((r) => !r.ok);
  const outDir = path.join(
    ROOT,
    "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
  );
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(
    outDir,
    "AVA_V4_LOCAL_ADMIN_DRAFT_PROMOTE_RESULTS.json"
  );
  const payload = {
    contract: "AVA_V4_LOCAL_ADMIN_DRAFT_PROMOTE_V1",
    generatedAt: new Date().toISOString(),
    storageRoot,
    manifestPath,
    stageRoot,
    dryRun,
    total: results.length,
    okCount,
    failCount: fail.length,
    results,
    verdict:
      fail.length === 0 && okCount === results.length && results.length > 0
        ? "AVA_V4_LOCAL_ADMIN_DRAFT_PROMOTE_COMPLETE"
        : "AVA_V4_LOCAL_ADMIN_DRAFT_PROMOTE_PARTIAL_OR_FAILED",
  };
  fs.writeFileSync(outPath, `${JSON.stringify(payload, null, 2)}\n`);
  console.log(`\nWrote ${outPath}`);
  console.log(payload.verdict, `${okCount}/${results.length}`);
  if (fail.length) {
    console.error(JSON.stringify(fail, null, 2));
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
