/**
 * Stage the 38 certified V4 PDFs into Admin draft storage (exact bytes).
 * Never overwrites published.pdf. Never rotates share tokens.
 *
 *   AVA_STORAGE_MODE=local node scripts/ava-v4-stage-certified-drafts-v1.mjs --dry-run
 *   AVA_STORAGE_MODE=local node scripts/ava-v4-stage-certified-drafts-v1.mjs
 *
 * Production durable transfer (founder auth only):
 *   AVA_STORAGE_MODE=required AVA_STORAGE_ROOT=... NODE_ENV=production \
 *     node scripts/ava-v4-stage-certified-drafts-v1.mjs --authorize-production-draft-transfer
 *
 * Optional backup root for prior drafts (apply only):
 *   AVA_DRAFT_BACKUP_ROOT=<dir>
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import {
  ensureAvaDurableStorage,
  resolveAvaStorageRoot,
} from "../lib/ai-visibility-audit-action-plan/resolve-ava-storage-root-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const DEFAULT_MANIFEST = path.join(REPORT, "AVA_38_HOTEL_FINAL_MANIFEST.json");
const dryRun = process.argv.includes("--dry-run");
const applyProd = process.argv.includes("--authorize-production-draft-transfer");

function arg(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function sha256File(abs) {
  return sha256(fs.readFileSync(abs));
}

if (!process.env.AVA_STORAGE_MODE) {
  console.error("AVA_STORAGE_MODE required (local|required)");
  process.exit(1);
}

if (process.env.NODE_ENV === "production" && !applyProd && !dryRun) {
  console.error(
    "Production draft transfer requires --authorize-production-draft-transfer (or --dry-run)"
  );
  process.exit(1);
}

const storage = ensureAvaDurableStorage();
if (!storage.ok) {
  console.error("AVA storage unavailable:", storage.message || storage.error);
  process.exit(1);
}
const storageRoot = resolveAvaStorageRoot();
const backupRoot =
  String(process.env.AVA_DRAFT_BACKUP_ROOT || "").trim() ||
  path.join(
    REPORT,
    `ava-draft-backup-dryrun-${new Date().toISOString().replace(/[:.]/g, "-")}`
  );

const manifestPath = path.resolve(arg("--manifest") || DEFAULT_MANIFEST);
if (!fs.existsSync(manifestPath)) {
  console.error("Manifest missing:", manifestPath);
  process.exit(1);
}
const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
const rows = [];

console.log(
  `${dryRun ? "DRY-RUN" : "APPLY"} draft transfer → ${storageRoot}` +
    ` (manifest=${path.basename(manifestPath)}, hotels=${(manifest.hotels || []).length})`
);

for (const h of manifest.hotels || []) {
  const abs = path.join(ROOT, h.pdfPath);
  if (!fs.existsSync(abs)) {
    rows.push({
      propertyId: h.propertyId,
      hotelName: h.hotelName,
      ok: false,
      error: "SOURCE_PDF_MISSING",
      sourceAbs: abs,
    });
    continue;
  }
  const buf = fs.readFileSync(abs);
  const hash = sha256(buf);
  if (hash !== h.pdfSha256) {
    rows.push({
      propertyId: h.propertyId,
      hotelName: h.hotelName,
      ok: false,
      error: "SHA_MISMATCH_BEFORE_STAGE",
      expected: h.pdfSha256,
      actual: hash,
    });
    continue;
  }

  const draftAbs = path.join(storageRoot, "pdfs", h.propertyId, "draft.pdf");
  const publishedAbs = path.join(
    storageRoot,
    "pdfs",
    h.propertyId,
    "published.pdf"
  );
  const priorMeta = getAuditMeta(h.propertyId) || {};
  const previousDraftSha =
    priorMeta.draftPdfSha256 ||
    (fs.existsSync(draftAbs) ? sha256File(draftAbs) : null);
  const previousDraftVersionId = priorMeta.draftVersionId || null;
  let publishedShaBefore = null;
  if (fs.existsSync(publishedAbs)) {
    publishedShaBefore = sha256File(publishedAbs);
  }

  const wouldTouchPublished = false; // stageAvaPdf writes draft only
  const backupPlan = {
    previousDraftSha,
    previousDraftVersionId,
    backupRel: previousDraftSha
      ? path.join(h.propertyId, `draft-before-${previousDraftSha.slice(0, 12)}.pdf`)
      : null,
    publishedProtected: true,
    shareTokensUntouched: true,
  };

  if (dryRun) {
    rows.push({
      propertyId: h.propertyId,
      hotelName: h.hotelName,
      ok: true,
      dryRun: true,
      sha256: hash,
      previousDraftSha,
      previousDraftVersionId,
      publishedShaBefore,
      wouldTouchPublished,
      publishedProtected: publishedShaBefore == null || publishedShaBefore !== hash
        ? true
        : publishedShaBefore !== hash, // new draft must not be written over published path
      backupPlan,
      rollback: previousDraftSha
        ? `Restore ${backupPlan.backupRel || "prior draft"} → draft.pdf; restore meta.draftPdfSha256=${previousDraftSha}`
        : "No prior draft to restore",
    });
    continue;
  }

  // Apply: backup prior draft bytes when present
  if (fs.existsSync(draftAbs) && previousDraftSha) {
    const dest = path.join(
      backupRoot,
      h.propertyId,
      `draft-before-${previousDraftSha.slice(0, 12)}.pdf`
    );
    fs.mkdirSync(path.dirname(dest), { recursive: true });
    fs.copyFileSync(draftAbs, dest);
    fs.writeFileSync(
      path.join(path.dirname(dest), "meta-before.json"),
      `${JSON.stringify(
        {
          propertyId: h.propertyId,
          previousDraftSha,
          previousDraftVersionId,
          publishedShaBefore,
          backedUpAt: new Date().toISOString(),
        },
        null,
        2
      )}\n`
    );
  }

  const versionId = `ava_v4_certified_${String(h.propertyId).replace(/^adp_/, "")}_${Date.now().toString(36)}`;
  const filename = path.basename(abs);
  const staged = stageAvaPdf({
    propertyId: h.propertyId,
    generatedAbsPath: abs,
    versionId,
    filename,
  });
  if (staged.sha256 !== hash) {
    rows.push({
      propertyId: h.propertyId,
      hotelName: h.hotelName,
      ok: false,
      error: "SHA_MISMATCH_AFTER_STAGE",
      expected: hash,
      actual: staged.sha256,
    });
    continue;
  }
  let publishedUntouched = true;
  if (publishedShaBefore) {
    publishedUntouched = sha256File(publishedAbs) === publishedShaBefore;
  }
  if (!publishedUntouched) {
    rows.push({
      propertyId: h.propertyId,
      hotelName: h.hotelName,
      ok: false,
      error: "PUBLISHED_PDF_MUTATED",
      publishedShaBefore,
      publishedShaAfter: sha256File(publishedAbs),
    });
    continue;
  }

  upsertAuditMeta(h.propertyId, {
    previousDraftPdfSha256: previousDraftSha || null,
    previousDraftVersionId: previousDraftVersionId || null,
    draftVersionId: versionId,
    draftPdfRelativePath: staged.draftPdfRelativePath || staged.pdfRelativePath,
    draftPdfSha256: staged.sha256,
    draftUpdatedAt: new Date().toISOString(),
    draftTemplate: "V4_CERTIFIED_PORTFOLIO",
    draftChannelSource: "CERTIFIED_FINAL_MANIFEST",
    draftManifestContract: manifest.contract || "AVA_38_HOTEL_FINAL_MANIFEST_V1",
    forceStatus: true,
  });
  const payloadPath = path.join(
    ROOT,
    "lib/ai-visibility-audit-action-plan/payloads",
    h.file
  );
  const payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
  saveAuditVersion(h.propertyId, versionId, payload, {
    status: "AUDIT_VALIDATED",
    metaStatus: "AUDIT_VALIDATED",
    forceStatus: true,
    kind: "REPORT_VERSION_V4_CERTIFIED",
    pdfRelativePath: staged.draftPdfRelativePath || staged.pdfRelativePath,
    channel: "draft",
    template: "V4_CERTIFIED_PORTFOLIO",
  });
  rows.push({
    propertyId: h.propertyId,
    hotelName: h.hotelName,
    ok: true,
    sha256: hash,
    versionId,
    previousDraftSha,
    publishedUntouched,
    publishedShaBefore,
    backupRoot: previousDraftSha ? backupRoot : null,
    shareTokensUntouched: true,
  });
  console.log(
    `STAGED ${h.hotelName} sha=${hash.slice(0, 12)} publishedUntouched=${publishedUntouched}`
  );
}

const summary = {
  contract: "AVA_V4_STAGE_CERTIFIED_DRAFTS_V1",
  generatedAt: new Date().toISOString(),
  dryRun,
  authorizeProductionDraftTransfer: applyProd,
  storageRoot,
  manifestPath: path.relative(ROOT, manifestPath).replace(/\\/g, "/"),
  certifiedRevision: manifest.certifiedRevision || null,
  backupRoot: dryRun ? null : backupRoot,
  total: rows.length,
  ok: rows.filter((r) => r.ok).length,
  failed: rows.filter((r) => !r.ok).length,
  publishedUntouched: rows.every(
    (r) => r.publishedUntouched !== false && r.wouldTouchPublished !== true
  ),
  shareTokensUntouched: true,
  rollbackProcedure: [
    "1. Stop before customer publish — drafts only.",
    "2. For each hotel, restore backed-up draft-before-<sha>.pdf over pdfs/<propertyId>/draft.pdf.",
    "3. Restore meta.previousDraftPdfSha256 / previousDraftVersionId into draftPdfSha256 / draftVersionId.",
    "4. Confirm published.pdf SHA unchanged vs publishedShaBefore.",
    "5. Do not rotate or rewrite share tokens.",
  ],
  rows,
};
fs.writeFileSync(
  path.join(REPORT, "AVA_V4_STAGE_CERTIFIED_DRAFTS_RESULTS.json"),
  `${JSON.stringify(summary, null, 2)}\n`
);
console.log(
  JSON.stringify(
    {
      dryRun,
      ok: summary.ok,
      failed: summary.failed,
      publishedUntouched: summary.publishedUntouched,
      shareTokensUntouched: summary.shareTokensUntouched,
      storageRoot,
    },
    null,
    2
  )
);
if (summary.failed) process.exit(2);
