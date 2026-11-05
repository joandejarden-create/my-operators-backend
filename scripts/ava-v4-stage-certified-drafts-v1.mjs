/**
 * Stage the 38 certified V4 PDFs into Admin draft storage (exact bytes).
 * Never overwrites published.pdf. Requires AVA_STORAGE_MODE=local|required.
 *
 *   AVA_STORAGE_MODE=local node scripts/ava-v4-stage-certified-drafts-v1.mjs
 *   AVA_STORAGE_MODE=local node scripts/ava-v4-stage-certified-drafts-v1.mjs --dry-run
 *
 * Production durable transfer uses the same script after deploy authorization.
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

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const MANIFEST = path.join(REPORT, "AVA_38_HOTEL_FINAL_MANIFEST.json");
const dryRun = process.argv.includes("--dry-run");
const applyProd = process.argv.includes("--authorize-production-draft-transfer");

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
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

const manifest = JSON.parse(fs.readFileSync(MANIFEST, "utf8"));
const rows = [];

for (const h of manifest.hotels || []) {
  const abs = path.join(ROOT, h.pdfPath);
  const buf = fs.readFileSync(abs);
  const hash = sha256(buf);
  if (hash !== h.pdfSha256) {
    rows.push({
      propertyId: h.propertyId,
      ok: false,
      error: "SHA_MISMATCH_BEFORE_STAGE",
      expected: h.pdfSha256,
      actual: hash,
    });
    continue;
  }
  const publishedAbs = path.join(
    ROOT,
    "data/ava-data/pdfs",
    h.propertyId,
    "published.pdf"
  );
  let publishedShaBefore = null;
  if (fs.existsSync(publishedAbs)) {
    publishedShaBefore = sha256(fs.readFileSync(publishedAbs));
  }

  if (dryRun) {
    rows.push({
      propertyId: h.propertyId,
      hotelName: h.hotelName,
      ok: true,
      dryRun: true,
      sha256: hash,
      publishedShaBefore,
    });
    continue;
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
      ok: false,
      error: "SHA_MISMATCH_AFTER_STAGE",
      expected: hash,
      actual: staged.sha256,
    });
    continue;
  }
  let publishedUntouched = true;
  if (publishedShaBefore) {
    publishedUntouched =
      sha256(fs.readFileSync(publishedAbs)) === publishedShaBefore;
  }
  upsertAuditMeta(h.propertyId, {
    draftVersionId: versionId,
    draftPdfRelativePath: staged.draftPdfRelativePath || staged.pdfRelativePath,
    draftPdfSha256: staged.sha256,
    draftUpdatedAt: new Date().toISOString(),
    draftTemplate: "V4_CERTIFIED_PORTFOLIO",
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
    publishedUntouched,
    publishedShaBefore,
  });
  console.log(
    `STAGED ${h.hotelName} sha=${hash.slice(0, 12)} publishedUntouched=${publishedUntouched}`
  );
}

const summary = {
  contract: "AVA_V4_STAGE_CERTIFIED_DRAFTS_V1",
  generatedAt: new Date().toISOString(),
  dryRun,
  total: rows.length,
  ok: rows.filter((r) => r.ok).length,
  failed: rows.filter((r) => !r.ok).length,
  publishedUntouched: rows.every(
    (r) => r.publishedUntouched !== false || r.dryRun
  ),
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
    },
    null,
    2
  )
);
if (summary.failed) process.exit(2);
