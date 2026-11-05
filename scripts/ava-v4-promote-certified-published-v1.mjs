/**
 * Promote certified draft PDFs to published channel — FAIL CLOSED.
 *
 * Requires ALL of:
 *   --authorize-customer-publication
 *   --hotels <id,id,...> | --all-38
 *   matching draft SHA vs AVA_38_HOTEL_FINAL_MANIFEST.json
 *
 * Does NOT run without explicit flags. Preserves prior published.pdf as published.prev.pdf.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { getAuditMeta, upsertAuditMeta } from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const MANIFEST = path.join(REPORT, "AVA_38_HOTEL_FINAL_MANIFEST.json");

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}
function arg(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

const authorized = process.argv.includes("--authorize-customer-publication");
const all38 = process.argv.includes("--all-38");
const hotelsArg = arg("--hotels");

if (!authorized) {
  console.error(
    "REFUSED: pass --authorize-customer-publication after founder publication authorization."
  );
  process.exit(1);
}

const manifest = JSON.parse(fs.readFileSync(MANIFEST, "utf8"));
let selected = manifest.hotels || [];
if (!all38) {
  if (!hotelsArg) {
    console.error("Pass --all-38 or --hotels id1,id2");
    process.exit(1);
  }
  const set = new Set(hotelsArg.split(",").map((s) => s.trim()));
  selected = selected.filter(
    (h) => set.has(h.propertyId) || set.has(h.file) || set.has(h.hotelName)
  );
}

const rows = [];
for (const h of selected) {
  const pdfDir = path.join(ROOT, "data/ava-data/pdfs", h.propertyId);
  const draftAbs = path.join(pdfDir, "draft.pdf");
  const publishedAbs = path.join(pdfDir, "published.pdf");
  const prevAbs = path.join(pdfDir, "published.prev.pdf");
  if (!fs.existsSync(draftAbs)) {
    rows.push({ propertyId: h.propertyId, ok: false, error: "DRAFT_MISSING" });
    continue;
  }
  const draftSha = sha256(fs.readFileSync(draftAbs));
  if (draftSha !== h.pdfSha256) {
    rows.push({
      propertyId: h.propertyId,
      ok: false,
      error: "DRAFT_SHA_MISMATCH_VS_MANIFEST",
      draftSha,
      manifestSha: h.pdfSha256,
    });
    continue;
  }
  if (fs.existsSync(publishedAbs)) {
    fs.copyFileSync(publishedAbs, prevAbs);
  }
  fs.copyFileSync(draftAbs, publishedAbs);
  const pubSha = sha256(fs.readFileSync(publishedAbs));
  if (pubSha !== h.pdfSha256) {
    rows.push({
      propertyId: h.propertyId,
      ok: false,
      error: "PUBLISHED_SHA_MISMATCH_AFTER_COPY",
    });
    continue;
  }
  upsertAuditMeta(h.propertyId, {
    publishedPdfSha256: pubSha,
    publishedPdfRelativePath: `pdfs/${h.propertyId}/published.pdf`,
    publishedUpdatedAt: new Date().toISOString(),
    publishedFromDraftSha256: draftSha,
    forceStatus: true,
  });
  rows.push({ propertyId: h.propertyId, hotelName: h.hotelName, ok: true, sha256: pubSha });
  console.log(`PUBLISHED ${h.hotelName}`);
}

const summary = {
  contract: "AVA_V4_PROMOTE_CERTIFIED_PUBLISHED_V1",
  generatedAt: new Date().toISOString(),
  total: rows.length,
  ok: rows.filter((r) => r.ok).length,
  failed: rows.filter((r) => !r.ok).length,
  rows,
};
fs.writeFileSync(
  path.join(REPORT, "AVA_V4_PROMOTE_PUBLISHED_RESULTS.json"),
  `${JSON.stringify(summary, null, 2)}\n`
);
if (summary.failed) process.exit(2);
