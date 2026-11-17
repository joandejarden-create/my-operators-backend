import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { getAuditMeta } from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { loadReviewPayload } from "../lib/ai-demand-positioning/monthly-review/admin/archive-store-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const PR_ID = "adp_mr_radisson_blu_toronto_dow_2026-11_v4";

function sha(p) {
  return crypto.createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}

const meta = getAuditMeta(PROPERTY_ID);
if (!meta) {
  console.error("FAIL no audit meta");
  process.exit(1);
}
if (meta.customerPublished !== true || meta.customerReady !== true) {
  console.error("FAIL customer flags", {
    customerPublished: meta.customerPublished,
    customerReady: meta.customerReady,
  });
  process.exit(1);
}
const latest = String(meta.latestVersionId || "");
if (
  !/toronto_corr_v2|toronto_closeout_v3/i.test(latest) &&
  !/PUBLISHED|AUDIT_VALIDATED/i.test(String(meta.status || ""))
) {
  console.error("FAIL latestVersionId", meta.latestVersionId, meta.status);
  process.exit(1);
}
if (!/toronto_corr_v2|toronto_closeout_v3/i.test(latest)) {
  console.error("FAIL latestVersionId not closeout/corr", meta.latestVersionId);
  process.exit(1);
}

const archiveCurrent = path.join(
  ROOT,
  "reports/ai-visibility-audit-action-plan/archive",
  PROPERTY_ID,
  "current.pdf"
);
if (!fs.existsSync(archiveCurrent)) {
  console.error("FAIL missing archive current.pdf");
  process.exit(1);
}
const digest = sha(archiveCurrent);
if (meta.pdfSha256 && meta.pdfSha256 !== digest) {
  console.error("FAIL pdf sha mismatch", meta.pdfSha256, digest);
  process.exit(1);
}

const pack = loadReviewPayload(PR_ID);
if (!pack) {
  console.error("FAIL missing PR payload", PR_ID);
  process.exit(1);
}
if (!pack.meta?.isCurrent) {
  console.error("FAIL PR not current", pack.meta);
  process.exit(1);
}
if (!fs.existsSync(pack.paths.reportPdf)) {
  console.error("FAIL missing PR pdf");
  process.exit(1);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:customer-package-current-artifacts-v1",
      avaVersion: meta.latestVersionId,
      avaSha12: digest.slice(0, 12),
      prId: PR_ID,
    },
    null,
    2
  )
);
