/**
 * Automated safeguards: AVA published/draft/history channels.
 *
 *   node scripts/test-ava-pdf-channel-parity-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  AVA_PDF_CHANNEL,
  describeAvaPdfChannels,
  resolveAvaPdfByChannel,
  pdfTextLooksLikeCustomerReportV3,
  ensurePublishedPdfFrozen,
  syncDraftPdfPointersFromDurable,
} from "../lib/ai-visibility-audit-action-plan/ava-pdf-channels-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";

const PROPERTY_ID = "adp_waterstone_boca_raton";
const FOUNDER_OLD =
  "8173ca19ace9add954c442bcecb48cade0fb7394de77ea9b5c468b34391e38c8";

ensurePublishedPdfFrozen(PROPERTY_ID);
syncDraftPdfPointersFromDurable(PROPERTY_ID);

const channels = describeAvaPdfChannels(PROPERTY_ID);
assert.equal(channels.published.available, true, "published available");
assert.equal(channels.draft.available, true, "draft available");
assert.equal(channels.draft.differsFromPublished, true, "draft ≠ published");

const published = resolveAvaPdfByChannel({
  propertyId: PROPERTY_ID,
  channel: AVA_PDF_CHANNEL.PUBLISHED,
});
const draftPreview = resolveAvaPdfByChannel({
  propertyId: PROPERTY_ID,
  channel: AVA_PDF_CHANNEL.DRAFT,
});
const draftDownload = resolveAvaPdfByChannel({
  propertyId: PROPERTY_ID,
  channel: AVA_PDF_CHANNEL.DRAFT,
});

assert.equal(published.ok, true);
assert.equal(draftPreview.ok, true);
assert.equal(published.sha256, FOUNDER_OLD, "published is founder 11-page PDF");
assert.notEqual(
  draftPreview.sha256,
  published.sha256,
  "draft must not resolve to published"
);
assert.equal(
  draftPreview.sha256,
  draftDownload.sha256,
  "preview/download parity"
);

// Missing history must not fall back
const missing = resolveAvaPdfByChannel({
  propertyId: PROPERTY_ID,
  channel: AVA_PDF_CHANNEL.HISTORY,
  versionId: "ava_v1_missing_artifact_gate_test",
});
assert.equal(missing.ok, false);
assert.equal(missing.error, "VERSION_NOT_FOUND");

// Invalid channel
const bad = resolveAvaPdfByChannel({
  propertyId: PROPERTY_ID,
  channel: "customer-share",
});
assert.equal(bad.ok, false);
assert.equal(bad.error, "INVALID_PDF_CHANNEL");

// Draft content gates — V4 certified portfolio (or legacy V3) is acceptable
const { getDocument } = await import("pdfjs-dist/legacy/build/pdf.mjs");
const buf = fs.readFileSync(draftPreview.abs);
const doc = await getDocument({
  data: new Uint8Array(buf),
  useSystemFonts: true,
}).promise;
assert.ok(doc.numPages >= 6, "draft has substantive page count");
let text = "";
for (let i = 1; i <= doc.numPages; i++) {
  const page = await doc.getPage(i);
  const content = await page.getTextContent();
  text += content.items.map((it) => it.str).join(" ") + "\n";
}
const looksV4 =
  /Executive Audit Summary/i.test(text) &&
  /Digital Platform Assessment/i.test(text) &&
  /Implementation Roadmap/i.test(text);
const v3 = pdfTextLooksLikeCustomerReportV3(text);
assert.equal(
  looksV4 || (v3.hasExecutive && v3.hasPlatformAssessment),
  true,
  "draft must look like AVA customer report V4 or V3"
);

// Admin JS must request channel=
const adminJs = fs.readFileSync(
  path.join(ROOT, "public/js/admin-ai-visibility-audit.js"),
  "utf8"
);
assert.match(adminJs, /channel=/);
assert.match(adminJs, /New Draft PDF/);
assert.match(adminJs, /Published PDF/);
assert.match(adminJs, /refusing to fall back/);

// API route must document channel
const api = fs.readFileSync(
  path.join(ROOT, "api/admin-ai-visibility-audit.js"),
  "utf8"
);
assert.match(api, /channel=published\|draft\|history/);
assert.match(api, /resolveAvaPdfByChannel/);
assert.match(api, /X-AVA-PDF-Channel/);

console.log(
  JSON.stringify(
    {
      ok: true,
      publishedSha: published.sha256,
      draftSha: draftPreview.sha256,
      draftPages: doc.numPages,
      looksV4,
      v3,
    },
    null,
    2
  )
);
