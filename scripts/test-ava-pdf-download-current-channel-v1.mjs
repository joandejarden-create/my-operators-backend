/**
 * AVA Download PDF current-channel resolution + draft no-fallback safety.
 *
 *   node scripts/test-ava-pdf-download-current-channel-v1.mjs
 */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  AVA_PDF_CHANNEL,
  describeAvaPdfChannels,
  pickLifecycleCurrentPdfChannel,
  resolveAvaPdfByChannel,
} from "../lib/ai-visibility-audit-action-plan/ava-pdf-channels-v1.js";
import { getAuditMeta } from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";

const TORONTO = "adp_radisson_blu_toronto_downtown";
const WATERSTONE = "adp_waterstone_boca_raton";

function sha256File(abs) {
  return crypto.createHash("sha256").update(fs.readFileSync(abs)).digest("hex");
}

// --- CASE 1: Published Toronto + current PDF → channel=current = published ---
const torontoMeta = getAuditMeta(TORONTO) || {};
assert.equal(
  String(torontoMeta.status || "").toUpperCase(),
  "PUBLISHED",
  "Toronto meta status must be PUBLISHED for this gate"
);
assert.equal(torontoMeta.founderApprovedForCustomer, true);
assert.equal(
  torontoMeta.customerReady === true ||
    String(torontoMeta.customerReadinessStatus || "").toUpperCase() ===
      "CUSTOMER_READY",
  true
);

const torontoDesc = describeAvaPdfChannels(TORONTO);
assert.equal(torontoDesc.published.available, true, "Toronto published PDF exists");

const torontoPick = pickLifecycleCurrentPdfChannel(torontoMeta, torontoDesc);
assert.equal(torontoPick, AVA_PDF_CHANNEL.PUBLISHED);

const torontoCurrent = resolveAvaPdfByChannel({
  propertyId: TORONTO,
  channel: AVA_PDF_CHANNEL.CURRENT,
});
assert.equal(torontoCurrent.ok, true, "Toronto current must resolve");
assert.equal(torontoCurrent.channel, AVA_PDF_CHANNEL.CURRENT);
assert.equal(torontoCurrent.resolvedAs, AVA_PDF_CHANNEL.PUBLISHED);
assert.equal(
  torontoCurrent.sha256,
  torontoDesc.published.file.sha256,
  "current must equal published bytes for Toronto"
);

const torontoPublished = resolveAvaPdfByChannel({
  propertyId: TORONTO,
  channel: AVA_PDF_CHANNEL.PUBLISHED,
});
assert.equal(torontoPublished.ok, true);
assert.equal(torontoCurrent.sha256, torontoPublished.sha256);

// --- CASE 2: Waterstone current follows lifecycle pick ---
const waterDesc = describeAvaPdfChannels(WATERSTONE);
const waterMeta = getAuditMeta(WATERSTONE) || {};
const waterCurrent = resolveAvaPdfByChannel({
  propertyId: WATERSTONE,
  channel: AVA_PDF_CHANNEL.CURRENT,
});
assert.equal(waterCurrent.ok, true, "Waterstone current must resolve");
const waterTarget = pickLifecycleCurrentPdfChannel(waterMeta, waterDesc);
assert.equal(waterCurrent.resolvedAs, waterTarget);

// --- CASE 3: Explicit draft refuses cross-channel fallback on missing history ---
const historyMissing = resolveAvaPdfByChannel({
  propertyId: TORONTO,
  channel: AVA_PDF_CHANNEL.HISTORY,
  versionId: "ava_v1_missing_artifact_gate_test_current_v1",
});
assert.equal(historyMissing.ok, false);
assert.ok(
  historyMissing.error === "VERSION_NOT_FOUND" ||
    historyMissing.error === "HISTORY_PDF_FILE_MISSING"
);

const torontoDraft = resolveAvaPdfByChannel({
  propertyId: TORONTO,
  channel: AVA_PDF_CHANNEL.DRAFT,
});
if (!torontoDesc.draft.available) {
  assert.equal(torontoDraft.ok, false);
  assert.ok(
    torontoDraft.error === "DRAFT_PDF_FILE_MISSING" ||
      torontoDraft.error === "DRAFT_PDF_NOT_REGISTERED" ||
      torontoDraft.error === "DRAFT_PDF_MISSING"
  );
} else {
  assert.equal(torontoDraft.ok, true);
  assert.equal(torontoDraft.channel, AVA_PDF_CHANNEL.DRAFT);
}

// --- CASE 4: Explicit draft when available (Waterstone) ---
if (waterDesc.draft.available) {
  const draftOk = resolveAvaPdfByChannel({
    propertyId: WATERSTONE,
    channel: AVA_PDF_CHANNEL.DRAFT,
  });
  assert.equal(draftOk.ok, true);
  assert.equal(draftOk.channel, AVA_PDF_CHANNEL.DRAFT);
}

// Frontend + API contracts
const adminJs = fs.readFileSync(
  path.join(ROOT, "public/js/admin-ai-visibility-audit.js"),
  "utf8"
);
assert.match(adminJs, /return "current"/);
assert.match(adminJs, /null,\s*\r?\n\s*"current"/);
assert.match(adminJs, /label:\s*"Download PDF"/);
assert.match(adminJs, /label:\s*"Download Draft"/);
assert.match(adminJs, /NEW DRAFT PDF NOT AVAILABLE/);
assert.match(adminJs, /"draft"/);
assert.match(
  adminJs,
  /act === "view-draft-pdf" \|\| act === "download-draft-pdf"/
);

const api = fs.readFileSync(
  path.join(ROOT, "api/admin-ai-visibility-audit.js"),
  "utf8"
);
assert.match(api, /channel=current\|published\|draft\|history/);
assert.match(api, /AVA_PDF_CHANNEL\.CURRENT/);

const namedPdf = path.join(
  ROOT,
  "reports/ai-visibility-audit-action-plan/archive",
  TORONTO,
  "Dealality - AI Visibility Audit & Action Plan - Radisson Blu Toronto Downtown - NOVEMBER2026.pdf"
);
assert.equal(fs.existsSync(namedPdf), true, "named November AVA PDF exists");
assert.equal(
  torontoPublished.sha256,
  sha256File(namedPdf),
  "published current.pdf must match November named AVA PDF"
);

console.log(
  JSON.stringify(
    {
      ok: true,
      toronto: {
        status: torontoMeta.status,
        currentSha: torontoCurrent.sha256,
        resolvedAs: torontoCurrent.resolvedAs,
        publishedSha: torontoPublished.sha256,
        draftAvailable: torontoDesc.draft.available,
      },
      waterstone: {
        status: waterMeta.status,
        resolvedAs: waterCurrent.resolvedAs,
        draftAvailable: waterDesc.draft.available,
        publishedAvailable: waterDesc.published.available,
      },
    },
    null,
    2
  )
);
